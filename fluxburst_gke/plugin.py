# Copyright 2023 Lawrence Livermore National Security, LLC and other
# HPCIC DevTools Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: (MIT)

import os
import socket
from dataclasses import dataclass
from typing import Optional

import fluxburst.plugins as plugins
from fluxburst.logger import logger
from fluxoperator.client import FluxMiniCluster
from kubernetes import client as kubernetes_client
from kubernetes import utils as k8sutils
from kubernetes.client.rest import ApiException

# This will allow us to create and interact with our cluster
from kubescaler.scaler.google import GKECluster

import fluxburst_gke.cluster as helpers


@dataclass
class BurstParameters:
    """
    Custom parameters for Flux Operator bursting.

    It should be possible to read this in from yaml, or the
    environment (or both).
    """

    # Google Cloud Project
    project: str

    # Lead broker service hostname or ip address
    lead_host: str

    # Lead broker service port (e.g, 30093)
    lead_port: str

    # Lead broker size
    lead_size: int

    # Custom broker toml template for bursted cluster
    broker_toml: Optional[str] = None

    # Name of a secret to be made in the same namespace
    munge_secret_name: Optional[str] = "munge-key"

    # Path to munge.key file (local) to use to create config map
    # If this is owned by root, likely won't be readable
    munge_key: Optional[str] = "/etc/munge/munge.key"

    # curve secret name to do the same for
    curve_cert_secret_name: Optional[str] = "curve-cert"

    # Path to curve.cert
    curve_cert: Optional[str] = "/mnt/curve/curve.cert"

    cluster_name: Optional[str] = "flux-bursted-cluster"
    machine_type: Optional[str] = "c2-standard-8"
    cpu_limit: Optional[int] = None
    memory_limit: Optional[int] = None

    # Container image to run for pods of MiniCluster
    image: Optional[str] = "ghcr.io/flux-framework/flux-restful-api:latest"

    # Name for external minicluster
    name: Optional[str] = "burst-0"

    # Namespace for external minicluster
    namespace: Optional[str] = "flux-operator"

    # Custom yaml definition to use to install the Flux Operator
    flux_operator_yaml: Optional[str] = None

    # Flux log level
    log_level: Optional[int] = 7

    # Custom flux user
    flux_user: Optional[str] = None

    # arguments to flux wrap, e.g., "strace,-e,network,-tt
    wrap: Optional[str] = None


class FluxBurstGKE(plugins.BurstPlugin):
    # Set our custom dataclass, otherwise empty
    _param_dataclass = BurstParameters

    def __init__(self, *args, **kwargs):
        self.__super__(*args, **kwargs)

        # Very simple and stupid way (for now) to keep track of bursted clusters
        # TODO we will need a way to know when to create/destroy
        self.clusters = {}

    def schedule(self, job):
        """
        Given a burstable job, determine if we can schedule it.

        This function should also consider logic for deciding if/when to
        assign clusters, but run should actually create/destroy.
        """
        # We cannot run any jobs without Google Application Credentials
        if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
            logger.warning(
                "GOOGLE_APPLICATION_CREDENTIALS not found in environment, cannot schedule to GKE."
            )
            return False

        # TODO determine if we can match some resource spec to another,
        # We likely want this class to be able to generate a lookup of
        # instances / spec about them.

        # For now, we just accept anything, and add to our jobs and return true
        if job["id"] in self.jobs:
            logger.debug(f"{job['id']} is already scheduled")
            return True

        # Add to self.jobs and return True!
        self.jobs[job["id"]] = job
        return True

    def cleanup(self, name=None):
        """
        Cleanup (delete) one or more clusters
        """
        if name and name not in self.clusters:
            raise ValueError(f"{name} is not a known cluster.")
        clusters = self.clusters if not name else {"name": self.clusters["name"]}
        for cluster_name, _ in clusters.items():
            logger.info("Cleaning up {cluster_name}")
            cli = GKECluster(
                project=self.params.project,
                name=cluster_name,
            )
            cli.delete_cluster()

        # Update known clusters
        updated = {}
        for name in self.clusters:
            if name not in clusters:
                updated[name] = self.clusters[name]
        self.clusters = updated

    def ensure_namespace(self, kubectl):
        """
        Use the instantiated kubectl to ensure the cluster namespace exists.
        """
        try:
            kubectl.create_namespace(
                kubernetes_client.V1Namespace(
                    metadata=kubernetes_client.V1ObjectMeta(name=self.params.namespace)
                )
            )
        except Exception:
            logger.warning(
                f"🥵️ Issue creating namespace {self.params.namespace}, assuming already exists."
            )

    def check_configs(self):
        """
        Ensure we have required config files.
        """
        message = "does not exist or you don't have permissions to see it"
        if not os.path.exists(self.params.munge_key):
            raise ValueError(f"Provided munge key {self.params.munge_key} {message}.")

        if not os.path.exists(self.params.curve_cert):
            raise ValueError(f"Provided curve cert {self.params.munge_key} {message}.")

    def create_cluster(self):
        """
        Create the cluster and return a client handle to it.
        """
        cluster_name = self.params.cluster_name
        logger.info(f"📛️ Cluster name will be {cluster_name}")

        # TODO - need a way to intelligently assign jobs to clusters
        # A cluster might already exist that we could use.
        # For now, just create cluster for max of job size
        max_size = max([v["nnodes"] for _, v in self.jobs.items()])
        logger.info(f"📛️ Cluster size will be {max_size}")

        # Create a handle to the GKE cluster
        cli = GKECluster(
            project=self.params.project,
            name=cluster_name,
            node_count=max_size,
            # This is a default machine type, but should also be
            # advised by the scheduler for the job
            machine_type=self.params.machine_type,
            min_nodes=max_size,
            max_nodes=max_size,
        )

        # Create the cluster (this times it)
        try:
            self.clusters[cluster_name] = cli.create_cluster()
        # What other cases might be here?
        except Exception:
            print("🥵️ Issue creating cluster, assuming already exists.")

        # Create a client from it
        logger.info(f"📦️ The cluster has {cli.node_count} nodes!")
        return cli

    def install_flux_operator(self, kubectl, flux_operator_yaml):
        """
        Install the flux operator yaml
        """
        try:
            k8sutils.create_from_yaml(kubectl.api_client, flux_operator_yaml)
            logger.info("Installed the operator.")
        except Exception as exc:
            logger.warning(
                f"Issue installing the operator: {exc}, assuming already exists"
            )

    def run(self):
        """
        Given some set of scheduled jobs, run bursting.
        """
        # Exit early if no jobs to burst
        if not self.jobs:
            logger.info(f"Plugin {self.name} has no jobs to burst.")
            return

        # Ensure we have a flux operator yaml file, fosho, foyaml!
        foyaml = helpers.ensure_flux_operator_yaml(self.params.flux_operator_yaml)

        # lead host / port / size / are required in the dataclass
        # We check munge paths here, because could be permissions issue
        self.check_configs()

        cli = self.create_cluster()
        kubectl = cli.get_k8s_client()

        # Install the operator!
        self.install_flux_operator(kubectl, foyaml)

        # Create a MiniCluster for each job
        for _, job in self.jobs.items():
            self.create_minicluster(kubectl, job)

    def create_minicluster(self, kubectl, job):
        """
        Create the MiniCluster
        """
        command = " ".join(job["spec"]["tasks"][0]["command"])
        logger.info(f"Preparing MiniCluster for {job['id']}: {command}")

        # The plugin is assumed to be running from the lead broker
        # of the cluster it is bursting from, this we get info about it
        podname = socket.gethostname()
        hostname = podname.rsplit("-", 1)[0]

        # TODO: we are using defaults for now, but will update this to be likely
        # configured based on the algorithm that chooses the best spec
        minicluster, container = helpers.get_minicluster(
            command,
            name=self.params.name,
            memory_limit=self.params.memory_limit,
            cpu_limit=self.params.cpu_limit,
            namespace=self.params.namespace,
            curve_cert_secret_name=self.params.curve_cert_secret_name,
            broker_toml=self.params.broker_toml,
            tasks=job["ntasks"],
            size=job["nnodes"],
            image=self.params.image,
            wrap=self.params.wrap,
            log_level=self.params.log_level,
            flux_user=self.params.flux_user,
            lead_host=self.params.lead_host,
            lead_port=self.params.lead_port,
            munge_secret_name=self.params.munge_secret_name,
            lead_jobname=hostname,
            lead_size=self.params.lead_size,
        )

        # Create the namespace
        self.ensure_namespace(kubectl)

        # Let's assume there could be bugs applying this differently
        crd_api = kubernetes_client.CustomObjectsApi(kubectl.api_client)

        self.ensure_secrets(kubectl)

        # Create the MiniCluster! This also waits for it to be ready
        print(
            f"⭐️ Creating the minicluster {self.params.name} in {self.params.namespace}..."
        )
        operator = FluxMiniCluster()
        import IPython

        IPython.embed()
        res = operator.create(**minicluster, container=container, crd_api=crd_api)

    def ensure_secrets(self, kubectl):
        """
        Ensure secrets (munge.key and curve.cert) are ready for a job
        """
        print("ENSURE SECRETS")
        import IPython

        IPython.embed()
        secrets = []

        # kubectl create secret --namespace flux-operator munge-key --from-file=/etc/munge/munge.key
        if self.params.curve_cert:
            secrets.append(
                helpers.create_secret(
                    self.params.curve_cert,
                    "curve.cert",
                    self.params.curve_cert_secret_name,
                    self.params.namespace,
                )
            )

        if self.params.munge_key:
            secrets.append(
                helpers.create_secret(
                    self.params.munge_key,
                    "munge.key",
                    self.params.munge_secret_name,
                    self.params.namespace,
                )
            )

        for secret in secrets:
            try:
                kubectl.create_namespaced_secret(
                    namespace=self.params.namespace,
                    body=secret,
                )
            except ApiException as e:
                print(
                    "Exception when calling CoreV1Api->create_namespaced_config_map: %s\n"
                    % e
                )
