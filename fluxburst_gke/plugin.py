# Copyright 2023 Lawrence Livermore National Security, LLC and other
# HPCIC DevTools Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: (MIT)

import os
from dataclasses import dataclass
from typing import Optional

import fluxburst.kubernetes as bases
from fluxburst.logger import logger

# This will allow us to create and interact with our cluster
from kubescaler.scaler.google import GKECluster


@dataclass
class BurstParameters:
    """
    Custom parameters for Flux Operator bursting.

    It should be possible to read this in from yaml, or the
    environment (or both).
    """

    # Google Cloud Project
    project: str

    # An isolated burst brings up an independent cluster
    isolated_burst: Optional[bool] = False

    # Lead broker service hostname or ip address
    lead_host: Optional[str] = None

    # Lead broker service port (e.g, 30093)
    lead_port: Optional[str] = None

    # Lead broker size
    lead_size: Optional[str] = None

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


class FluxBurstGKE(bases.KubernetesBurstPlugin):
    # Set our custom dataclass, otherwise empty
    _param_dataclass = BurstParameters

    def validate(self):
        """
        Validate ensures that required conditions are met.

        This should return True/False to indicate if valid or not, and
        print meaninging error messages for the user
        """
        # We cannot run any jobs without Google Application Credentials
        if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
            logger.warning(
                "GOOGLE_APPLICATION_CREDENTIALS not found in environment, cannot schedule to GKE."
            )
            return False

        # Shared validation functions from kubernetes
        return super(FluxBurstGKE, self).validate()

    def schedule(self, job):
        """
        Given a burstable job, determine if we can schedule it.

        This function should also consider logic for deciding if/when to
        assign clusters, but run should actually create/destroy.
        """
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
            logger.warning(f"{name} is not a known cluster.")
        clusters = list(self.clusters) if not name else [name]
        for cluster_name in clusters:
            logger.info(f"Cleaning up {cluster_name}")
            cli = GKECluster(
                project=self.params.project,
                name=cluster_name,
            )
            try:
                cli.delete_cluster()
            except Exception:
                logger.warning(f"Issue deleting {cluster_name}")

        # Update known clusters
        updated = {}
        for name in self.clusters:
            if name not in clusters:
                updated[name] = self.clusters[name]
        self.clusters = updated

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
        # We still need to register the cluster exists
        except Exception:
            self.clusters[cluster_name] = cli.create_cluster()
            print("🥵️ Issue creating cluster, assuming already exists.")

        # Create a client from it
        logger.info(f"📦️ The cluster has {cli.node_count} nodes!")
        return cli
