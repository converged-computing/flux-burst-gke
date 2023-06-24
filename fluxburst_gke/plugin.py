# Copyright 2023 Lawrence Livermore National Security, LLC and other
# HPCIC DevTools Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: (MIT)

import os

import fluxburst.plugins as plugins
import kubescaler.utils as utils
from fluxburst.logger import logger

# This will allow us to create and interact with our cluster
from kubescaler.scaler.google import GKECluster

import fluxburst_gke.defaults as defaults

# Save data here
here = os.path.dirname(os.path.abspath(__file__))

from fluxoperator.client import FluxMiniCluster
from kubernetes import client as kubernetes_client
from kubernetes import utils as k8sutils
from kubernetes.client.rest import ApiException

here = os.path.abspath(os.path.dirname(__file__))

from dataclasses import dataclass
from typing import Optional


@dataclass
class BurstParameters:
    """
    Custom parameters for Flux Operator bursting.

    It should be possible to read this in from yaml, or the
    environment (or both).
    """

    # Google Cloud Project
    project: str
    cluster_name: str = "flux-cluster"
    machine_type: str = "c2-standard-8"
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

    # Path to munge.key file (local) to use to create config map
    munge_key: str

    # Name of a secret to be made in the same namespace
    munge_secret_name: str

    # Path to curve.cert
    curve_cert: Optional[str] = "/mnt/curve/curve.cert"

    # Broker toml template for bursted cluster
    broker_toml: str

    # Lead broker service hostname or ip address
    lead_host: str

    # Lead broker service port (e.g, 30093)
    lead_port: str

    # Lead broker size
    lead_size: str

    # Flux log level
    log_level: Optional[int] = 7

    # Custom flux user
    flux_user: Optional[str] = None

    # arguments to flux wrap, e.g., "strace,-e,network,-tt
    wrap: Optional[str] = None


# TODO: need to parse parser args into dataclass, should be in set params
# also need to have a WAY TO ENSURE DEFULTS
# TODO also ensure we can get envars from the environment


class FluxBurstGKE(plugins.BurstPlugin):
    def schedule(self, *args, **kwargs):
        """
        Given a burstable job, determine if we can schedule it.

        This function should also consider logic for deciding if/when to
        assign clusters, but run should actually create/destroy.
        """
        print("GKE SCHEDULE")
        import IPython

        IPython.embed()
        # TODO determine if we can match some resource spec to another,
        # add to self.jobs
        # We likely want this class to be able to generate a lookup of instances / spec about them.

    def run(self):
        """
        Given some set of scheduled jobs, run bursting.
        """
        if not self.jobs:
            logger.info(f"Plugin {self.name} has no jobs to burst.")
            return

        print("RUN")
        import IPython

        IPython.embed()
        # Exit early if no jobs to burst.


def ensure_curve_cert(args):
    """
    Ensure we are provided with an existing curve certificate we can load.
    """
    if not args.curve_cert or not os.path.exists(args.curve_cert):
        logger.exit(
            f"Curve cert (provided as {args.curve_cert}) needs to be defined and exist."
        )
    return utils.read_file(args.curve_cert)


def write_minicluster_yaml(mc):
    """
    Write the MiniCluster spec to yaml to apply
    """
    # this could be saved for reproducibility, if needed.
    minicluster_yaml = utils.get_tmpfile(prefix="minicluster") + ".yaml"
    utils.write_yaml(mc, minicluster_yaml)
    return minicluster_yaml


def main():
    """
    Create an external cluster we can burst to, and optionally resize.

    TODO this will get converted to schedule and run
    """
    # If an error occurs while parsing the arguments, the interpreter will exit with value 2
    args, _ = parser.parse_known_args()
    if not args.project:
        sys.exit("Please define your Google Cloud Project with --project")

    # Pull cluster name out of argument
    # TODO: likely we will start Flux with an ability to say "allow this external flux cluster"
    # and then it will have a name that can be derived from that.
    cluster_name = args.cluster_name
    print(f"📛️ New cluster name will be {cluster_name}")

    ensure_flux_operator_yaml(args)
    curve_cert = ensure_curve_cert(args)

    # Lead host and port are required
    if not args.lead_port or not args.lead_host:
        sys.exit("--lead-host and --lead-port must be defined.")
    print(
        "Broker lead will be expected to be accessible on {args.lead_host}:{args.lead_port}"
    )

    if args.munge_key and not os.path.exists(args.munge_key):
        sys.exit(f"Provided munge key {args.munge_key} does not exist.")
    if args.munge_key and not args.munge_config_map:
        args.munge_config_map = "munge-key"

    # Create a spec for what we need to burst.
    # This will be just for one moment in time, obviously there would be different
    # ways to do this (to decide when to burst, based on what metrics, etc.)
    # For now we will just assume one cluster + burst per job!
    burstable = []
    listing = flux.job.job_list(handle).get()
    for job in listing.get("jobs", []):
        info = get_job_info(job["id"])
        if not is_burstable(info):
            continue
        print(f"🧋️  Job {job['id']} is marked for bursting.")
        burstable.append(info)

    if not burstable:
        sys.exit("No jobs were found marked for burstable.")

    # Assume we just have one configuration to create for now
    # We ideally want something more elegant
    info = burstable[0]

    # Determine if the cluster exists, and if not, create it
    # For now, ensure lead broker in both is same hostname
    podname = socket.gethostname()
    hostname = podname.rsplit("-", 1)[0]

    # Try creating the cluster (this is just the GKE cluster)
    # n2-standard-8 has 4 actual cores, so 4x4 == 16 tasks
    cli = GKECluster(
        project=args.project,
        name=cluster_name,
        node_count=info["nnodes"],
        machine_type=args.machine_type,
        min_nodes=info["nnodes"],
        max_nodes=info["nnodes"],
    )
    # Create the cluster (this times it)
    try:
        cli.create_cluster()
    # What other cases might be here?
    except:
        print("🥵️ Issue creating cluster, assuming already exists.")

    # Create a client from it
    print(f"📦️ The cluster has {cli.node_count} nodes!")
    kubectl = cli.get_k8s_client()

    # Install the operator!
    try:
        k8sutils.create_from_yaml(kubectl.api_client, args.flux_operator_yaml)
        print("Installed the operator.")
    except Exception as exc:
        print(f"Issue installing the operator: {exc}, assuming already exists")

    # NOTE we previously populated a broker.toml template here, and we don't
    # need to do that anymore - the operator will generate the config

    # Assemble the command from the requested job
    command = " ".join(info["spec"]["tasks"][0]["command"])
    print(f"Command is {command}")

    # TODO: we are using defaults for now, but will update this to be likely
    # configured based on the algorithm that chooses the best spec
    minicluster, container = get_minicluster(
        command,
        name=args.name,
        memory_limit=args.memory_limit,
        cpu_limit=args.cpu_limit,
        namespace=args.namespace,
        curve_cert=curve_cert,
        broker_toml=args.broker_toml,
        tasks=info["ntasks"],
        size=info["nnodes"],
        image=args.image,
        wrap=args.wrap,
        log_level=args.log_level,
        flux_user=args.flux_user,
        lead_host=args.lead_host,
        lead_port=args.lead_port,
        munge_config_map=args.munge_config_map,
        lead_jobname=hostname,
        lead_size=args.lead_size,
    )

    # Create the namespace
    try:
        kubectl.create_namespace(
            kubernetes_client.V1Namespace(
                metadata=kubernetes_client.V1ObjectMeta(name=args.namespace)
            )
        )
    except:
        print(f"🥵️ Issue creating namespace {args.namespace}, assuming already exists.")

    # Let's assume there could be bugs applying this differently
    crd_api = kubernetes_client.CustomObjectsApi(kubectl.api_client)

    # kubectl create configmap --namespace flux-operator munge-key --from-file=/etc/munge/munge.key
    # WORKING HERE
    # TODO create from file in the same namespace?
    import IPython

    IPython.embed()

    if args.munge_key:
        cm = create_munge_configmap(
            args.munge_key, args.munge_config_map, args.namespace
        )
        try:
            kubectl.create_namespaced_config_map(
                namespace=args.namespace,
                body=cm,
            )
        except ApiException as e:
            print(
                "Exception when calling CoreV1Api->create_namespaced_config_map: %s\n"
                % e
            )

    # Create the MiniCluster! This also waits for it to be ready
    # TODO we need a check here for completed - it will hang
    # Need to fix this so it doesn't hang. We need to decide when to
    # bring down the minicluster.
    print(f"⭐️ Creating the minicluster {args.name} in {args.namespace}...")
    operator = FluxMiniCluster()
    operator.create(**minicluster, container=container, crd_api=crd_api)

    # Eventually to clean up...
    cli.delete_cluster()
