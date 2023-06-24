# Copyright 2023 Lawrence Livermore National Security, LLC and other
# HPCIC DevTools Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: (MIT)

import base64
import os

import fluxburst.utils as utils
import requests
from fluxburst.logger import logger
from kubernetes import client as kubernetes_client

import fluxburst_gke.defaults as defaults


def get_minicluster(
    command,
    size=None,
    tasks=None,  # nodes * cpu per node, where cpu per node is vCPU / 2
    cpu_limit=None,
    memory_limit=None,
    flags=None,
    name=None,
    namespace=None,
    image=None,
    wrap=None,
    log_level=7,
    flux_user=None,
    lead_host=None,
    lead_port=None,
    broker_toml=None,
    munge_config_map=None,
    lead_size=None,
    lead_jobname=None,
    zeromq=False,
    quiet=False,
    strict=False,
):
    """
    Get a MiniCluster CRD as a dictionary

    Limits should be slightly below actual pod resources. The curve cert and broker config
    are required, since we need this external cluster to connect to ours!
    """
    flags = flags or "-ompi=openmpi@5 -c 1 -o cpu-affinity=per-task"
    image = image or "ghcr.io/flux-framework/flux-restful-api"
    container = {"image": image, "command": command, "resources": {}}

    if cpu_limit is None and memory_limit is None:
        del container["resources"]
    elif cpu_limit is not None or memory_limit is not None:
        container["resources"] = {"limits": {}, "requests": {}}
    if cpu_limit is not None:
        container["resources"]["limits"]["cpu"] = cpu_limit
        container["resources"]["requests"]["cpu"] = cpu_limit
    if memory_limit is not None:
        container["resources"]["limits"]["memory"] = memory_limit
        container["resources"]["requests"]["memory"] = memory_limit

    # Do we have a custom flux user for the container?
    if flux_user:
        container["flux_user"] = {"name": flux_user}

    # The MiniCluster has the added name and namespace
    mc = {
        "size": size,
        "namespace": namespace,
        "name": name,
        "interactive": False,
        "logging": {"zeromq": zeromq, "quiet": quiet, "strict": strict},
        "flux": {
            "option_flags": flags,
            "connect_timeout": "5s",
            "log_level": log_level,
            # Providing the lead broker and port points back to the parent
            "bursting": {
                "lead_broker": {
                    "address": lead_host,
                    "port": int(lead_port),
                    "name": lead_jobname,
                    "size": int(lead_size),
                },
                "clusters": [{"size": size, "name": name}],
            },
        },
    }

    if tasks is not None:
        mc["tasks"] = tasks
    if munge_config_map:
        mc["flux"]["mungeConfigMap"] = munge_config_map
    if broker_toml:
        mc["flux"]["broker_config"] = broker_toml

    # eg., this would require strace "strace,-e,network,-tt"
    if wrap is not None:
        mc["flux"]["wrap"] = wrap
    return mc, container


def ensure_flux_operator_yaml(args):
    """
    Ensure we are provided with the installation yaml and it exists!
    """
    # flux operator yaml default is current from main
    if not args.flux_operator_yaml:
        args.flux_operator_yaml = utils.get_tmpfile(prefix="flux-operator") + ".yaml"
        r = requests.get(defaults.flux_operator_yaml, allow_redirects=True)
        utils.write_file(r.content, args.flux_operator_yaml)

    # Ensure it really really exists
    args.flux_operator_yaml = os.path.abspath(args.flux_operator_yaml)
    if not os.path.exists(args.flux_operator_yaml):
        logger.exit(f"{args.flux_operator_yaml} does not exist.")


def create_munge_configmap(path, name, namespace):
    """
    Create a binary data config map
    """
    # Configureate ConfigMap metadata
    metadata = kubernetes_client.V1ObjectMeta(
        name=name,
        namespace=namespace,
    )
    # Get File Content
    with open(path, "rb") as f:
        content = f.read()

    # base64 encoded string
    content = base64.b64encode(content).decode("utf-8")

    # Instantiate the configmap object
    return kubernetes_client.V1ConfigMap(
        api_version="v1",
        kind="ConfigMap",
        binary_data={"munge.key": content},
        metadata=metadata,
    )
