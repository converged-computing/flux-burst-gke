# Copyright 2023 Lawrence Livermore National Security, LLC and other
# HPCIC DevTools Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: (MIT)

from fluxburst_gke.version import __version__

assert __version__


def client(**kwargs):
    """
    Parse custom arguments and return the Burst client.

    It is recommended to import here in case connecting to Flux
    is required.
    """
    from .burst import FluxBurstGKE

    return FluxBurstGKE(**kwargs)
