# Copyright 2023 Lawrence Livermore National Security, LLC and other
# HPCIC DevTools Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: (MIT)

from fluxburst_gke.version import __version__

assert __version__


def init(dataclass, **kwargs):
    """
    Parse custom arguments and return the Burst client.

    We use a function based import here in case additional checks are
    needed, or things that aren't available when the module is loaded
    (but are later when this function is called)
    """
    from .plugin import FluxBurstGKE

    return FluxBurstGKE(dataclass, **kwargs)
