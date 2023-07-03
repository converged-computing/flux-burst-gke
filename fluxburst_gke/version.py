# Copyright 2023 Lawrence Livermore National Security, LLC and other
# HPCIC DevTools Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: (MIT)

__version__ = "0.0.11"
AUTHOR = "Vanessa Sochat"
EMAIL = "vsoch@users.noreply.github.com"
NAME = "flux-burst-gke"
PACKAGE_URL = "https://github.com/converged-computing/flux-burst-gke"
KEYWORDS = "flux, flux-framework, workflow, example, plugin"
DESCRIPTION = "A bursting plugin for Flux and GKE"
LICENSE = "LICENSE"

################################################################################
# Global requirements

# Since we assume wanting Singularity and lmod, we require spython and Jinja2

INSTALL_REQUIRES = (
    ("flux-burst", {"min_version": None}),
    ("requests", {"min_version": None}),
    ("kubescaler", {"min_version": None}),
    ("fluxoperator", {"min_version": None}),
)

TESTS_REQUIRES = (("pytest", {"min_version": "4.6.2"}),)

################################################################################
# Submodule Requirements (versions that include database)

INSTALL_REQUIRES_ALL = INSTALL_REQUIRES + TESTS_REQUIRES
