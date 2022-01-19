# -*- coding: utf-8 -*-
#
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Generated by synthtool. DO NOT EDIT!

from __future__ import absolute_import
import os
import pathlib
import re
import shutil

import nox


BLACK_VERSION = "black==19.10b0"
BLACK_PATHS = ["docs", "sqlalchemy_bigquery", "tests", "noxfile.py", "setup.py"]

DEFAULT_PYTHON_VERSION = "3.8"

# We're using two Python versions to test with sqlalchemy 1.3 and 1.4.
SYSTEM_TEST_PYTHON_VERSIONS = ["3.8", "3.10"]
UNIT_TEST_PYTHON_VERSIONS = ["3.6", "3.7", "3.8", "3.9", "3.10"]

CURRENT_DIRECTORY = pathlib.Path(__file__).parent.absolute()

# 'docfx' is excluded since it only needs to run in 'docs-presubmit'
nox.options.sessions = [
    "unit",
    "system",
    "cover",
    "lint",
    "lint_setup_py",
    "blacken",
    "docs",
]

# Error if a python version is missing
nox.options.stop_on_first_error = True
nox.options.error_on_missing_interpreters = True


@nox.session(python=DEFAULT_PYTHON_VERSION)
def lint(session):
    """Run linters.

    Returns a failure if the linters find linting errors or sufficiently
    serious code quality issues.
    """
    session.install("flake8", BLACK_VERSION)
    session.run(
        "black", "--check", *BLACK_PATHS,
    )
    session.run("flake8", "sqlalchemy_bigquery", "tests")


@nox.session(python=DEFAULT_PYTHON_VERSION)
def blacken(session):
    """Run black. Format code to uniform standard."""
    session.install(BLACK_VERSION)
    session.run(
        "black", *BLACK_PATHS,
    )


@nox.session(python=DEFAULT_PYTHON_VERSION)
def lint_setup_py(session):
    """Verify that setup.py is valid (including RST check)."""
    session.install("docutils", "pygments")
    session.run("python", "setup.py", "check", "--restructuredtext", "--strict")


def default(session):
    # Install all test dependencies, then install this package in-place.

    constraints_path = str(
        CURRENT_DIRECTORY / "testing" / f"constraints-{session.python}.txt"
    )
    session.install(
        "mock",
        "asyncmock",
        "pytest",
        "pytest-cov",
        "pytest-asyncio",
        "-c",
        constraints_path,
    )

    if session.python == "3.8":
        extras = "[tests,alembic]"
    elif session.python == "3.10":
        extras = "[tests,geography]"
    else:
        extras = "[tests]"
    session.install("-e", f".{extras}", "-c", constraints_path)

    # Run py.test against the unit tests.
    session.run(
        "py.test",
        "--quiet",
        f"--junitxml=unit_{session.python}_sponge_log.xml",
        "--cov=sqlalchemy_bigquery",
        "--cov=tests/unit",
        "--cov-append",
        "--cov-config=.coveragerc",
        "--cov-report=",
        "--cov-fail-under=0",
        os.path.join("tests", "unit"),
        *session.posargs,
    )


@nox.session(python=UNIT_TEST_PYTHON_VERSIONS)
def unit(session):
    """Run the unit test suite."""
    default(session)


@nox.session(python=SYSTEM_TEST_PYTHON_VERSIONS)
def system(session):
    """Run the system test suite."""
    constraints_path = str(
        CURRENT_DIRECTORY / "testing" / f"constraints-{session.python}.txt"
    )
    system_test_path = os.path.join("tests", "system.py")
    system_test_folder_path = os.path.join("tests", "system")

    # Check the value of `RUN_SYSTEM_TESTS` env var. It defaults to true.
    if os.environ.get("RUN_SYSTEM_TESTS", "true") == "false":
        session.skip("RUN_SYSTEM_TESTS is set to false, skipping")
    # Install pyopenssl for mTLS testing.
    if os.environ.get("GOOGLE_API_USE_CLIENT_CERTIFICATE", "false") == "true":
        session.install("pyopenssl")

    system_test_exists = os.path.exists(system_test_path)
    system_test_folder_exists = os.path.exists(system_test_folder_path)
    # Sanity check: only run tests if found.
    if not system_test_exists and not system_test_folder_exists:
        session.skip("System tests were not found")

    # Use pre-release gRPC for system tests.
    session.install("--pre", "grpcio")

    # Install all test dependencies, then install this package into the
    # virtualenv's dist-packages.
    session.install("mock", "pytest", "google-cloud-testutils", "-c", constraints_path)
    if session.python == "3.8":
        extras = "[tests,alembic]"
    elif session.python == "3.10":
        extras = "[tests,geography]"
    else:
        extras = "[tests]"
    session.install("-e", f".{extras}", "-c", constraints_path)

    # Run py.test against the system tests.
    if system_test_exists:
        session.run(
            "py.test",
            "--quiet",
            f"--junitxml=system_{session.python}_sponge_log.xml",
            system_test_path,
            *session.posargs,
        )
    if system_test_folder_exists:
        session.run(
            "py.test",
            "--quiet",
            f"--junitxml=system_{session.python}_sponge_log.xml",
            system_test_folder_path,
            *session.posargs,
        )


@nox.session(python=DEFAULT_PYTHON_VERSION)
def prerelease(session):
    session.install(
        "--prefer-binary",
        "--pre",
        "--upgrade",
        "alembic",
        "geoalchemy2",
        "google-api-core",
        "google-cloud-bigquery",
        "google-cloud-bigquery-storage",
        "google-cloud-core",
        "google-resumable-media",
        "grpcio",
        "sqlalchemy",
        "shapely",
    )
    session.install(
        "freezegun",
        "google-cloud-testutils",
        "mock",
        "psutil",
        "pytest",
        "pytest-cov",
        "pytz",
    )

    # Because we test minimum dependency versions on the minimum Python
    # version, the first version we test with in the unit tests sessions has a
    # constraints file containing all dependencies and extras.
    with open(
        CURRENT_DIRECTORY
        / "testing"
        / f"constraints-{UNIT_TEST_PYTHON_VERSIONS[0]}.txt",
        encoding="utf-8",
    ) as constraints_file:
        constraints_text = constraints_file.read()

    # Ignore leading whitespace and comment lines.
    deps = [
        match.group(1)
        for match in re.finditer(
            r"^\s*(\S+)(?===\S+)", constraints_text, flags=re.MULTILINE
        )
    ]

    # We use --no-deps to ensure that pre-release versions aren't overwritten
    # by the version ranges in setup.py.
    session.install(*deps)
    session.install("--no-deps", "-e", ".")

    # Print out prerelease package versions.
    session.run("python", "-m", "pip", "freeze")

    # Run all tests, except a few samples tests which require extra dependencies.
    session.run(
        "py.test",
        "--quiet",
        f"--junitxml=prerelease_unit_{session.python}_sponge_log.xml",
        os.path.join("tests", "unit"),
    )
    session.run(
        "py.test",
        "--quiet",
        f"--junitxml=prerelease_system_{session.python}_sponge_log.xml",
        os.path.join("tests", "system"),
    )


@nox.session(python=SYSTEM_TEST_PYTHON_VERSIONS)
def compliance(session):
    """Run the SQLAlchemy dialect-compliance system tests"""
    constraints_path = str(
        CURRENT_DIRECTORY / "testing" / f"constraints-{session.python}.txt"
    )
    system_test_folder_path = os.path.join("tests", "sqlalchemy_dialect_compliance")

    if os.environ.get("RUN_COMPLIANCE_TESTS", "true") == "false":
        session.skip("RUN_COMPLIANCE_TESTS is set to false, skipping")
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""):
        session.skip("Credentials must be set via environment variable")
    if os.environ.get("GOOGLE_API_USE_CLIENT_CERTIFICATE", "false") == "true":
        session.install("pyopenssl")
    if not os.path.exists(system_test_folder_path):
        session.skip("Compliance tests were not found")

    session.install("--pre", "grpcio")

    session.install(
        "mock",
        "pytest",
        "pytest-rerunfailures",
        "google-cloud-testutils",
        "-c",
        constraints_path,
    )
    if session.python == "3.8":
        extras = "[tests,alembic]"
    elif session.python == "3.10":
        extras = "[tests,geography]"
    else:
        extras = "[tests]"
    session.install("-e", f".{extras}", "-c", constraints_path)

    session.run(
        "py.test",
        "-vv",
        f"--junitxml=compliance_{session.python}_sponge_log.xml",
        "--reruns=3",
        "--reruns-delay=60",
        "--only-rerun=403 Exceeded rate limits",
        "--only-rerun=409 Already Exists",
        "--only-rerun=404 Not found",
        "--only-rerun=400 Cannot execute DML over a non-existent table",
        system_test_folder_path,
        *session.posargs,
    )


@nox.session(python=DEFAULT_PYTHON_VERSION)
def cover(session):
    """Run the final coverage report.

    This outputs the coverage report aggregating coverage from the unit
    test runs (not system test runs), and then erases coverage data.
    """
    session.install("coverage", "pytest-cov")
    session.run("coverage", "report", "--show-missing", "--fail-under=100")

    session.run("coverage", "erase")


@nox.session(python=DEFAULT_PYTHON_VERSION)
def docs(session):
    """Build the docs for this library."""

    session.install("-e", ".")
    session.install(
        "sphinx==4.0.1", "alabaster", "geoalchemy2", "shapely", "recommonmark"
    )

    shutil.rmtree(os.path.join("docs", "_build"), ignore_errors=True)
    session.run(
        "sphinx-build",
        "-W",  # warnings as errors
        "-T",  # show full traceback on exception
        "-N",  # no colors
        "-b",
        "html",
        "-d",
        os.path.join("docs", "_build", "doctrees", ""),
        os.path.join("docs", ""),
        os.path.join("docs", "_build", "html", ""),
    )


@nox.session(python=DEFAULT_PYTHON_VERSION)
def docfx(session):
    """Build the docfx yaml files for this library."""

    session.install("-e", ".")
    session.install(
        "sphinx==4.0.1",
        "alabaster",
        "geoalchemy2",
        "shapely",
        "recommonmark",
        "gcp-sphinx-docfx-yaml",
    )

    shutil.rmtree(os.path.join("docs", "_build"), ignore_errors=True)
    session.run(
        "sphinx-build",
        "-T",  # show full traceback on exception
        "-N",  # no colors
        "-D",
        (
            "extensions=sphinx.ext.autodoc,"
            "sphinx.ext.autosummary,"
            "docfx_yaml.extension,"
            "sphinx.ext.intersphinx,"
            "sphinx.ext.coverage,"
            "sphinx.ext.napoleon,"
            "sphinx.ext.todo,"
            "sphinx.ext.viewcode,"
            "recommonmark"
        ),
        "-b",
        "html",
        "-d",
        os.path.join("docs", "_build", "doctrees", ""),
        os.path.join("docs", ""),
        os.path.join("docs", "_build", "html", ""),
    )
