.. Generated by synthtool. DO NOT EDIT!
############
Contributing
############

#. **Please sign one of the contributor license agreements below.**
#. Fork the repo, develop and test your code changes, add docs.
#. Make sure that your commit messages clearly describe the changes.
#. Send a pull request. (Please Read: `Faster Pull Request Reviews`_)

.. _Faster Pull Request Reviews: https://github.com/kubernetes/community/blob/master/contributors/guide/pull-requests.md#best-practices-for-faster-reviews

.. contents:: Here are some guidelines for hacking on the Google Cloud Client libraries.

***************
Adding Features
***************

In order to add a feature:

- The feature must be documented in both the API and narrative
  documentation.

- The feature must work fully on the following CPython versions:
  3.9, 3.10, 3.11, 3.12, and 3.13 on both UNIX and Windows.

- The feature must not add unnecessary dependencies (where
  "unnecessary" is of course subjective, but new dependencies should
  be discussed).

****************************
Using a Development Checkout
****************************

You'll have to create a development environment using a Git checkout:

- While logged into your GitHub account, navigate to the
  ``python-bigquery-sqlalchemy`` `repo`_ on GitHub.

- Fork and clone the ``python-bigquery-sqlalchemy`` repository to your GitHub account by
  clicking the "Fork" button.

- Clone your fork of ``python-bigquery-sqlalchemy`` from your GitHub account to your local
  computer, substituting your account username and specifying the destination
  as ``hack-on-python-bigquery-sqlalchemy``.  E.g.::

   $ cd ${HOME}
   $ git clone git@github.com:USERNAME/python-bigquery-sqlalchemy.git hack-on-python-bigquery-sqlalchemy
   $ cd hack-on-python-bigquery-sqlalchemy
   # Configure remotes such that you can pull changes from the googleapis/python-bigquery-sqlalchemy
   # repository into your local repository.
   $ git remote add upstream git@github.com:googleapis/python-bigquery-sqlalchemy.git
   # fetch and merge changes from upstream into main
   $ git fetch upstream
   $ git merge upstream/main

Now your local repo is set up such that you will push changes to your GitHub
repo, from which you can submit a pull request.

To work on the codebase and run the tests, we recommend using ``nox``,
but you can also use a ``virtualenv`` of your own creation.

.. _repo: https://github.com/googleapis/python-bigquery-sqlalchemy

Using ``nox``
=============

We use `nox <https://nox.readthedocs.io/en/latest/>`__ to instrument our tests.

- To test your changes, run unit tests with ``nox``::
    $ nox -s unit

- To run a single unit test::

    $ nox -s unit-3.13 -- -k <name of test>


  .. note::

    The unit tests and system tests are described in the
    ``noxfile.py`` files in each directory.

.. nox: https://pypi.org/project/nox/

*****************************************
I'm getting weird errors... Can you help?
*****************************************

If the error mentions ``Python.h`` not being found,
install ``python-dev`` and try again.
On Debian/Ubuntu::

  $ sudo apt-get install python-dev

************
Coding Style
************
- We use the automatic code formatter ``black``. You can run it using
  the nox session ``blacken``. This will eliminate many lint errors. Run via::

   $ nox -s blacken

- PEP8 compliance is required, with exceptions defined in the linter configuration.
  If you have ``nox`` installed, you can test that you have not introduced
  any non-compliant code via::

   $ nox -s lint

- In order to make ``nox -s lint`` run faster, you can set some environment
  variables::

   export GOOGLE_CLOUD_TESTING_REMOTE="upstream"
   export GOOGLE_CLOUD_TESTING_BRANCH="main"

  By doing this, you are specifying the location of the most up-to-date
  version of ``python-bigquery-sqlalchemy``. The
  remote name ``upstream`` should point to the official ``googleapis``
  checkout and the branch should be the default branch on that remote (``main``).

- This repository contains configuration for the
  `pre-commit <https://pre-commit.com/>`__ tool, which automates checking
  our linters during a commit.  If you have it installed on your ``$PATH``,
  you can enable enforcing those checks via:

.. code-block:: bash

   $ pre-commit install
   pre-commit installed at .git/hooks/pre-commit

Exceptions to PEP8:

- Many unit tests use a helper method, ``_call_fut`` ("FUT" is short for
  "Function-Under-Test"), which is PEP8-incompliant, but more readable.
  Some also use a local variable, ``MUT`` (short for "Module-Under-Test").

********************
Running System Tests
********************

- To run system tests, you can execute::

   # Run all system tests
   $ nox -s system

   # Run a single system test
   $ nox -s system-3.13 -- -k <name of test>


  .. note::

      System tests are only configured to run under Python 3.9, 3.12, and 3.13.
      For expediency, we do not run them in older versions of Python 3.

  This alone will not run the tests. You'll need to change some local
  auth settings and change some configuration in your project to
  run all the tests.

- System tests will be run against an actual project. You should use local credentials from gcloud when possible. See `Best practices for application authentication <https://cloud.google.com/docs/authentication/best-practices-applications#local_development_and_testing_with_the>`__. Some tests require a service account. For those tests see `Authenticating as a service account <https://cloud.google.com/docs/authentication/production>`__.

*************
Test Coverage
*************

- The codebase *must* have 100% test statement coverage after each commit.
  You can test coverage via ``nox -s cover``.

******************************************************
Documentation Coverage and Building HTML Documentation
******************************************************

If you fix a bug, and the bug requires an API or behavior modification, all
documentation in this package which references that API or behavior must be
changed to reflect the bug fix, ideally in the same commit that fixes the bug
or adds the feature.

Build the docs via:

   $ nox -s docs

*************************
Samples and code snippets
*************************

Code samples and snippets live in the `samples/` catalogue. Feel free to
provide more examples, but make sure to write tests for those examples.
Each folder containing example code requires its own `noxfile.py` script
which automates testing. If you decide to create a new folder, you can
base it on the `samples/snippets` folder (providing `noxfile.py` and
the requirements files).

The tests will run against a real Google Cloud Project, so you should
configure them just like the System Tests.

- To run sample tests, you can execute::

   # Run all tests in a folder
   $ cd samples/snippets
   $ nox -s py-3.9

   # Run a single sample test
   $ cd samples/snippets
   $ nox -s py-3.9 -- -k <name of test>

********************************************
Note About ``README`` as it pertains to PyPI
********************************************

The `description on PyPI`_ for the project comes directly from the
``README``. Due to the reStructuredText (``rst``) parser used by
PyPI, relative links which will work on GitHub (e.g. ``CONTRIBUTING.rst``
instead of
``https://github.com/googleapis/python-bigquery-sqlalchemy/blob/main/CONTRIBUTING.rst``)
may cause problems creating links or rendering the description.

.. _description on PyPI: https://pypi.org/project/sqlalchemy-bigquery


*************************
Supported Python Versions
*************************

We support:

-  `Python 3.9`_
-  `Python 3.10`_
-  `Python 3.11`_
-  `Python 3.12`_
-  `Python 3.13`_

.. _Python 3.9: https://docs.python.org/3.9/
.. _Python 3.10: https://docs.python.org/3.10/
.. _Python 3.11: https://docs.python.org/3.11/
.. _Python 3.12: https://docs.python.org/3.12/
.. _Python 3.13: https://docs.python.org/3.13/


Supported versions can be found in our ``noxfile.py`` `config`_.

.. _config: https://github.com/googleapis/python-bigquery-sqlalchemy/blob/main/noxfile.py


We also explicitly decided to support Python 3 beginning with version 3.9.
Reasons for this include:

-  Encouraging use of newest versions of Python 3
-  Taking the lead of `prominent`_ open-source `projects`_
-  `Unicode literal support`_ which allows for a cleaner codebase that
   works in both Python 2 and Python 3

.. _prominent: https://docs.djangoproject.com/en/1.9/faq/install/#what-python-version-can-i-use-with-django
.. _projects: http://flask.pocoo.org/docs/0.10/python3/
.. _Unicode literal support: https://www.python.org/dev/peps/pep-0414/

**********
Versioning
**********

This library follows `Semantic Versioning`_.

.. _Semantic Versioning: http://semver.org/

Some packages are currently in major version zero (``0.y.z``), which means that
anything may change at any time and the public API should not be considered
stable.

******************************
Contributor License Agreements
******************************

Before we can accept your pull requests you'll need to sign a Contributor
License Agreement (CLA):

- **If you are an individual writing original source code** and **you own the
  intellectual property**, then you'll need to sign an
  `individual CLA <https://developers.google.com/open-source/cla/individual>`__.
- **If you work for a company that wants to allow you to contribute your work**,
  then you'll need to sign a
  `corporate CLA <https://developers.google.com/open-source/cla/corporate>`__.

You can sign these electronically (just scroll to the bottom). After that,
we'll be able to accept your pull requests.
