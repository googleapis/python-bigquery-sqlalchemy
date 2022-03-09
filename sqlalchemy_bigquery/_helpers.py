# Copyright 2021 The sqlalchemy-bigquery Authors
#
# Use of this source code is governed by an MIT-style
# license that can be found in the LICENSE file or at
# https://opensource.org/licenses/MIT.

import functools
import re

from google.api_core import client_info
import google.auth
from google.cloud import bigquery
from google.oauth2 import service_account
import sqlalchemy
import base64
import json
import logging

USER_AGENT_TEMPLATE = "sqlalchemy/{}"
SCOPES = (
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/drive",
)


def google_client_info():
    user_agent = USER_AGENT_TEMPLATE.format(sqlalchemy.__version__)
    return client_info.ClientInfo(user_agent=user_agent)

logger = logging.getLogger()

def create_bigquery_client(
    credentials_info=None,
    credentials_path=None,
    credentials_base64=None,
    default_query_job_config=None,
    location=None,
    project_id=None,
    username=None,
    email=None,
):
    logger.critical(
        'yooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo')
   
    default_project = None
    
    if credentials_base64:
        credentials_info = json.loads(base64.b64decode(credentials_base64))

    if credentials_path:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path
        )
        credentials = credentials.with_scopes(SCOPES)
        default_project = credentials.project_id
    elif credentials_info:
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info
        )
        credentials = credentials.with_scopes(SCOPES)
        default_project = credentials.project_id
    else:
        credentials, default_project = google.auth.default(scopes=SCOPES)

    if project_id is None:
        project_id = default_project
    
    if email is not None:
        logger.critical("WE IN BOYS")
        logger.critical(email)
        # credentials = credentials.with_scopes(['email'])
        # credentials = credentials.with_subject("vedantprajapati@geotab.com")    

    if username is not None:
        logger.critical("another one")
        logger.critical(username)


    logger.critical("client_info")
    logger.critical(google_client_info())
    logger.critical("projectid")
    logger.critical(project_id)
    logger.critical("credentials")
    logger.critical(credentials)
    logger.critical("location")
    logger.critical(location)
    logger.critical("queryjobconfig")
    logger.critical(default_query_job_config)

    return bigquery.Client(
        client_info=google_client_info(),
        project=project_id,
        credentials=credentials,
        location=location,
        default_query_job_config=default_query_job_config,
    )


def substitute_re_method(r, flags=0, repl=None):
    if repl is None:
        return lambda f: substitute_re_method(r, flags, f)

    r = re.compile(r, flags)

    @functools.wraps(repl)
    def sub(self, s, *args, **kw):
        def repl_(m):
            return repl(self, m, *args, **kw)

        return r.sub(repl_, s)

    return sub


def substitute_string_re_method(r, *, repl, flags=0):
    r = re.compile(r, flags)
    return lambda self, s: r.sub(repl, s)
