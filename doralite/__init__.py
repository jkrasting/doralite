import cftime
import io
import json
import math
import nc_time_axis
import numpy as np
import os
import sqlite3
import pandas as pd
import requests

import matplotlib.pyplot as plt
import gzip
from io import BytesIO
import urllib3
import intake_esm
import datetime
import subprocess

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

api = "https://dora.gfdl.noaa.gov/"

from . import frepp


def proxy(status=True, url="http://localhost:3128"):
    """Sets https proxy"""
    if status is True:
        os.environ["HTTPS_PROXY"] = url
    if status is False:
        if "HTTPS_PROXY" in os.environ.keys():
            os.environ.pop("HTTPS_PROXY")


def dora_metadata(expid):
    query = api + "api/info?id=" + str(expid)
    try:
        x = requests.get(url=query).content
    except:
        x = requests.get(url=query, verify=False).content
    x = json.loads(x)
    x["pathHistory"] = x["pathPP"].replace("/pp", "/history")
    return x


def catalog_raw(expid, decompress=True):
    query = api + "api/catalog?id=" + str(expid) + "&compressed=true"
    try:
        x = requests.get(url=query).content
    except:
        x = requests.get(url=query, verify=False).content
    x = BytesIO(x)
    if decompress is True:
        with gzip.GzipFile(fileobj=x, mode='rb') as f:
            content = f.read()
    else:
        content = x
    return content


def catalog(expid):
    df = pd.read_csv(catalog_raw(expid, decompress=False), compression="gzip", low_memory=False)
    exp = dora_metadata(expid)
    for key in [
        "source_id",
        "experiment_id",
        "frequency",
        "table_id",
        "grid_label",
        "realm",
        "member_id",
        "chunk_freq",
    ]:
        df[key] = df[key].fillna("unknown")
    
    esmcat_memory = {
        "esmcat": {  # <== Metadata only here
            "esmcat_version": "0.0.1",
            "attributes": [
                {"column_name": "activity_id", "vocabulary": "", "required": False},
                {"column_name": "institution_id", "vocabulary": "", "required": False},
                {"column_name": "source_id", "vocabulary": "", "required": False},
                {"column_name": "experiment_id", "vocabulary": "", "required": True},
                {
                    "column_name": "frequency",
                    "vocabulary": "https://raw.githubusercontent.com/NOAA-GFDL/CMIP6_CVs/master/CMIP6_frequency.json",
                    "required": True,
                },
                {"column_name": "realm", "vocabulary": "", "required": True},
                {"column_name": "table_id", "vocabulary": "", "required": False},
                {"column_name": "member_id", "vocabulary": "", "required": False},
                {"column_name": "grid_label", "vocabulary": "", "required": False},
                {"column_name": "variable_id", "vocabulary": "", "required": True},
                {"column_name": "time_range", "vocabulary": "", "required": True},
                {"column_name": "chunk_freq", "vocabulary": "", "required": False},
                {"column_name": "platform", "vocabulary": "", "required": False},
                {"column_name": "target", "vocabulary": "", "required": False},
                {
                    "column_name": "cell_methods",
                    "vocabulary": "",
                    "required": False,
                },  # Adjusted from "enhanced" -> False
                {"column_name": "path", "vocabulary": "", "required": True},
                {
                    "column_name": "dimensions",
                    "vocabulary": "",
                    "required": False,
                },  # Adjusted from "enhanced" -> False
                {"column_name": "version_id", "vocabulary": "", "required": False},
                {
                    "column_name": "standard_name",
                    "vocabulary": "",
                    "required": False,
                },  # Adjusted from "enhanced" -> False
            ],
            "assets": {
                "column_name": "path",
                "format": "netcdf",
                "format_column_name": None,
            },
            "aggregation_control": {
                "variable_column_name": "variable_id",
                "groupby_attrs": [
                    "source_id",
                    "experiment_id",
                    "frequency",
                    "table_id",
                    "grid_label",
                    "realm",
                    "member_id",
                    "chunk_freq",
                ],
                "aggregations": [
                    {"type": "union", "attribute_name": "variable_id", "options": {}},
                    {
                        "type": "join_existing",
                        "attribute_name": "time_range",
                        "options": {
                            "dim": "time",
                            "coords": "minimal",
                            "compat": "override",
                        },
                    },
                ],
            },
            "id": exp["expName"],
            "description": exp["expName"],
            "title": exp["expName"],
            "last_updated": datetime.datetime.now().isoformat(),
            "catalog_file": "dummy.csv",
        },
        "df": df,  # <== Your loaded DataFrame
    }
    
    return intake_esm.esm_datastore(esmcat_memory)


def dmget(files):
    files = [files] if not isinstance(files,list) else files
    cmd = ["dmget"]+files
    _ = subprocess.check_output(cmd) 


def search(string, attribute="pathPP"):
    """Returns dictionary of an attribute keyed by the id of experiments
    matching "string".

    By default, the returned attribute is the post-processing path ("pathPP")
    but others such as "pathDB", "pathAnalysis" and "expName" are allowed.
    If no match is found an empty dictionary is returned."""
    query = api + "api/search?search=" + str(string)
    try:
        x = json.loads(requests.get(url=query).content)
    except:
        x = json.loads(requests.get(url=query, verify=False).content)
    return dict((int(k), x[k][attribute]) for k in x.keys())


def list_project(project_name):
    """Returns dictionary of an attribute keyed by the id of experiments
    matching "string".

    By default, the returned attribute is the post-processing path ("pathPP")
    but others such as "pathDB", "pathAnalysis" and "expName" are allowed.
    If no match is found an empty dictionary is returned."""
    query = api + "api/list?project_name=" + str(project_name)
    try:
        x = json.loads(requests.get(url=query).content)
    except:
        x = json.loads(requests.get(url=query, verify=False).content)
    return x


def global_mean_data(
    expid,
    component,
):
    """
    Fetches global means from central server
    """

    query_dict = {}
    query_dict["id"] = str(expid)
    query_dict["component"] = component

    query = []
    for q in iter(query_dict):
        query.append("=".join((q, query_dict[q])))

    query = "&".join(query)
    query = api + "api/data?" + query

    try:
        x = requests.get(url=query).content
    except:
        x = requests.get(url=query, verify=False).content

    x = x.decode("utf-8")

    return x
