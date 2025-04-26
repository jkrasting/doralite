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


def dora_catalog(expid):
    query = api + "api/catalog?id=" + str(expid) + "&compressed=true"
    try:
        x = requests.get(url=query).content
    except:
        x = requests.get(url=query, verify=False).content
    with gzip.GzipFile(fileobj=BytesIO(x), mode='rb') as f:
        content = f.read()
    return content


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
