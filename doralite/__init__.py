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
    """
    Returns a dictionary of metadata for a given GFDL experiment ID or project ID.

    ARGUMENTS
    ----------
    expid [int or str] -- GFDL-wide master experiment ID or a project-specific ID
        expid can be an int or a purely numeric string, in which case it is interpreted
        as a GFDL-wide master experiment ID, or a string containing a project name (e.g.
        "cm5") followed by a "-" and then a project-specific ID number string (e.g. "1").

    RETURNS
    -------
    dict

    EXAMPLES
    -------
    >>> pdict = doralite.dora_metadata(1767)
    >>> pp = doralite.dora_metadata("odiv-209")['pathPP']
    """
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
    return pd.read_csv(catalog_raw(expid, decompress=False), compression="gzip", low_memory=False)


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
    """Returns dictionary containing all categories of experiments within
    a GFDL project.

    ARGUMENTS
    ---------
    project_name [str] -- GFDL project_name, e.g. "odiv" or "cm5"

    EXAMPLES
    --------
    >>> project_list = doralite.list_project("odiv")['project']
    >>> project_titles = [project["title"] for project in project_list]
    >>> project_titles
    ['OM5 experiments',
     'OMIP 0.125 experiments',
     'OMIP 0.25 experiments',
     'OMIP 0.5 experiments',
     'JRA-55do experiments',
     'CORE2 IAF experiments',
     'CM4 experiments',
     'MOM6 regional experiments',
     'AMIP experiments',
     'DECK experiments']
    
    >>> om5_experiments = project_list[project_titles.index('OM5 experiments')]['experiments']
    >>> om5_expNames = [expdict['expName'] for expdict in om5_experiments]
    >>> master_ids = [expdict['master_id'] for expdict in om5_experiments]
    >>> list(np.array(om5_expNames)[np.argsort(master_ids)][0:10])
    ['om5_b00_cycle1_c4',
     'om5_b00_cycle1_c5',
     'om5_b00_nonBouss_c4',
     'om5_b00_coorconfigALE',
     'BUG_om5_b00_BBL_VS_KV0_MStrC1p25_minThk_Bodner0p03_c5',
     'om5_b00_ePBLbisect',
     'om5_b00_nonSymmetric',
     'BUG_om5_b00_BBL_VS_KV0_MStrC1p25_minThk_Bodner0p03_c4',
     'om5_b00_cycle1_c4_repro',
     'om5_b00_nonBouss_c5']
    """

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
