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
import warnings

import xarray as xr


# Suppress ONLY FutureWarnings
warnings.simplefilter(action="ignore", category=FutureWarning)

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


def is_overlapping(period_a, period_b):
    start_a, end_a = period_a
    start_b, end_b = period_b
    return start_a < end_b and end_a > start_b


def infer_av_files(cat, subcat):
    avlist = [
        "ann",
        "01",
        "02",
        "03",
        "04",
        "05",
        "06",
        "07",
        "08",
        "09",
        "10",
        "11",
        "12",
    ]
    for var in subcat.vars:
        _subcat = cat.search(variable_id=var)
        for realm in _subcat.realms:
            varentry = _subcat.search(realm=realm).df.iloc[0]
            df = cat.search(variable_id=avlist).df
            df = df[df["path"].str.contains(f"/{realm}/")]
            for k in [
                "source_id",
                "experiment_id",
                "frequency",
                "realm",
                "variable_id",
            ]:
                df[k] = varentry[k]
            df["cell_methods"] = "av"
            df["standard_name"] = varentry["standard_name"]
            df["chunk_freq"] = df["chunk_freq"].str.replace("monthly_", "", regex=False)
            df["chunk_freq"] = df["chunk_freq"].str.replace("annual_", "", regex=False)
            df = df.reindex()
            _subcat = _subcat.merge(df_to_cat(df))
    return _subcat


def process_time_string(tstring):
    if isinstance(tstring, tuple):
        try:
            for x in tstring:
                assert isinstance(x, datetime.datetime) or x is None
            timetup = tstring
        except:
            timetup = (None, None)
    else:
        try:
            tstring = str(tstring)
            timetup = [x.ljust(8, "0") for x in tstring.split("-")]
            timetup = [[x[0:4], x[4:6], x[6:8]] for x in timetup]
            timetup[0][0] = int(timetup[0][0])
            timetup[0][1] = 1 if timetup[0][1] == "00" else int(timetup[0][1])
            timetup[0][2] = 1 if timetup[0][2] == "00" else int(timetup[0][2])
            timetup[1][0] = int(timetup[1][0])
            timetup[1][1] = 12 if timetup[1][1] == "00" else int(timetup[1][1])
            timetup[1][2] = 31 if timetup[1][2] == "00" else int(timetup[1][2])
            timetup = [tuple(x) for x in timetup]
            timetup = tuple([datetime.datetime(*x) for x in timetup])
        except:
            timetup = (None, None)

    return timetup


def load_dora_catalog(idnum, **kwargs):
    return Dora_datastore(catalog(idnum).__dict__["_captured_init_args"][0], **kwargs)


class Dora_datastore(intake_esm.core.esm_datastore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def source_catalog(self):
        return self.__dict__["_captured_init_args"][0]

    def find(
        self,
        var=None,
        freq=None,
        kind=None,
        trange=None,
        infer_av=True,
        preferred_realm=None,
        preferred_chunkfreq=None,
    ):
        res = self

        if var is not None:
            res = res.search(variable_id=var)
        if infer_av is True:
            res = infer_av_files(self, res)

        if freq is not None:
            res = res.search(frequency=freq)

        if kind is not None:
            assert kind in ["av", "ts", "both"], "kind must be 'av, 'ts', or 'both'"
        else:
            kind = "both"
        kind = ["av", "ts"] if kind == "both" else [kind]
        res = res.search(cell_methods=kind)

        if trange is not None:
            res = res.datetime()
            res = res.tsel(trange)

        if preferred_realm is not None:
            _realm = " "
            for x in preferred_realm:
                if x in res.realms:
                    _realm = x
                    break
            res = res.search(realm=_realm)

        if preferred_chunkfreq is not None:
            _chunk_freq = " "
            for x in preferred_chunkfreq:
                if x in res.chunk_freqs:
                    _chunk_freq = x
                    break
            res = res.search(chunk_freq=_chunk_freq)

        return res

    def tsel(self, trange):
        _source = self.source_catalog()
        df = self.df.copy()
        trange = list(trange)
        trange = [x.split("-") for x in trange]
        trange[0] = datetime.datetime(*tuple([int(x) for x in trange[0]]))
        trange[1] = datetime.datetime(*tuple([int(x) for x in trange[1]]))
        trange = tuple(trange)
        non_matching_times = []
        for index, row in df.iterrows():
            if not is_overlapping(trange, row["time_range"]):
                non_matching_times.append(index)
        df = df.drop(non_matching_times)
        _source["df"] = df
        return Dora_datastore(_source)

    def datetime(self):
        _source = self.source_catalog()
        df = self.df.copy()
        df["time_range"] = df["time_range"].apply(process_time_string)
        _source["df"] = df
        return Dora_datastore(_source)

    def merge(self, catalogs):
        _source = self.source_catalog()
        if iter(catalogs):
            if isinstance(catalogs, intake_esm.core.esm_datastore):
                catalogs = [catalogs]
            elif isinstance(catalogs, Dora_datastore):
                catalogs[catalogs]
            else:
                catalogs = list(catalogs)
        else:
            raise ValueError("input must be an iterable object")
        catalogs = [self] + catalogs
        _ids = [x.__dict__["_captured_init_args"][0]["esmcat"]["id"] for x in catalogs]
        _dfs = [x.df for x in catalogs]
        label = _ids[0] if all(x == _ids[0] for x in _ids) else ""
        _source["df"] = pd.concat(_dfs)
        _source["id"] = label
        _source["description"] = label
        _source["title"] = label
        return Dora_datastore(_source)

    def info(self, attr):
        return sorted(list(set(list(self.df[attr]))))

    def to_xarray(self, dmget=True):
        assert len(self.df) > 0, "No datasets to open."

        try:
            assert not len(self.realms) > 1
        except:
            raise ValueError(
                f"More than one realm is present in the catalog. Filter the catalog further. {self.realms}"
            )

        try:
            assert not len(self.chunk_freqs) > 1
        except:
            raise ValueError(
                f"More than one chunk frequency is present in the catalog. Filter the catalog further. {self.chunk_freqs}"
            )

        _paths = sorted(self.df["path"].tolist())
        if dmget is True:
            call_dmget(_paths)

        ds = xr.open_mfdataset(_paths, use_cftime=True)

        alltimes = sorted([t for x in list(self.df["time_range"].values) for t in x])
        ds.attrs["time_range"] = f"{alltimes[0].isoformat()},{alltimes[-1].isoformat()}"

        return ds

    def to_momgrid(self, dmget=True, to_xarray=True):
        res = mg.Gridset(self.to_xarray(dmget=dmget))
        if to_xarray:
            res = res.data
        return res

    @property
    def realms(self):
        return self.info("realm")

    @property
    def vars(self):
        return self.info("variable_id")

    @property
    def chunk_freqs(self):
        return self.info("chunk_freq")


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
        with gzip.GzipFile(fileobj=x, mode="rb") as f:
            content = f.read()
    else:
        content = x
    return content


def catalog(expid):
    df = pd.read_csv(
        catalog_raw(expid, decompress=False), compression="gzip", low_memory=False
    )
    exp = dora_metadata(expid)
    return df_to_cat(df, label=exp["expName"])


def df_to_cat(df, label=""):
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
            "id": label,
            "description": label,
            "title": label,
            "last_updated": datetime.datetime.now().isoformat(),
            "catalog_file": "dummy.csv",
        },
        "df": df,  # <== Your loaded DataFrame
    }

    return intake_esm.esm_datastore(esmcat_memory)


def call_dmget(files):
    files = [files] if not isinstance(files, list) else files
    cmd = ["dmget"] + files
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
