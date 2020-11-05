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

api = "https://dora-dev.gfdl.noaa.gov/cgi-bin/analysis/"


def proxy(status=True, url="http://localhost:3128"):
    """Sets https proxy"""
    if status is True:
        os.environ["HTTPS_PROXY"] = url
    if status is False:
        if "HTTPS_PROXY" in os.environ.keys():
            os.environ.pop("HTTPS_PROXY")


def _remove_trend(x, y, order=1, anomaly=True, return_coefs=False, coefs=None):
    """Internal function to remove a linear trend"""
    if None in list(y):
        return None
    if coefs is None:
        idx = np.isfinite(x) & np.isfinite(y)
        coefs = np.polyfit(x[idx], y[idx], order)
        if return_coefs is True:
            return coefs
    model = np.poly1d(coefs)
    fit = model(x)
    if anomaly is True:
        return y - fit
    else:
        fit = fit - fit[0]
        return y - fit


def _calc_trend(x, y, order=1):
    """Internal function to calculate trend line/curve"""
    idx = np.isfinite(x) & np.isfinite(y)
    coefs = np.polyfit(x[idx], y[idx], order)
    model = np.poly1d(coefs)
    return model(x)


def _remove_reference_trend(t, x, other, anomaly=True):
    """Removes trends from a reference dataset"""
    if str(x.name) not in list(other.columns):
        return None
    else:
        _coefs = other[x.name].to_numpy()
        return _remove_trend(t, x, anomaly=anomaly, coefs=_coefs)


def reformat_time_axis(ax=None):
    """Reformats x-axis labels to YYYY format"""
    if ax is None:
        ax = plt.gca()
    labels = [x.get_text() for x in ax.xaxis.get_ticklabels()]
    labels = [x.split("-")[0] for x in labels]
    _ = ax.set_xticklabels(labels)


class DoraDataFrame(pd.DataFrame):
    # temporary properties
    _internal_names = pd.DataFrame._internal_names + ["internal_cache"]
    _internal_names_set = set(_internal_names)

    # normal properties
    _metadata = ["added_property"]

    @property
    def _constructor(self):
        return DoraDataFrame

    def smooth(self, window, extrap=False):
        _df = self.rolling(window, center=True).mean()
        if extrap is True:
            _df.fillna(method="ffill", inplace=True)
            _df.fillna(method="bfill", inplace=True)
        return _df

    def detrend(self, reference=None, order=1, anomaly=True, return_coefs=False):
        tindex = np.array(
            [
                cftime.date2num(x, "days since 0001-01-01", calendar="noleap")
                for x in self.index
            ]
        )
        if reference is not None:
            if order != 1:
                print(
                    "Only a linear trend can be removed based on another dataset. Setting order to 1."
                )
                order = 1
            coefs = reference.detrend(order=order, return_coefs=True)
            return self.apply(
                lambda x: (_remove_reference_trend(tindex, x, coefs, anomaly=anomaly))
            )
        else:
            return self.apply(
                lambda x: (
                    _remove_trend(
                        tindex,
                        x,
                        order=order,
                        anomaly=anomaly,
                        return_coefs=return_coefs,
                    )
                )
            )

    def trend(self, order=1, anomaly=True):
        tindex = np.array(
            [
                cftime.date2num(x, "days since 0001-01-01", calendar="noleap")
                for x in self.index
            ]
        )
        return self.apply(lambda x: (_calc_trend(tindex, x, order=order)))


class timeseries:
    def __init__(
        self,
        f,
        var,
        scale=1.0,
        multiply_by_area=False,
        legacy_land=False,
        start=None,
        end=None,
    ):
        con = sqlite3.connect(f)
        cur = con.cursor()
        if legacy_land is True:
            res = cur.execute("SELECT year,sum FROM " + var)
        else:
            res = cur.execute("SELECT year,value FROM " + var)
        results = cur.fetchall()
        self.t, self.data = zip(*results)
        self.t = np.array(self.t)
        if multiply_by_area is True:
            res = cur.execute("SELECT name FROM sqlite_master where TYPE='table'")
            tables = [str(record[0]) for record in cur.fetchall()]
            if "cell_measure" in tables:
                res = cur.execute(
                    "SELECT value FROM cell_measure where var='" + var + "'"
                )
                cell_measure = cur.fetchone()[0]
            else:
                cell_measure = "area"
            res = cur.execute("SELECT value FROM " + cell_measure)
            area = np.array(cur.fetchall()).squeeze()
            scale = area * scale
        self.data = np.array(self.data) * scale
        cur.close()
        con.close()
        if start is not None:
            idx = [i for i, val in enumerate(self.t) if val >= start]
            self.t = self.t[idx]
            self.data = self.data[idx]
        else:
            start = self.t.min()
        if end is not None:
            idx = [i for i, val in enumerate(self.t) if val <= end]
            self.t = self.t[idx]
            self.data = self.data[idx]
        else:
            end = self.t.max() + 1
        A = set(np.arange(start, end)) - set(self.t)
        if len(list(A)) != 0:
            print("# WARNING: Timeseries is incomplete for " + var, A)
        self.dict = dict(zip(self.t, self.data))


def dora_metadata(expid):
    query = api + "meta.py?id=" + str(expid)
    try:
        x = requests.get(url=query).content
    except:
        x = requests.get(url=query, verify=False).content
    x = json.loads(x)
    x["pathHistory"] = x["pathPP"].replace("/pp", "/history")
    return x


def search(string, attribute="pathPP"):
    """Returns dictionary of an attribute keyed by the id of experiments
    matching "string".

    By default, the returned attribute is the post-processing path ("pathPP")
    but others such as "pathDB", "pathAnalysis" and "expName" are allowed.
    If no match is found an empty dictionary is returned."""
    query = api + "search.py?search=" + str(string)
    try:
        x = json.loads(requests.get(url=query).content)
    except:
        x = json.loads(requests.get(url=query, verify=False).content)
    return dict((int(k), x[k][attribute]) for k in x.keys())


def csv_to_pd(csv, comment="#", delim_whitespace=False, metadata=None):
    """Function to convert csv output to Pandas Dataframe"""
    df = pd.read_csv(csv, comment=comment, delim_whitespace=delim_whitespace)
    df["year"] = cftime.num2date(
        (df.year * 365.0) - (365.0 / 2.0) - 1,
        "days since 0001-01-01",
        calendar="365_day",
    )
    df.rename(columns={"year": "date"}, inplace=True)
    df.set_index("date", inplace=True)
    df = DoraDataFrame(df)
    if metadata is not None:
        df.id = metadata["id"]
        df.title = metadata["expName"]
    return df


def read_db(
    dbfile, variables=None, yearshift=0.0, legacy_land=False, start=None, end=None,
):
    """Function to read sqlite dbfile"""

    if variables is None:
        conn = sqlite3.connect(dbfile)
        c = conn.cursor()
        sql = "SELECT name FROM sqlite_master WHERE type='table'"
        sqlres = c.execute(sql)
        variables = [str(record[0]) for record in c.fetchall()]
        c.close()
        conn.close()
        removes = ["units", "long_name", "cell_measure"]
        variables = [x for x in variables if x not in removes]

    # -- Loop over variables
    data = {}
    years = []
    skipped = []
    for n, var in enumerate(variables):
        try:
            ts = timeseries(dbfile, var, legacy_land=legacy_land, start=start, end=end)
            if len(ts.t) > 0:
                data[var] = ts.data
                years = years + list(ts.t)
        except:
            skipped.append(var)

    years = list(set(years))
    years = [x + float(yearshift) for x in years]

    variables = list(set(variables) - set(skipped))

    if start is None:
        start = -1 * math.inf

    if end is None:
        end = math.inf

    df = pd.DataFrame(data, index=years)
    df = df[(df.index >= start) & (df.index <= end)]
    df.index = cftime.num2date(
        (df.index * 365.0) - (365.0 / 2.0) - 1,
        "days since 0001-01-01",
        calendar="365_day",
    )
    df = DoraDataFrame(df)
    return df


def load_c4mip(locator, obgc="COBALT", legacy=False, start=None, end=None):
    """Loads C4MIP formatted data for an experiment"""
    if isinstance(locator, int):
        dfA = global_mean_data(locator, "globalAveAtmos", start=start, end=end)
        dfL = global_mean_data(
            locator, "globalAveLand", legacy_land=legacy, start=start, end=end
        )
        dfO = global_mean_data(locator, f"globalAve{obgc}", start=start, end=end)
    else:
        dfA = read_db(locator + "/globalAveAtmos.db", start=start, end=end)
        dfL = read_db(
            locator + "/globalAveLand.db", legacy_land=legacy, start=start, end=end
        )
        dfO = read_db(locator + f"/globalAve{obgc}.db", start=start, end=end)
    return c4mip(dfA, dfL, dfO, legacy=legacy)


def c4mip(dfAtmos, dfLand, dfOcean, validation_diags=False, legacy=False):
    """Returns a dataframe in C4MIP units"""
    df = pd.DataFrame()
    if "t_ref" in list(dfAtmos.columns):
        df["ST"] = dfAtmos["t_ref"]

    if "XCO2" in list(dfLand.columns):
        df["CO2"] = dfAtmos["XCO2"] * 1.0e6

    if legacy:
        if "fco2_ca" in list(dfLand.columns):
            df["LA_CO2_FLUX"] = dfLand["fco2_ca"] * 86400.0 * 365.0 * 1.0e-12 * -1.0
    else:
        if "fco2" in list(dfLand.columns):
            df["LA_CO2_FLUX"] = (
                dfLand["fco2"] * dfLand["land_area"] * 86400.0 * 365.0 * 1.0e-12 * -1.0
            )

    if "dic_stf_gas" in list(dfOcean.columns):
        df["OA_CO2_FLUX"] = (
            dfOcean["dic_stf_gas"] * dfOcean["area"] * 86400.0 * 365.0 * 12.0 * 1.0e-15
        )

    if not legacy:
        if "" in list(dfLand.columns):
            df["GPP"] = dfLand["gpp"] * dfLand["land_area"] * 1.0e-12
        if "" in list(dfLand.columns):
            df["NPP"] = dfLand["npp"] * dfLand["land_area"] * 1.0e-12
        if "" in list(dfLand.columns):
            df["HETRES"] = dfLand["rsoil"] * dfLand["land_area"] * 1.0e-12
        if "" in list(dfLand.columns):
            df["CLIVE"] = dfLand["btot"] * dfLand["land_area"] * 1.0e-12
        if "" in list(dfLand.columns):
            df["CDEAD"] = dfLand["tot_soil_C"] * dfLand["land_area"] * 1.0e-12
        if validation_diags is True:
            df["csmoke_rate"] = dfLand["csmoke_rate"] * dfLand["land_area"] * 1.0e-12
            df["cwlitt_C"] = dfLand["cwlitt_C"] * dfLand["land_area"] * 1.0e-12
            df["lflitt_C"] = dfLand["lflitt_C"] * dfLand["land_area"] * 1.0e-12

    return DoraDataFrame(df)


def global_mean_data(
    expids,
    component,
    varlist=None,
    start=None,
    end=None,
    yearshift=None,
    output="dataframe",
    showquery=False,
):
    """
    Fetches global means from central server
    """

    if not isinstance(expids, list):
        expids = [expids]

    dfs = []
    combined_csv = io.StringIO()
    for expid in expids:
        query_dict = {}
        query_dict["id"] = str(expid)
        query_dict["component"] = component
        if start is not None:
            query_dict["start"] = str(start)
        if end is not None:
            query_dict["end"] = str(end)
        if yearshift is not None:
            query_dict["yearshift"] = str(yearshift)

        query = []
        for q in iter(query_dict):
            query.append("=".join((q, query_dict[q])))

        query = "&".join(query)
        query = api + "api.py?" + query

        if component == "c4mip":
            query = api + "c4mip.py?id=" + str(expid)

        if showquery:
            print(query)

        try:
            x = requests.get(url=query).content
        except:
            x = requests.get(url=query, verify=False).content

        combined_csv.write(x.decode("utf8"))
        combined_csv.write("\n\n")

        if output == "dataframe":
            if component == "c4mip":
                df = pd.read_csv(
                    io.StringIO(x.decode("utf8")),
                    delim_whitespace=True,
                    metadata=dora_metadata(expid),
                )
                df.set_index("YEAR", inplace=True)
            else:
                df = csv_to_pd(
                    io.StringIO(x.decode("utf8")), metadata=dora_metadata(expid)
                )
            dfs.append(df)

    if output == "csv":
        return combined_csv
    else:
        if len(dfs) > 1:
            df = pd.concat(dfs, sort=False)
        else:
            df = dfs[0]
        return df
