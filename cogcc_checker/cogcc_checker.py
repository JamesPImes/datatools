# Copyright (c) 2021, James P. Imes. All rights reserved.

"""
----- COGCC Checker -----
A command-line application to analyze one or more .xls files downloaded
from the public records of the Colorado Oil & Gas Conservation
Commission (COGCC) website at <https://cogcc.state.co.us/#/home> to
quickly see if there has been consistent production of oil and/or gas to
apparently "hold" an oil and gas lease.

To use:
1) For each relevant well, navigate to its scout card on the COGCC
website and download the production records in .xls format.
2) Save the .xls spreadsheets to a common directory (with nothing else
in it).
3) From command line:
    py cogcc_checker.py -dir "<filepath to directory>"

For background:
A typical oil and gas lease will expire after some amount of time unless
oil and/or gas is actively 'produced' from the lands it covers (i.e.
taken from one or more wells). The expiration can be pushed back by the
company that owns the lease by [a] resuming production (i.e. reopening
a gas well, assuming it is still capable of producing gas) within the
period defined in the lease (often 90 days since last production), or
[b] by paying "shut-in" royalties -- basically, a placeholder payment
'as though' production was actually occurring. (Or by other means not
contemplated in this data.)

This program will help visualize when wells may have been shut-in, or
when production otherwise ceased, to help investigate whether the
provisions of a given lease have been met.

# TODO: features to add:
-- Customize the minimum gap in production before bothering to flag it.
-- Customize the minimum shut-in period before bothering to flag it.
-- Optional filtering out of certain formations for certain wells.
-- Customize the minimum Bbls of oil or Mcf of gas to count as
actual production within each well, and within all wells.
-- Customize the date range to consider.
-- Automate downloading .xls files from COGCC?
"""

import os
import argparse
from calendar import monthrange
from datetime import datetime
import textwrap

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import date2num

__version__ = "0.0.1"
__disclaimer__ = (
    "Note that there are certain limitations to the COGCC's records. "
    "For example, production is reported on a month-by-month basis. "
    "For this program, any production in a given month is assumed to "
    "have occurred across the entire month, which means that gaps in "
    "production and shut-in periods are likely to be slightly larger "
    "than calculated by this program (i.e. potentially a handful or "
    "more days during the preceding and following calendar months that "
    "cannot be definitively captured due to the COGCC data). "
    "For this reason and others, use this program and its results at "
    "your own risk."
)
__author__ = "James P. Imes"
__email__ = "jamesimes@gmail.com"


def find_shutin_periods(df, add_columns=True):
    """
    Add columns (in-situ) for running consecutive months and days of
    shut-in periods (if `add_columns=True`, on by default). Return a
    list of 2-tuples of all shut-in periods.
    :param df:
    :param add_columns: Whether to add the appropriate columns to the
    DataFrame. Defaults to `True`.
    :return: A list of 2-tuples (being start/end months, inclusive, of
    months)
    """
    days_si = []
    months_si = []
    days_si_counting = 0
    months_si_counting = 0
    last_si_date = None
    si_start_stops = []

    for row in df.iterrows():
        first_day = row[1]["FirstOfMonth"]
        last_day = row[1]["LastOfMonth"]
        if row[1]["any_well_shutin"] and not row[1]["any_well_active"]:
            days_si_counting += last_day.day
            months_si_counting += 1
            if last_si_date is None:
                last_si_date = first_day
        else:
            if last_si_date is not None:
                si_start_stops.append((last_si_date, first_day))
            days_si_counting = 0
            months_si_counting = 0
            last_si_date = None
        days_si.append(days_si_counting)
        months_si.append(months_si_counting)

        # TODO: In case it is most recently shut-in... Capture that.

    if add_columns:
        # Add a column for how many days / months it's been shut-in.
        df["months_si"] = months_si
        df["days_si"] = days_si

    return si_start_stops


def find_production_gaps(df, raw=False, add_columns=True):
    """
    Add columns (in-situ) for running consecutive months and days of
    non-production (if `add_columns=True`, which is on by default).
    Return a list of 2-tuples of all periods of non-production.
    Optionally consider shut-in months as producing with `raw=False`
    (which is default behavior).
    :param df:
    :param add_columns: Whether to add the appropriate columns to the
    DataFrame. Defaults to `True`.
    :param raw: If True, will include periods that were covered by
    shut-in wells. If False, will be those periods with no production
    where the wells were also not shut-in. (Defaults to False.)
    :return: A list of 2-tuples (being start/end months, inclusive, of
    months)
    """
    days_nonprod = []
    months_nonprod = []
    days_nonprod_counting = 0
    months_nonprod_counting = 0
    last_prod_stop = None
    prod_stop_starts = []

    for row in df.iterrows():
        first_day = row[1]["FirstOfMonth"]
        last_day = row[1]["LastOfMonth"]
        no_prod = row[1]["GasProduced"] + row[1]["OilProduced"] == 0
        if no_prod and (raw or not row[1]["any_well_shutin"]):
            days_nonprod_counting += last_day.day
            months_nonprod_counting += 1
            if last_prod_stop is None:
                last_prod_stop = first_day
        else:
            if last_prod_stop is not None:
                prod_stop_starts.append((last_prod_stop, first_day))
            days_nonprod_counting = 0
            months_nonprod_counting = 0
            last_prod_stop = None
        days_nonprod.append(days_nonprod_counting)
        months_nonprod.append(months_nonprod_counting)

        # TODO: In case it is most recently not producing... Capture that.

    if add_columns:
        # Add a column for how many days / months since there was production.
        df[f"months_nonprod{'_raw' * raw}"] = months_nonprod
        df[f"days_nonprod{'_raw' * raw}"] = days_nonprod

    return prod_stop_starts


def get_last_day(dt):
    """
    From a Datetime object, find the last calendar day of that month,
    returned as a string 'YYYY-MM-DD'.
    """
    _, last_day = monthrange(dt.year, dt.month)
    return f"{dt.year}-{str(dt.month).rjust(2, '0')}-{str(last_day).rjust(2, '0')}"


def output_gaps(timestamps: list, header="Gaps:", threshold=0):
    """
    Cull the list of 2-tuples containing Timestamp objects, down to
    those gaps greater than or equal to the number of days specified as
    `threshold`. Output a clean string.
    :param timestamps: A list of 2-tuples, as output by
    find_production_gaps() or find_shutin_periods().
    :param header: Title to print atop the string.
    :param threshold: Number of days.
    :return: A clean string.
    """
    threshold = pd.Timedelta(threshold, unit="day")
    outs = [x for x in timestamps if lambda x: (x[1] - x[0]) >= threshold]
    lines = [
        " -- {}{} -- {}".format(
            (str((out[1] - out[0]).days) + " days:").ljust(12, " "),
            out[0].isoformat()[:10],
            out[1].isoformat()[:10]
        )
        for out in outs
    ]
    if not lines:
        lines = [f" -- [None that meet the threshold of {threshold.days} days.]"]

    return header + "\n" + "\n".join(lines)


def main():
    # ----------------------------------------------
    # Get directory from command-line arg
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-dir", "--d", type=str, required=True,
        help="The filepath to the directory containing the relevant .xls files")
    parser.add_argument(
        '-launch', '--launch', action='store_true',
        help="Open the output directory after analyzing")
    args = vars(parser.parse_args())

    DIR = args["d"] + "\\"
    SAVE_DIR = DIR + r"COGCC_checker_results\\"
    os.makedirs(SAVE_DIR, exist_ok=True)

    # TODO: Add cmd-line args for these:
    MIN_DAYS_PRODUCTION_CESSATION = 0
    MIN_DAYS_SHUTIN_PERIOD = 0

    # TODO: List API's > download the data direct from the COGCC. Possible
    #  to automate?

    # -----------------------------------------------
    # Populate our DataFrame from raw COGCC .xls data

    # load each xls into the df.
    files = os.listdir(DIR)
    dfs = []
    api_nums = {}
    for f in files:
        if not f.lower().endswith(".xls"):
            continue
        fp = DIR + f
        df = pd.read_excel(fp, parse_dates=["FirstOfMonth"])
        api_num = "05-" + f[:9]
        df["api_num"] = api_num
        dfs.append(df)

        # Store filename and file creation time (i.e. when it was pulled from COGCC)
        ctime = pd.to_datetime(os.path.getctime(fp), unit="s").isoformat()[:10]
        api_nums[api_num] = (f, ctime)

    # concatenate the dataframes into a single production dataframe
    prod = pd.concat(dfs, ignore_index=True)

    # Ensure there are no months missing from the data.
    FIRST_MONTH = prod["FirstOfMonth"].min()
    LAST_MONTH = prod["FirstOfMonth"].max()
    every_month = pd.DataFrame()
    every_month["FirstOfMonth"] = pd.date_range(
        start=FIRST_MONTH, end=LAST_MONTH, freq="MS")
    prod = pd.concat([prod, every_month], ignore_index=True)

    # -----------------------------------------------
    # Groupby / cull down to relevant data.

    # Determine whether each well was in "PR" or "SI" status in a given month.
    prod["any_well_active"] = prod["WellStatus"] == "PR"
    prod["any_well_shutin"] = prod["WellStatus"] == "SI"

    # These columns are identical duplicates for calculating sums
    prod["num_wells_producing"] = prod["WellStatus"] == "PR"
    prod["num_wells_shutin"] = prod["WellStatus"] == "SI"

    # The relevant fields for our HBP analyses
    fields = [
        "DaysProduced",
        "OilProduced",
        "GasProduced",
        "any_well_active",
        "any_well_shutin",
        "num_wells_producing",
        "num_wells_shutin"
    ]

    aggfuncs = {
        "DaysProduced": "max",
        "OilProduced": "sum",
        "GasProduced": "sum",
        "any_well_active": "max",
        "any_well_shutin": "max",
        "num_wells_producing": "sum",
        "num_wells_shutin": "sum"
    }

    prod_gb = prod.groupby("FirstOfMonth", as_index=False)[fields].agg(aggfuncs)

    # Add the last day of each month.
    prod_gb["LastOfMonth"] = pd.to_datetime(prod_gb["FirstOfMonth"].apply(get_last_day))

    # -----------------------------------------------
    # Analyze gaps in production, and periods where wells are only shut-in

    # TODO: Bugfix. `nonprods` (i.e. `raw=True`) is not functioning properly
    nonprods = find_production_gaps(prod_gb)
    nonprods_raw = find_production_gaps(prod_gb, raw=True)
    si = find_shutin_periods(prod_gb)

    # -----------------------------------------------
    # Save our results to files.

    # Dump DataFrame to csv
    prod_gb.to_csv(SAVE_DIR + "production_summary.csv")

    # Write .txt file summary
    with open(SAVE_DIR + "production_gaps_summary.txt", "w") as file:

        file.write(
            "Production from {} through {}...\n\n".format(
                FIRST_MONTH.isoformat()[:10], LAST_MONTH.isoformat()[:10]))

        file.write(output_gaps(
            nonprods_raw, "Gaps in production (raw):",
            MIN_DAYS_PRODUCTION_CESSATION))
        file.write("\n\n")

        file.write(output_gaps(
            nonprods, "Gaps in production (with shut-ins counting as production):",
            MIN_DAYS_PRODUCTION_CESSATION))
        file.write("\n\n")

        file.write(output_gaps(
            si, "Periods of shut-in:", MIN_DAYS_SHUTIN_PERIOD))
        file.write("\n\n")

        file.write("Considering wells...\n")
        for k, v in api_nums.items():
            file.write(f" -- {k}")
            file.write(f"\n      Accessed {v[1]}" + " " * 4 + f"<{v[0]}>\n")

        file.write("\n\n")
        file.write("\n".join(textwrap.wrap(__disclaimer__)))
        file.write("\n\n")
        file.write(f"Generated by COGCC Checker, version {__version__}\n")
        file.write(f"Copyright (c) 2021, {__author__}.\n")
        file.write(f"<{__email__}>")

    # Generate and save a simple graph of production, highlighting the gaps
    fig, ax = plt.subplots()
    gas_color = "red"
    oil_color = "blue"
    al = 0.6

    title = "Total Verified Production {} to {}".format(
        FIRST_MONTH.isoformat()[:7], LAST_MONTH.isoformat()[:7])
    ax.set_title(title)

    y_vals = prod_gb["FirstOfMonth"]

    ax.plot(y_vals, prod_gb["GasProduced"], color=gas_color, alpha=al)
    ax2 = ax.twinx()
    ax2.plot(y_vals, prod_gb["OilProduced"], color=oil_color, alpha=al)

    ax.set_xlabel("Time (year)")
    ax.set_ylabel("Gas produced (Mcf)", color=gas_color)
    ax2.set_ylabel("Oil produced (Bbls)", color=oil_color)

    lb = "Production Gaps (raw)"
    for gap in nonprods_raw:
        d1 = date2num(datetime(gap[0].year, gap[0].month, gap[0].day))
        d2 = date2num(datetime(gap[1].year, gap[1].month, gap[1].day))
        ax.axvspan(d1, d2, color="aqua", alpha=0.3, label=lb)
        lb = None

    ax.legend(loc=0)

    fig.savefig(SAVE_DIR + "production_graph.png")

    if args["launch"]:
        os.startfile(SAVE_DIR)

    print(f"Success. Results saved to {SAVE_DIR}.")


if __name__ == '__main__':
    main()
