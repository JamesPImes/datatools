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
in it), using the default filenames provided by the COGCC website (which
encode the unique API number for each well).
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
import zipfile

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import date2num

__version__ = "0.0.2"
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
__website__ = "github.com/jamespimes"


def running_timeperiods(
        df, date_col, logic, day_col_header=None, month_col_header=None):
    """
    Count the running number of consecutive days and months during which
    a condition remains True. Specify the condition by passing a
    function as arg `logic=<function>` (the function should return a
    bool or value that can be interpreted as a bool in an `if`
    statement, and should operate on the values of each row).

    Returns a new DataFrame of the start dates and end dates of those
    periods of time, and the total days and months represented by each
    such period.

    Optionally add columns to the original DataFrame for the running
    totals by specifying the headers of args `day_col_header=<str>`
    and/or `month_col_header=<str>`.

    NOTE: Only works for a DataFrame where each row is a single month,
    represented by the first calendar day of the month, and where there
    are no gaps in time.

    :param df: A pandas DataFrame.
    :param date_col: The header of the column containing the dates to
    check against. (Should all be first day of month.)
    :param logic: A function (lambda or otherwise) to apply to each row
    to determine if that row falls within an intended gap. The function
    should return a boolean (or otherwise resolve to True or False in an
    `if` statement).  ex: `lambda row: row["OilProduced"] == 0`
    :param day_col_header: A string specifying the header for the added
    column for running day counts. If left as None, will not add the
    column.
    :param month_col_header: A string specifying the header for the
    added column for running month counts. If left as None, will not add
    the column.
    :return: A list of 2-tuples, being the start/end dates (inclusive)
    of the periods that match the provided logic.
    """

    days_col_new = []
    months_col_new = []
    days_counter = 0
    months_counter = 0
    start_of_new_gap = None
    prev_last_day = None
    gaps = []

    for _, row in df.iterrows():
        first_day = row[date_col]
        last_day = pd.to_datetime(get_last_day(first_day))
        if logic(row):
            days_counter += last_day.day
            months_counter += 1
            if start_of_new_gap is None:
                start_of_new_gap = first_day
        else:
            if start_of_new_gap is not None:
                gaps.append((start_of_new_gap, prev_last_day))
            days_counter = 0
            months_counter = 0
            start_of_new_gap = None
        days_col_new.append(days_counter)
        months_col_new.append(months_counter)
        prev_last_day = last_day

    if start_of_new_gap is not None:
        # In case the final row matches our `logic`, then it would mark
        # the final gap in our dataset, and need to handle add it to our
        # gaps list.
        gaps.append((start_of_new_gap, prev_last_day))

    # Add the columns, if requested
    if day_col_header is not None:
        df[day_col_header] = days_col_new
    if month_col_header is not None:
        df[month_col_header] = months_col_new

    gaps_df = pd.DataFrame({
        "start_date": [x[0] for x in gaps],
        "end_date": [x[1] for x in gaps]
    })

    total_months = []
    total_days = []
    if len(gaps_df) > 0:
        # Summarize the total months and days for the gaps dataframe
        total_months = (
                (gaps_df["end_date"].dt.year - gaps_df["start_date"].dt.year) * 12
                + (gaps_df["end_date"].dt.month - gaps_df["start_date"].dt.month) + 1
        )
        total_days = (gaps_df["end_date"] - gaps_df["start_date"]).dt.days + 1

    gaps_df["total_months"] = total_months
    gaps_df["total_days"] = total_days

    return gaps_df


def get_last_day(dt):
    """
    From a Datetime object, find the last calendar day of that month,
    returned as a string 'YYYY-MM-DD'.
    """
    _, last_day = monthrange(dt.year, dt.month)
    return f"{dt.year}-{str(dt.month).rjust(2, '0')}-{str(last_day).rjust(2, '0')}"


def output_gaps_as_string(df, header="Gaps:", threshold_days=0):
    """
    Clean up and output the contents of a DataFrame that was returned by
    the running_timeperiods() function as a single string, with the
    specified header, and limited to those periods that are at least as
    long as the specified number of `threshold_days`.
    """
    lines = []
    for _, row in df.iterrows():
        if row["total_days"] >= threshold_days:
            s = " -- {} days ({} months)".format(
                row["total_days"],
                row["total_months"]
            ).ljust(26, ' ')

            s = s + "::  {} -- {}".format(
                row['start_date'].isoformat()[:10],
                row['end_date'].isoformat()[:10]
            )

            lines.append(s)

    if len(lines) == 0:
        lines = [" -- None that meet the threshold."]

    return header + f"\n[[at least {threshold_days} days in length]]\n" + "\n".join(lines)


def main():
    # ----------------------------------------------
    # Get directory from command-line arg
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-dir", "--d", type=str, required=True,
        help="The filepath to the directory containing the relevant .xls files")
    parser.add_argument(
        "-launch", "--l", action="store_true",
        help="Specify to open the output directory after analysis is complete")
    parser.add_argument(
        "-zip", "--z", action="store_true",
        help="Specify to add source .xls files to .zip file in output directory")
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

        # Store filename, file creation time (i.e. when it was pulled from COGCC),
        # and the full filepath.
        ctime = pd.to_datetime(os.path.getctime(fp), unit="s").isoformat()[:10]
        api_nums[api_num] = (f, ctime, fp)

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

    # # Add the last day of each month.
    # prod_gb["LastOfMonth"] = pd.to_datetime(prod_gb["FirstOfMonth"].apply(get_last_day))

    # -----------------------------------------------
    # Analyze gaps in production, and periods where wells are only shut-in

    # The conditions we'll check against.
    logic_base = {
        "production_raw": lambda row: (
                (row["GasProduced"] + row["OilProduced"]) == 0
        ),
        "production": lambda row: (
                (row["GasProduced"] + row["OilProduced"]) == 0
                and not row["any_well_shutin"]
        ),
        "shutin": lambda row: (
                (row["GasProduced"] + row["OilProduced"]) == 0
                and row["any_well_shutin"]
        )
    }

    # Production gaps, where shut-in does NOT count as production
    nonprods_raw_df = running_timeperiods(
        prod_gb, date_col="FirstOfMonth", logic=logic_base["production_raw"],
        day_col_header="days_nonprod_raw", month_col_header="months_nonprod_raw")

    # Production gaps, where shut-in DOES count as production
    nonprods_df = running_timeperiods(
        prod_gb, date_col="FirstOfMonth", logic=logic_base["production"],
        day_col_header="days_nonprod", month_col_header="months_nonprod")

    # Production gaps that are otherwise filled by at least one shut-in well
    shutin_df = running_timeperiods(
        prod_gb, date_col="FirstOfMonth", logic=logic_base["shutin"],
        day_col_header="days_shutin", month_col_header="months_shutin")

    # -----------------------------------------------
    # Save our results to files.

    # Dump DataFrames to csv
    prod_gb.to_csv(SAVE_DIR + "production_summary.csv")
    nonprods_raw_df.to_csv(SAVE_DIR + "production_gaps_raw.csv")
    nonprods_df.to_csv(SAVE_DIR + "production_gaps.csv")
    shutin_df.to_csv(SAVE_DIR + "shutin_periods.csv")

    # Write .txt file summary
    with open(SAVE_DIR + "production_gaps_summary.txt", "w") as file:

        file.write(
            "Production from {} through {}...\n\n".format(
                FIRST_MONTH.isoformat()[:7], LAST_MONTH.isoformat()[:7]))

        file.write(output_gaps_as_string(
            nonprods_raw_df, "Gaps in production (raw):", MIN_DAYS_PRODUCTION_CESSATION))
        file.write("\n\n")

        file.write(output_gaps_as_string(
            nonprods_df, "Gaps in production (with shut-ins counting as production):",
            MIN_DAYS_PRODUCTION_CESSATION))
        file.write("\n\n")

        file.write(output_gaps_as_string(
            shutin_df, "Periods of shut-in:", MIN_DAYS_SHUTIN_PERIOD))
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
        file.write(f"<{__email__}>\n")
        file.write(f"<{__website__}>")

    # Generate and save a simple graph of production, highlighting the gaps
    fig, ax = plt.subplots()
    gas_color = "green"
    oil_color = "blue"
    highlight_color = "red"
    gas_al = 1.0
    oil_al = 0.6

    title = "Total Verified Production {} to {}".format(
        FIRST_MONTH.isoformat()[:7], LAST_MONTH.isoformat()[:7])
    ax.set_title(title)

    y_vals = prod_gb["FirstOfMonth"]

    ax.plot(y_vals, prod_gb["GasProduced"], color=gas_color, alpha=gas_al)
    ax2 = ax.twinx()
    ax2.plot(y_vals, prod_gb["OilProduced"], color=oil_color, alpha=oil_al)

    ax.set_xlabel("Time (year)")
    ax.set_ylabel("Gas produced (Mcf)", color=gas_color)
    ax2.set_ylabel("Oil produced (Bbls)", color=oil_color)

    lb = "Production Gaps (raw)"
    for _, row in nonprods_raw_df.iterrows():
        sd = row["start_date"]
        ed = row["end_date"]
        d1 = date2num(datetime(sd.year, sd.month, sd.day))
        d2 = date2num(datetime(ed.year, ed.month, ed.day))
        ax.axvspan(d1, d2, color=highlight_color, alpha=0.3, label=lb)
        lb = None

    ax.legend()

    fig.savefig(SAVE_DIR + "production_graph.png")

    if args["z"]:
        with zipfile.ZipFile(SAVE_DIR + "source.zip", "w") as zipper:
            for v in api_nums.values():
                zipper.write(v[2], arcname=v[0])

    if args["l"]:
        os.startfile(SAVE_DIR)

    print(f"Success. Results saved to {SAVE_DIR}.")


if __name__ == '__main__':
    main()
