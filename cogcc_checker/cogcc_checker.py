# Copyright (c) 2021-2022, James P. Imes. All rights reserved.

"""
----- COGCC Checker -----
A command-line application to analyze one or more .xlsx files downloaded
from the public records of the Colorado Oil & Gas Conservation
Commission (COGCC) website at <https://cogcc.state.co.us/#/home> to
quickly see if there has been consistent production of oil and/or gas to
apparently "hold" an oil and gas lease.

To use:
1) For each relevant well, navigate to its scout card on the COGCC
website and download the production records in .xls or .xlsx format.
2) Save the .xlsx spreadsheets to a common directory (with nothing else
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


CHANGELOG:
-- v0.0.3: Now also accepts '.xlsx' files (because the COGCC has started
    exporting to that file format).
-- v0.1.0: Spun out main analysis functionality to production_checker
    package.


# TODO: features to add:
-- Customize the minimum gap in production before bothering to flag it.
-- Customize the minimum shut-in period before bothering to flag it.
-- Optional filtering out of certain formations for certain wells.
-- Customize the minimum Bbls of oil or Mcf of gas to count as
actual production within each well, and within all wells.
-- Customize the date range to consider.
-- Automate downloading .xlsx files from COGCC?
"""

import os
import argparse
import textwrap
import zipfile

import pandas as pd

from production_checker import ProductionChecker

__version__ = "0.1.0"
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
        if not f.lower().endswith((".xls", ".xlsx")):
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
    prod_checker = ProductionChecker(
        prod,
        date_col='FirstOfMonth',
        oil_prod_col='OilProduced',
        gas_prod_col='GasProduced',
        days_produced_col='DaysProduced',
        status_col='WellStatus',
        shutin_codes=['SI'],
        oil_prod_min=0,
        gas_prod_min=0
    )

    # Production gaps, where shut-in does NOT count as production
    nonprods_raw_df = prod_checker.gaps_by_production_threshold(
        new_days_col='days_nonprod_raw',
        new_months_col='months_nonprod_raw')

    # Production gaps, where shut-in DOES count as production
    nonprods_df = prod_checker.gaps_by_production_threshold(
        shutin_as_producing=True,
        new_days_col='days_nonprod',
        new_months_col='months_nonprod')

    # Production gaps, where shut-in DOES count as production
    nonprods_days_df = prod_checker.gaps_by_producing_days(
        shutin_as_producing=True,
        new_days_col='days_nonprod_by_days',
        new_months_col='months_nonprod_by_days')

    # Production gaps that are otherwise filled by at least one shut-in well
    shutin_df = prod_checker.periods_of_shutin(
        consider_production=True,
        new_days_col='days_shutin',
        new_months_col='months_shutin')

    # -----------------------------------------------
    # Save our results to files.

    # Dump DataFrames to csv
    prod_checker.prod_df.to_csv(SAVE_DIR + "production_summary.csv")
    nonprods_raw_df.to_csv(SAVE_DIR + "production_gaps_raw.csv")
    nonprods_df.to_csv(SAVE_DIR + "production_gaps.csv")
    shutin_df.to_csv(SAVE_DIR + "shutin_periods.csv")

    # Write .txt file summary
    with open(SAVE_DIR + "production_gaps_summary.txt", "w") as file:

        file.write(
            f"Production from {prod_checker.first_month.isoformat()[:7]} "
            f"through {prod_checker.last_month.isoformat()[:7]}...\n\n")

        file.write(prod_checker.output_gaps_as_string(
            nonprods_raw_df, "Gaps in production (raw):", MIN_DAYS_PRODUCTION_CESSATION))
        file.write("\n\n")

        file.write(prod_checker.output_gaps_as_string(
            nonprods_df, "Gaps in production (with shut-ins counting as production):",
            MIN_DAYS_PRODUCTION_CESSATION))
        file.write("\n\n")

        by_days_header = (
            "Gaps in production (with shut-ins counting as production, "
            "using stated producing days per month **):"
        )
        file.write(prod_checker.output_gaps_as_string(
            nonprods_days_df, by_days_header,
            MIN_DAYS_PRODUCTION_CESSATION))
        clarification = (
            "\n** Assumes all unproducing days occur consecutively, and "
            "at the start or end of the month."
        )
        file.write(clarification)
        file.write("\n\n")

        file.write(prod_checker.output_gaps_as_string(
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
        file.write(f"Copyright (c) 2021-2022, {__author__}.\n")
        file.write(f"<{__email__}>\n")
        file.write(f"<{__website__}>")

    # Generate and save a simple graph of production, highlighting the gaps.
    graph_fp = SAVE_DIR + "production_graph.png"
    prod_checker.generate_graph(nonprods_raw_df, graph_fp)

    if args["z"]:
        with zipfile.ZipFile(SAVE_DIR + "source.zip", "w") as zipper:
            for v in api_nums.values():
                zipper.write(v[2], arcname=v[0])

    if args["l"]:
        os.startfile(SAVE_DIR)

    print(f"Success. Results saved to {SAVE_DIR}.")


if __name__ == '__main__':
    main()
