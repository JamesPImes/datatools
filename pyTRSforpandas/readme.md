# A few tools for expanding [pyTRS](https://github.com/JamesPImes/pyTRS) functionality to pandas.

(This readme does not go into much detail on pyTRS functionality. For more information on the PLSS and how pyTRS parses land descriptions, see the [pyTRS docs](https://github.com/JamesPImes/pyTRS).)

#### `parse_plssdesc()` -- parsing PLSS descriptions into Tracts (and into lots and QQs)

```
# Create a DataFrame from a .csv file with PLSS land descriptions in a column
# with the header "land_desc"
sample_df = pd.read_csv(r"C:\land\sample_land_descriptions.csv")

# Parse the PLSS descriptions into a new DataFrame, with rows added as needed,
# such that there is one parsed tract per row. (`config=` is optional; here,
# we're using "n,w,segment" config param -- see docs on pyTRS.Config objects
# for possible parameters)
parsed_df = parse_plssdescs(sample_df, plss_col="land_desc", config="n,w,segment")

# Print the TRS, description block, lots, and quarter-quarters ("QQs"), all as
# parsed by pyTRS
print(parsed_df[["trs", "desc", "lots", "qqs"]])
```

#### `parse_tracts()` -- parsing Tracts into lots and QQs

A tract in this context means a description that has already been separated from its Twp/Rge/Sec. For example, `"Lots 1, 2, 3, S/2NE/4"` or `"S/2"` in `"Lots 1, 2, 3, S/2NE/4 of Section 1, S/2 of Section 2, T154N-R97W"`. (Many databases already have this data in separate columns.)

```
# Create a DataFrame from a .csv file with tract descriptions in a column
# with the header "tract_desc"
sample_df = pd.read_csv(r"C:\land\sample_land_descriptions.csv")

# Parse the tracts into lots and QQs into the same DataFrame. (`config=` is
# optional again.)
parse_tracts(sample_df, tract_col="tract_desc", config="cleanQQ")

# Print the lots and quarter-quarters ("QQs"), as parsed by pyTRS
print(sample_df[["lots", "qqs"]])
```

#### `filter_by_trs()` -- filter a DataFrame by Township/Range/Section (TRS)

(Even if the PLSS land descriptions in the dataframe contain multiple TRS's, and if that data has not been parsed.)

*__Note:__* TRS must be specified in the format used by pyTRS, being a single string consisting of three parts:

-- Twp specified as __up to__ 3 digits (use fewer if appropriate), plus the direction as a lowercase `'n'` or `'s'` (e.g. `'154n'` or `'6s'`)

-- Rge specified as __up to__ 3 digits (use fewer if appropriate), plus the direction as a lowercase `'e'` or `'w'` (e.g. `'97w'` or `'101e'`)

-- Section specified as __exactly__ 2 digits (e.g., `'14'` or `'02'`)

... so, for example, `'154n97w14'` (for Section 14 of T154N-R97W) of `'6s101e02'` (for Section 2, T6S-R101E).

```
# Create a DataFrame from a .csv file with PLSS land descriptions in a column
# with the header "land_desc"
sample_df = pd.read_csv(r"C:\land\sample_land_descriptions.csv")

# To filter for a single Twp/Rge/Sec, pass `trs` as a string
filtered_df_single = filter_by_trs(
    sample_df, plss_col="land_desc", trs="154n97w14")

# To filter for multiple TRS, pass a list as `trs`...
relevant_trs_list = ["154n97w14", "6s101e02"]

# Filter the dataframe to those whose land descriptions include at least one of the
# TRS's in the list...
filtered_df_including = filter_by_trs(
    sample_df, plss_col="land_desc", trs=relevant_trs_list)

# Use the `include=False` arg to filter for those descriptions that contain NONE of the
# Twp/Rge/Sec's passed as `trs`.
filtered_df_excluding = filter_by_trs(
    sample_df, plss_col="land_desc", trs=relevant_trs_list, include=False)
```

For this method, a match of __any__ TRS will count as a positive match, both for purposes of inclusive filter (`include=True`) and exclusive filter (`include=False`).
