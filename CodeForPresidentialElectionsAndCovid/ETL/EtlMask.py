import pandas as pd
import numpy as np
from pathlib import Path

from .EtlElection import *

def getCountyPopulationMask():
    # Read the persidential election CSV from local disk
    population_df = pd.read_csv(
        r"../DataForPresidentialElectionsAndCovid/Dataset 3 Population Estimate through 2020/County Data Till 2020 co-est2020-alldata.csv",
        encoding="latin-1",
    )


    county_pop_df = population_df[population_df["SUMLEV"] == 50].copy()
    county_pop_df["COUNTYFP"] = county_pop_df["STATE"].astype("str").str.pad(
        2, "left", "0"
    ) + county_pop_df["COUNTY"].astype("str").str.pad(3, "left", "0")
    county_pop_df["COUNTYFP"] = county_pop_df["COUNTYFP"].astype("int")
    county_pop_df = county_pop_df[
        [
            "STATE",
            "COUNTYFP",
            "CTYNAME",
            "POPESTIMATE2016",
            "POPESTIMATE2020",
            "RNETMIG2020",
        ]
    ]

    county_mask_df = pd.read_csv(
        r"https://raw.githubusercontent.com/nytimes/covid-19-data/master/mask-use/mask-use-by-county.csv"
    )
    # county_mask_df.to_csv( r"../DataForPresidentialElectionsAndCovid/Dataset 7 Covid/mask-use-by-county.csv")
    county_pop_mask_df = pd.merge(
        county_pop_df, county_mask_df, how="right", on=["COUNTYFP"], indicator=True
    )
    return county_pop_mask_df


##########################################################################################
def createFrequentAndInfrequentMaskUsers():
    # Add up groupings of frequent and non frequent
    county_pop_mask_df = getCountyPopulationMask()
    election_winners_df = getElectionSegmentsData()
    county_pop_mask_melt_df = county_pop_mask_df.melt(
        id_vars=[
            "STATE",
            "COUNTYFP",
            "CTYNAME",
            "POPESTIMATE2016",
            "POPESTIMATE2020",
            "RNETMIG2020",
        ],
        value_vars=["NEVER", "RARELY", "SOMETIMES", "FREQUENTLY", "ALWAYS"],
        var_name="mask_usage_type",
        value_name="mask_usage",
        col_level=None,
        ignore_index=True,
    )

    # Create two groups
    county_pop_mask_melt_df["mask_usage_type"] = pd.Series(
        np.where(
            county_pop_mask_melt_df["mask_usage_type"].isin(["ALWAYS", "FREQUENTLY"]),
            "FREQUENT",
            "NOT FREQUENT",
        )
    )

    # Add up the new groupings of FREQUENT AND NON FREQUENT
    county_pop_mask_melt_df = (
        county_pop_mask_melt_df.groupby(
            [
                "STATE",
                "COUNTYFP",
                "CTYNAME",
                "POPESTIMATE2016",
                "POPESTIMATE2020",
                "RNETMIG2020",
                "mask_usage_type",
            ]
        )["mask_usage"]
        .sum()
        .reset_index()
    )

    changes_df = election_winners_df[
        election_winners_df.changecolor.isin(
            [TO_DEMOCRAT, TO_REPUBLICAN, STAYED_DEMOCRAT, STAYED_REPUBLICAN]
        )
    ][["COUNTYFP", "changecolor"]]
    county_pop_mask_melt_df = county_pop_mask_melt_df.merge(
        changes_df, how="inner", on="COUNTYFP"
    )
    return county_pop_mask_melt_df


##########################################################################################
def createDataForFreqAndInFreqMaskUse():
    """[This function creates three dataframes]

    Returns:
        [Pandas dataframes]: [A consolidated dataframe, frequent mask usage dataframe and infrequent mask usage dataframe]
    """
    county_pop_mask_df = createFrequentAndInfrequentMaskUsers()
    county_pop_mask_df["segmentname"] = county_pop_mask_df["changecolor"].map(
        color_segment_dict
    )
    county_pop_mask_df.segmentname = county_pop_mask_df.segmentname.str.replace(
        "Stayed ", ""
    )
    county_pop_mask_df.segmentname = county_pop_mask_df.segmentname.str.replace(
        "To ", ""
    )

    county_pop_mask_df = county_pop_mask_df[
        ["STATE", "COUNTYFP", "CTYNAME", "mask_usage_type", "mask_usage", "segmentname"]
    ].copy()
    county_pop_mask_df["mask_usage_range"] = county_pop_mask_df["mask_usage"].apply(
        lambda x: getMaskUsageRange(x)
    )

    county_pop_mask_df["range_color"] = county_pop_mask_df[
        ["segmentname", "mask_usage_range"]
    ].apply(
        lambda x: getColorRangeMaskUsage(x["segmentname"], x["mask_usage_range"]),
        axis=1,
    )

    county_pop_mask_freq_df = county_pop_mask_df[
        county_pop_mask_df["mask_usage_type"] == "FREQUENT"
        ].copy()
    county_pop_mask_infreq_df = county_pop_mask_df[
        county_pop_mask_df["mask_usage_type"] == "NOT FREQUENT"
        ].copy()
    return county_pop_mask_df, county_pop_mask_freq_df, county_pop_mask_infreq_df