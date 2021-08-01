import pandas as pd
import numpy as np
from pathlib import Path

from .EtlElection import *



color_segment_dict = {
    TO_OTHER: "To other",
    TO_DEMOCRAT: "To Democrat",
    TO_REPUBLICAN: "To Republican",
    STAYED_DEMOCRAT: "Stayed Democrat",
    STAYED_REPUBLICAN: "Stayed Republican",
    STAYED_OTHER: "Stayed Other",
}

DataFolder = Path("../DataForPresidentialElectionsAndCovid/")


########################################################################################
def getRollingCaseAverageSegmentLevel():
    """
        THIS FUNCTION 1- Obtains COVID cases and deaths per 100k at the county level. Questions: From when?
                      2- Obtains presidential election results at the county level, showing the winning
                         party and whether the winning party changed between 2016 and 2020 (indicated by color).
                      3- Merges the two on county.
                      4- Groups by date and color (see getElectionSegmentsData() for color definitions).
                      5- Aggregates to get mean number of cases per 100k at the segment level.
                      6- Adds a column for the name of the color segment.
            
        Functions called: getCasesRollingAveragePer100K(), getElectionSegmentsData()
        Called by: Main code
        
        Input arguments: None
        Returns: Dataframe 'case_rolling_df' with columns:
        
                 date
                 changecolor            (change in political affiliation, if any)
                 cases_avg_per_100k
                 segmentname            (name for changecolor)

    """

    # Get rolling averages data
    case_rolling_df = getCasesRollingAveragePer100K()

    # Questions: Limiting to data before start of 2021. Meaning of next comment??
    ### Plot all data
    case_rolling_df = case_rolling_df[
        case_rolling_df["date"] < pd.to_datetime("2021-01-01")
    ].copy()

    # Get election results data
    election_winners_df = getElectionSegmentsData()
    case_rolling_df = case_rolling_df.merge(
        election_winners_df[["state", "COUNTYFP", "changecolor"]],
        how="inner",
        on="COUNTYFP",
    )
    case_rolling_df = (
        case_rolling_df.groupby(["date", "changecolor"])
        .agg(cases_avg_per_100k=("cases_avg_per_100k", "mean"))
        .reset_index()
    )
    case_rolling_df["segmentname"] = case_rolling_df["changecolor"].map(
        color_segment_dict
    )
    return case_rolling_df


########################################################################################
def getCasesRollingAveragePer100K():
    """ 
        THIS FUNCTION reads in the county cases/deaths rolling averages and
        loads it into a dataframe, keeping the columns listed below.
    
        Functions called: None
        Called by: getRollingCaseAverageSegmentLevel()
        
        Input arguments: None
        Returns: Dataframe 'case_rolling_df' with rolling average of cases
                 Columns: date
                          cases_avg_per_100k
                          deaths_avg_per_100k 
                          COUNTYFP (i.e. FIPS number) Questions
        
    """

    ## The below is the rolling average, as it is updated we will get the latest data

    # Questions: Are we going to update the data? Should we?
    # case_rolling_df = pd.read_csv(r"https://raw.githubusercontent.com/nytimes/ \
    #                                    covid-19-data/master/rolling-averages/us-counties.csv")
    case_rolling_df = pd.read_csv(
        DataFolder / r"Dataset 7 Covid/june 26 _rolling_average_us-counties.csv"
    )
    case_rolling_df["date"] = pd.to_datetime(case_rolling_df["date"])
    case_rolling_df.sort_values(by=["state", "county", "date"], inplace=True)

    # Questions: Are we going to use this? Related to data updates?
    # print(f"First date in dataset = {case_rolling_df['date'].min()}\n \
    #                                    Last date in dataset = {case_rolling_df['date'].max()}")

    case_rolling_df = case_rolling_df[
        ["date", "geoid", "county", "cases_avg_per_100k", "deaths_avg_per_100k"]
    ].copy()

    case_rolling_df["COUNTYFP"] = case_rolling_df["geoid"].str.slice(4)
    case_rolling_df["COUNTYFP"] = case_rolling_df["COUNTYFP"].astype(int)
    case_rolling_df.drop(columns=["geoid", "county"], inplace=True)

    return case_rolling_df




########################################################################################
def getPercentilePointChageDeathsData():
    """
    THIS CODE creates a new county-level dataframe merged_df similar to case_rolling_df in the pevious
    section, but with COVID deaths instead of cases. It merges COVID deaths data with presidential election
    data, the latter segmented by change of county political affiliation, if any.

    This is used by the chart preparation code below, and has the following columns:

        COUNTYFP
        deaths_avg_per_100k
        state
        state_po
        CTYNAME
        party_winner_2020
        totalvotes_2020
        fractionalvotes_2020
        party_winner_2016
        totalvotes_2016
        fractionalvotes_2016
        changecolor
        _merge
        pct_increase
        segmentname
    """

    # Find the annual number of cases and deaths per county
    cases_rolling_df = getCasesRollingAveragePer100K()

    # cases_rolling_df = cases_rolling_df[(cases_rolling_df['date']>=pd.to_datetime('2020-01-01'))
    #                                  & (cases_rolling_df['date']<=pd.to_datetime('2020-12-31'))]
    cases_rolling_df = (
        cases_rolling_df.groupby("COUNTYFP")["deaths_avg_per_100k"]
        .mean()
        .fillna(0)
        .reset_index()
    )

    # Select the top 100 in COVID deaths
    cases_rolling_df = cases_rolling_df.sort_values(
        ["deaths_avg_per_100k"], ascending=False
    )
    deaths_top_100_rolling_df = cases_rolling_df[:400].copy()

    # Get county-level presidential election data
    election_df = getElectionSegmentsData()

    # Merge the dataframes
    merged_df = deaths_top_100_rolling_df.merge(
        election_df, how="left", on="COUNTYFP", indicator=True
    )
    merged_df = merged_df[merged_df["_merge"] == "both"].copy()
    merged_df["pct_increase"] = (
        merged_df["fractionalvotes_2020"] - merged_df["fractionalvotes_2016"]
    )
    merged_df["pct_increase"] = merged_df["pct_increase"] * 100

    merged_df["segmentname"] = merged_df["changecolor"].map(color_segment_dict)
    return merged_df


########################################################################################
def getDailyVaccinationPercentData():
    """
        This function retrieves the daily percentage of vaccinated people in each state
        The day_num column is the count of days from the date of first vaccination administered in any state,
        which will be used in the slider providing interactivity. 
        
        Input: None
        Output: 'Date', 
                'Location'
                'Percent with one dose'
                 'state'
                 'state_po',
                 'state_fips'
                 'STATEFP'
                 'STNAME'
                 'Total population'
                 'candidatevotes'
                 'totalvotes'
                 'party_simplified'
                 'fractionalvotes'
                 'day_num'
      
    """
    vaccination_df = pd.read_csv(
        r"../DataForPresidentialElectionsAndCovid/Dataset 7 Covid/COVID-19_Vaccinations_in_the_United_States_Jurisdiction.csv"
    )
    ## Percent of population with at lease one dose based on the jurisdiction where recipient lives
    vaccination_df = vaccination_df[
        ["Date", "Location", "Administered_Dose1_Recip_18PlusPop_Pct"]
    ].copy()
    vaccination_df["Date"] = pd.to_datetime(vaccination_df["Date"])

    state_election_df = getStateLevelElectionData2020()
    vaccination_df = vaccination_df.merge(
        state_election_df, how="inner", left_on="Location", right_on="state_po"
    )
    vaccination_df.drop(
        columns=["candidatevotes", "totalvotes", "party_simplified", "fractionalvotes"],
        inplace=True,
    )

    # Read the persidential election CSV from local disk
    population_df = pd.read_csv(
        r"../DataForPresidentialElectionsAndCovid/Dataset 3 Population Estimate through 2020/County Data Till 2020 co-est2020-alldata.csv",
        encoding="latin-1",
    )
    state_pop_df = population_df[population_df["SUMLEV"] != 50].copy()
    state_pop_df = state_pop_df[["STATE", "STNAME", "POPESTIMATE2020"]]

    vaccination_df = vaccination_df.merge(
        state_pop_df, how="inner", left_on="state_fips", right_on="STATE"
    )
    vaccination_df = vaccination_df.rename(
        columns={
            "STATE": "STATEFP",
            "Administered_Dose1_Recip_18PlusPop_Pct": "Percent with one dose",
            "POPESTIMATE2020": "Total population",
        }
    )

    state_election_df = getStateLevelElectionData2020()
    vaccination_df = vaccination_df.merge(
        state_election_df,
        how="inner",
        left_on=["STATEFP", "state_po", "state", "state_fips"],
        right_on=["state_fips", "state_po", "state", "state_fips"],
    )

    # for charting purposes
    vaccination_df["Percent with one dose"] = (
        vaccination_df["Percent with one dose"] / 100
    )

    # vaccination_df[vaccination_df['Date'].dt.year == 2020]['Percent with one dose'].unique()

    min_date = vaccination_df[vaccination_df["Percent with one dose"] > 0]["Date"].min()
    max_date = vaccination_df[vaccination_df["Percent with one dose"] > 0]["Date"].max()
    vaccination_df["day_num"] = (vaccination_df["Date"] - min_date).dt.days

    vaccination_df = vaccination_df[vaccination_df["Percent with one dose"] > 0].copy()

    return vaccination_df

