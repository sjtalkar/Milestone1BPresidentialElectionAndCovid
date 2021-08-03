import re
import pandas as pd
import numpy as np

from pathlib import Path
from datetime import datetime, date
from .EtlElection import *
from .EtlCovid import *

DataFolder = Path("../DataForPresidentialElectionsAndCovid/")

def getUnemploymentRate(level="county"):
    """
        THIS FUNCTION reads the county level unemployment rate from the 2020 dataset published by the BLS
        and 
        
        Functions called: 
        
        Input: 
            level (str): "county" or "state". Indicate the level at which the data should be aggregated.
        Returns: Dataframe unemployment_covid_df
                
    """
    
    #
    # Prepare unemployment Data
    # 
    unemployment_df = pd.read_excel(DataFolder / r"laucntycur14.xlsx",
                                    names=["LAUS_code","state_FIPS","county_FIPS","county_name_and_state_abbreviation","Period","labor_force","employed","unemployed","unemployment_rate"],
                                    header=5,
                                    skipfooter=3)
    
    # Format the county FIPS as the state FIPS followed by the county FIPS
    concatenate_fips = lambda x : int(str(x["state_FIPS"]) + "{:03d}".format(x["county_FIPS"]))
    unemployment_df["COUNTYFP"] = unemployment_df.apply(concatenate_fips, axis=1)
    # Keep only US mainland states
    unemployment_df = unemployment_df[unemployment_df["COUNTYFP"] < 57000]
    # extract state and county names
    extract_names_regex = r"^(?P<CTYNAME>.*),\s(?P<state>[A-Z]{2})$"
    extract_county_names = lambda x : re.search(extract_names_regex,x).group("CTYNAME") if x != "District of Columbia" else "District of Columbia"
    extract_state_names = lambda x : re.search(extract_names_regex,x).group("state") if x != "District of Columbia" else "District of Columbia"
    unemployment_df["CTYNAME"] = unemployment_df["county_name_and_state_abbreviation"].apply(extract_county_names)
    unemployment_df["state"] = unemployment_df["county_name_and_state_abbreviation"].apply(extract_state_names)
    # Reformat present month which ends with " p"
    reformat_present_month = lambda x: x[:-2] if x[-2:] ==" p" else x 
    unemployment_df["Period"] = unemployment_df["Period"].apply(reformat_present_month)
    # Convert period to datetime
    unemployment_df["month"] = pd.to_datetime(unemployment_df["Period"], format="%b-%y")
    unemployment_df["unemployment_rate"] = unemployment_df["unemployment_rate"].astype("float64")
    unemployment_df.drop(columns=["state_FIPS", "county_FIPS", "LAUS_code","county_name_and_state_abbreviation","Period","labor_force","employed","unemployed"], inplace=True)
    
    #
    # Prepare and merge Covid case and death rates data
    #
    covid_df = getCasesRollingAveragePer100K()
    # Remove non mainland US states
    covid_df = covid_df[covid_df["COUNTYFP"] < 57000]
    # Change period to month and average cases per 100K per month and county
    covid_df["year_month"] = covid_df["date"].dt.to_period('M')
    covid_df.drop(columns=["date"], inplace=True)
    covid_df = covid_df.groupby(["year_month", "COUNTYFP"]).sum()
    covid_df.reset_index(inplace=True)
    # Get back the month period as a timestamp
    covid_df["month"] = covid_df["year_month"].apply(lambda x: x.to_timestamp(freq="D", how="start"))
    covid_df.drop(columns=["year_month"], inplace=True)
    
    unemployment_covid_df = pd.merge(unemployment_df, covid_df, how="left", on=["month", "COUNTYFP"])
    
    #
    # Merge election data at the state or county level
    #
    if level == "state":
        unemployment_covid_df = unemployment_covid_df.groupby(["month", "state"]).agg({
            "unemployment_rate": lambda x : x.mean(),
            "cases_avg_per_100k": lambda x : x.sum(),
            "deaths_avg_per_100k": lambda x : x.sum()
        })
        unemployment_covid_df.reset_index(inplace=True)
        election_df = getStateLevelElectionData2020()
        election_df = election_df[["state_po", "party_simplified"]]
        election_df.rename(columns={"state_po": "state", "party_simplified": "party"}, inplace = True)
        unemployment_covid_df = pd.merge(unemployment_covid_df, election_df, how="left", on="state" )
    else:
        election_df = getElectionData()
        election_df = election_df[["COUNTYFP", "party_winner_2020"]]
        election_df.rename(columns={"party_winner_2020": "party"}, inplace = True)
        unemployment_covid_df = pd.merge(unemployment_covid_df, election_df, how="left", on="COUNTYFP" )
    
    
    # uncemployment_covid_df.join(covid_df, on="COUNTYFP", how="left")
    
    return unemployment_covid_df

def getJuly2020UnemploymentAndMask(level="county", unemployment_covid_df=None):
    if unemployment_covid_df is None:
        unemployment_covid_df = getUnemploymentRate("county")
    county_mask_df = pd.read_csv( DataFolder / r"mask-use-by-county.csv")
    july_2020 = datetime.fromisoformat("2020-07-01")
    
    # Mask Data are from July 2020
    # So keep only the unemployment and covid data until July 2020 and aggregate
    unemployment_covid_july_df = unemployment_covid_df[unemployment_covid_df["month"] <= july_2020]
    unemployment_covid_july_df.drop(columns=["month"])
    unemployment_covid_july_df = unemployment_covid_july_df.groupby(["COUNTYFP"]).agg({
            "unemployment_rate": lambda x : x.mean(),
            "cases_avg_per_100k": lambda x : x.sum(),
            "deaths_avg_per_100k": lambda x : x.sum(),
            "CTYNAME": "first",
            "state": "first",
            "party": "first"
        })
    unemployment_covid_july_df.reset_index(inplace=True)
    
    # Merge the Mask dataset
    county_mask_df = pd.read_csv( DataFolder / r"mask-use-by-county.csv")
    unemployment_covid_july_df = pd.merge(unemployment_covid_july_df, county_mask_df, how="left", on="COUNTYFP")
    
    # If we look at "state" level, aggregate by state
    if level == "state":
        unemployment_covid_july_df = unemployment_covid_july_df.groupby(["state"]).agg({
                "unemployment_rate": lambda x : x.mean(),
                "cases_avg_per_100k": lambda x : x.sum(),
                "deaths_avg_per_100k": lambda x : x.sum(),
                "party": "first",
                "NEVER": lambda x : x.mean(),
                "RARELY": lambda x : x.mean(),
                "SOMETIMES": lambda x : x.mean(),
                "FREQUENTLY": lambda x : x.mean(),
                "ALWAYS": lambda x : x.mean()
            })
        unemployment_covid_july_df.reset_index(inplace=True)
    return unemployment_covid_july_df