import re
import pandas as pd
import numpy as np
import requests
import json

from pathlib import Path
from datetime import datetime, date
from .EtlElection import *
from .EtlCovid import *

DataFolder = Path("../DataForPresidentialElectionsAndCovid/")

##########################################################################################
# Get the unemployment data from December 2019 per county using the BLS APIs
##########################################################################################
def get_counties_bls_laus_codes():
    unemployment_df = pd.read_excel(DataFolder / r"laucntycur14.xlsx",
                                    names=["LAUS_code","state_FIPS","county_FIPS","county_name_and_state_abbreviation","Period","labor_force","employed","unemployed","unemployment_rate"],
                                    header=5,
                                    skipfooter=3)
    unemployment_df["LAUS_code"] = unemployment_df["LAUS_code"].apply(lambda x: "LAU" + x + "03")
    list_laus_codes = unemployment_df["LAUS_code"].unique()
    pd.Series(list_laus_codes).to_csv(r"../DataForPresidentialElectionsAndCovid/bls_laus_codes.csv", header=None, index=None)
    
def split_codes_in_chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
    
def get_unemployment_rates_from_api(apy_key):
    bls_laus_codes = list(pd.read_csv(DataFolder / r"bls_laus_codes.csv", header=None).iloc[:,0])
    #for laus_code in bls_laus_codes:
    headers = {'Content-type': 'application/json'}
    # The API only accepts 50 series codes per query
    i=1
    unemployment_df = pd.DataFrame(columns=["series_id","state_FIPS","county_FIPS","year","month","unemployment_rate","footnotes"])
    for list_codes in split_codes_in_chunks(bls_laus_codes, 50):
        print(str(i) + " - Querying for 50 series from " + list_codes[0] + " to " + list_codes[-1])
        data = json.dumps({"seriesid": list_codes,"startyear":"2019", "endyear":"2021", "registrationkey": apy_key})
        r = requests.post("https://api.bls.gov/publicAPI/v2/timeseries/data/", data=data, headers=headers)
        if r.status_code == 200:
            print(str(i) + " - Answer received processing the data")
            json_data = json.loads(r.text)
            for series in json_data['Results']['series']:
                seriesId = series['seriesID']
                state_fips = seriesId[5:7]
                county_fips = seriesId[7:10]
                for item in series['data']:
                    year = item['year']
                    month = int(item['period'][1:])
                    unemployment_rate = item['value']
                    footnotes=""
                    for footnote in item['footnotes']:
                        if footnote:
                            footnotes = footnotes + footnote['text'] + ','

                    if 1 <= month <= 12:
                        _row = pd.Series([seriesId,state_fips,county_fips,year,month,unemployment_rate,footnotes[0:-1]], index=unemployment_df.columns)
                        unemployment_df = unemployment_df.append(_row, ignore_index=True)
            print(str(i) + " - Dataframe length = " + str(len(unemployment_df)))
            i+=1
        else:
             print(str(i) + " - ERROR - code = " + str(r.status_code))
    unemployment_df.to_csv(r"../DataForPresidentialElectionsAndCovid/bls_unemployment_rates.csv", header=None, index=None)

    
    
##########################################################################################
# Get the pre-pandemic December 2019 data
##########################################################################################
def getUnemploymentRateSince122019(level="county"):
    #
    # Prepare unemployment Data
    # 
    unemployment_df = pd.read_csv(DataFolder / r"bls_unemployment_rates.csv",
                                    names=["LAUS_code","state_fips","county_fips","year","month","unemployment_rate","footnotes"],
                                    header=0)
    # Convert year and month to datetime
    unemployment_df["month"] = unemployment_df.apply(lambda x: datetime(x["year"], x["month"], 1), axis=1)
    # Keep only the data after December 2019
    unemployment_df = unemployment_df[(unemployment_df["month"]>=pd.to_datetime("2019-12", format="%Y-%m"))]
    # Format the county FIPS as the state FIPS followed by the county FIPS
    concatenate_fips = lambda x : int(str(x["state_fips"]) + "{:03d}".format(x["county_fips"]))
    unemployment_df["COUNTYFP"] = unemployment_df.apply(concatenate_fips, axis=1)
    # Keep only US mainland states
    unemployment_df = unemployment_df[unemployment_df["COUNTYFP"] < 57000]
    # Calculate for each record the number of month since the start
    first_month = unemployment_df["month"].min().to_period("M")
    calculate_month_since_start = lambda x : (x.to_period("M") - first_month).n + 1
    unemployment_df["month_since_start"] = unemployment_df["month"].apply(calculate_month_since_start)
    unemployment_df["unemployment_rate"] = unemployment_df["unemployment_rate"].astype("float64")
    #
    # Merge election data at the state or county level
    #
    if level == "state":
        unemployment_df.drop(columns=["county_fips", "LAUS_code","year","month","footnotes"], inplace=True)
        unemployment_df = unemployment_df.groupby(["month", "state_fips"]).agg({
            "unemployment_rate": lambda x : x.mean()})
        unemployment_df.reset_index(inplace=True)
        election_df = getStateLevelElectionData2020()
        election_df = election_df[["state_po", "party_simplified"]]
        election_df.rename(columns={"state_po": "state", "party_simplified": "party"}, inplace = True)
        unemployment_df = pd.merge(unemployment_df, election_df, how="left", on="state_fips" )
    else:
        unemployment_df.drop(columns=["state_fips", "county_fips", "LAUS_code","year","month","footnotes"], inplace=True)
        election_df = getElectionData()
        election_df = election_df[["COUNTYFP", "party_winner_2020"]]
        election_df.rename(columns={"party_winner_2020": "party"}, inplace = True)
        unemployment_df = pd.merge(unemployment_df, election_df, how="left", on="COUNTYFP" )
    return unemployment_df

##########################################################################################
# Get the unemployment data from December 2019 per county using the BLS APIs
##########################################################################################
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
    unemployment_df = pd.read_csv(DataFolder / r"bls_unemployment_rates.csv",
                                    names=["LAUS_code","state_fips","county_fips","year","month","unemployment_rate","footnotes"],
                                    header=0)
    # Convert year and month to datetime
    unemployment_df["month"] = unemployment_df.apply(lambda x: datetime(x["year"], x["month"], 1), axis=1)
    # Keep only the data from January 2020 (we only have Covid cases from that month)
    unemployment_df = unemployment_df[(unemployment_df["month"]>=pd.to_datetime("2020-01", format="%Y-%m"))]
    # Format the county FIPS as the state FIPS followed by the county FIPS
    concatenate_fips = lambda x : int(str(x["state_fips"]) + "{:03d}".format(x["county_fips"]))
    unemployment_df["COUNTYFP"] = unemployment_df.apply(concatenate_fips, axis=1)
    # Keep only US mainland states
    unemployment_df = unemployment_df[unemployment_df["COUNTYFP"] < 57000]
    # Calculate for each record the number of month since the start
    first_month = unemployment_df["month"].min().to_period("M")
    calculate_month_since_start = lambda x : (x.to_period("M") - first_month).n + 1
    unemployment_df["month_since_start"] = unemployment_df["month"].apply(calculate_month_since_start)
    unemployment_df["unemployment_rate"] = unemployment_df["unemployment_rate"].astype("float64")
    unemployment_df.drop(columns=["state_fips", "county_fips", "LAUS_code","year","footnotes"], inplace=True)
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