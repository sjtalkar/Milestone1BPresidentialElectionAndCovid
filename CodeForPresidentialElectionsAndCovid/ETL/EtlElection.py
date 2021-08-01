import pandas as pd
import numpy as np
from pathlib import Path



# Color global variables
TO_OTHER = "#556B2F"
TO_DEMOCRAT = "#11A3D6"
TO_REPUBLICAN = "#8C1616"
STAYED_DEMOCRAT = "#0015BC"
STAYED_REPUBLICAN = "#FF0000"
STAYED_OTHER = "#B4D3B2"

segment_color_dict = {
    "TO_OTHER": TO_OTHER,
    "TO_DEMOCRAT": TO_DEMOCRAT,
    "TO_REPUBLICAN": TO_REPUBLICAN,
    "STAYED_DEMOCRAT": STAYED_DEMOCRAT,
    "STAYED_REPUBLICAN": STAYED_REPUBLICAN,
    "STAYED_OTHER": STAYED_OTHER,
}

DataFolder = Path("../DataForPresidentialElectionsAndCovid/")


US_STATE_ABBRV = {
    "Alabama": "AL",
    "Alaska": "AK",
    "American Samoa": "AS",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "District of Columbia": "DC",
    "Florida": "FL",
    "Georgia": "GA",
    "Guam": "GU",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Northern Mariana Islands": "MP",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Puerto Rico": "PR",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virgin Islands": "VI",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
}


########################################################################################
def getElectionSegmentsData(segment_color_dict=segment_color_dict):
    """
        THIS FUNCTION obtains the dataframe election_winners_df from the function getElectionData(),
        adds a color column, then uses the color to indicate whether or not the county was won by a
        different party between the 2016 and 2020 elections.
    
        Functions called: getElectionData()
        Called by: getRollingCaseAverageSegmentLevel()

        Input: Dictionary segment_color_dict from above, which defines segment colors.
        Returns: Dataframe election_winners_df from getElectionData() with one extra column:
        
                 changecolor: This column shows Segments = TO_OTHER
                                                           TO_DEMOCRAT
                                                           TO_REPUBLICAN
                                                           STAYED_DEMOCRAT
                                                           STAYED_REPUBLICAN
                                                           STAYED_OTHER
        
    """

    election_winners_df = getElectionData()

    # Set a variable of color that marks NO change and other categories

    # Split the no change further into those that stayed democart and those that stayed republican
    election_winners_df["changecolor"] = pd.Series(
        np.where(
            election_winners_df["party_winner_2020"]
            == election_winners_df["party_winner_2016"],
            # No change stayed the same - find if before and after is republican, \
            # democrat or other
            np.where(
                election_winners_df["party_winner_2020"] == "REPUBLICAN",
                segment_color_dict["STAYED_REPUBLICAN"],
                np.where(
                    election_winners_df["party_winner_2020"] == "DEMOCRAT",
                    segment_color_dict["STAYED_DEMOCRAT"],
                    segment_color_dict["STAYED_OTHER"],
                ),
            ),
            np.where(
                election_winners_df["party_winner_2020"] == "REPUBLICAN",
                segment_color_dict["TO_REPUBLICAN"],
                np.where(
                    election_winners_df["party_winner_2020"] == "DEMOCRAT",
                    segment_color_dict["TO_DEMOCRAT"],
                    segment_color_dict["TO_OTHER"],
                ),
            ),
        )
    )

    return election_winners_df


########################################################################################
def getElectionData():
    """
        THIS FUNCTION reads in county-level presidential election vote data from 2000 to 2020,
        selects the last two elections (2016 and 2020), and returns a dataframe with the result
        totals and fractions for DEMOCRAT and REPUBLICAN, and groups all others under OTHER.
        
        Functions called: None
        Called by: getElectionSegmentsData()
        
        Input: None
        Returns: Dataframe election_winners_df with the following set of columns.
                 Note: Granularity = COUNTYFP.
        
            state                  (full name)
            state_po               (2-letter abbreviation)
            CTYNAME                (full name)
            COUNTYFP               (FIPS number) Questions
            party_winner_2020
            totalvotes_2020
            fractionalvotes_2020
            party_winner_2016
            totalvotes_2016
            fractionalvotes_2016
                
    """

    # Read in presidential election data by county, then select only after 2016 (i.e. 2016 and 2020).
    election_df = pd.read_csv(DataFolder / r"countypres_2000-2020.csv")
    election_df = election_df[election_df["year"] >= 2016].copy()

    election_df.rename(
        columns={"county_fips": "COUNTYFP", "county_name": "CTYNAME"}, inplace=True
    )
    election_df.loc[
        election_df["CTYNAME"] == "DISTRICT OF COLUMBIA", "COUNTYFP"
    ] = 11001.0

    # Questions: These commented lines still needed?
    # election_df.version.unique() #array([20191203, 20210608], dtype=int64)
    # election_df.office.unique() array(['PRESIDENT', 'US PRESIDENT'], dtype=object)
    election_df.drop(columns=["office", "mode", "version", "candidate"], inplace=True)

    # Drop rows that are precincts and do not have a county fup
    election_df.dropna(subset=["COUNTYFP"], inplace=True)

    # Questions: Do we want to collect all other parties under 'OTHER'?
    # Include similar ideologies under them where appropriate? Worth the effort?
    election_df["party"] = np.where(
        (election_df["party"] != "DEMOCRAT") & (election_df["party"] != "REPUBLICAN"),
        "OTHER",
        election_df["party"],
    )
    election_df["COUNTYFP"] = election_df["COUNTYFP"].astype(int)

    election_df = (
        election_df.groupby(
            ["year", "state", "state_po", "CTYNAME", "COUNTYFP", "party", "totalvotes"]
        )
        .agg(candidatevotes=("candidatevotes", sum))
        .reset_index()
    )

    election_df["fractionalvotes"] = (
        election_df["candidatevotes"] / election_df["totalvotes"]
    )

    # get the party that won in each county, total and fractional votes
    election_df["maxfractionalvotes"] = election_df.groupby(
        ["year", "state", "state_po", "CTYNAME", "COUNTYFP", "totalvotes"]
    )["fractionalvotes"].transform(max)

    election_2016_winners_df = election_df[
        (election_df["fractionalvotes"] == election_df["maxfractionalvotes"])
        & (election_df["year"] == 2016)
    ].copy()
    election_2016_winners_df.rename(
        columns={
            "totalvotes": "totalvotes_2016",
            "fractionalvotes": "fractionalvotes_2016",
            "party": "party_winner_2016",
        },
        inplace=True,
    )
    election_2016_winners_df.drop(
        columns=["year", "maxfractionalvotes", "candidatevotes"], inplace=True
    )
    election_2020_winners_df = election_df[
        (election_df["fractionalvotes"] == election_df["maxfractionalvotes"])
        & (election_df["year"] == 2020)
    ].copy()
    election_2020_winners_df.rename(
        columns={
            "totalvotes": "totalvotes_2020",
            "fractionalvotes": "fractionalvotes_2020",
            "party": "party_winner_2020",
        },
        inplace=True,
    )
    election_2020_winners_df.drop(
        columns=["year", "maxfractionalvotes", "candidatevotes"], inplace=True
    )

    # Merge 2016 and 2020 dataframes on state and county
    election_winners_df = election_2020_winners_df.merge(
        election_2016_winners_df,
        how="left",
        on=["COUNTYFP"],
    )
    
    election_winners_df.drop(columns=["state_y", "state_po_y", "CTYNAME_y"], inplace=True)
    election_winners_df.rename(columns={
        "state_x": "state",
        "state_po_x": "state_po",
        "CTYNAME_x": "CTYNAME"},
        inplace=True)

    return election_winners_df


########################################################################################
def getStateLevelElectionData2020():
    """
        THIS FUNCTION gets the winning party of the 2020 presidential election by state.
        
        Functions called: None
        Called by: createStateVaccinationData()
        
        Input: None
        Returns: Dataframe with the following columns:
        
                 year
                 state
                 state_po                (2-letter abbreviation)
                 state_fips
                 candidatevotes
                 totalvotes              (for the whole state)
                 party_simplified        (only DEMOCRAT, REPUBLICAN, LIBERTARIAN or OTHER)
                 fractionalvotes         (candidatevotes / totalvotes)
    """
    # Join with state level election data to color the circles
    state_election_df = pd.read_csv(
        DataFolder
        / r"Dataset 1 Population numbers from Dataverse/1976-2020-president.csv"
    )
    state_election_df = state_election_df[state_election_df["year"] == 2020].copy()
    state_election_df.drop(
        columns=[
            "state_cen",
            "state_ic",
            "office",
            "candidate",
            "writein",
            "version",
            "notes",
            "party_detailed",
        ],
        inplace=True,
    )

    state_election_df["fractionalvotes"] = (
        state_election_df["candidatevotes"] / state_election_df["totalvotes"]
    )

    # get the party that won in each county
    state_election_df["maxfractionalvotes"] = state_election_df.groupby(
        ["year", "state", "state_po", "state_fips", "totalvotes"]
    )["fractionalvotes"].transform(max)
    state_election_df = state_election_df[
        (
            state_election_df["fractionalvotes"]
            == state_election_df["maxfractionalvotes"]
        )
    ].copy()
    state_election_df.drop(columns=["maxfractionalvotes", "year"], inplace=True)
    return state_election_df

