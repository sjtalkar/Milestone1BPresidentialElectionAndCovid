import pandas as pd
import numpy as np
import altair as alt
from vega_datasets import data
from pathlib import Path

# uses intermediate json files to speed things up
alt.data_transformers.enable("json")

import pandas as pd
import altair as alt
from vega_datasets import data

import numpy as np
import plotly.graph_objs as go
import plotly.express as px

# Theme settings
import plotly.io as plt_io

alt.data_transformers.disable_max_rows()

## Color global variables
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

color_segment_dict = {
    TO_OTHER: "To other",
    TO_DEMOCRAT: "To Democrat",
    TO_REPUBLICAN: "To Republican",
    STAYED_DEMOCRAT: "Stayed Democrat",
    STAYED_REPUBLICAN: "Stayed Republican",
    STAYED_OTHER: "Stayed Other",
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

    ## Set a variable of color that marks NO change and other categories

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
        election_2016_winners_df, how="left", on=["COUNTYFP"],
    )

    election_winners_df.drop(
        columns=["state_y", "state_po_y", "CTYNAME_y"], inplace=True
    )
    election_winners_df.rename(
        columns={"state_x": "state", "state_po_x": "state_po", "CTYNAME_x": "CTYNAME"},
        inplace=True,
    )

    return election_winners_df


########################################################################################
def createChart(case_rolling_df):
    """
      THIS FUNCTION uses the 'base' encoding chart created by getBaseChart() to create a line chart.
      
      The highlight_segment variable uses the mark_line function to create a line chart out of the encoding. The
      color of the line is set using the conditional color set for the categorical variable using the selection.
      The chart is bound to the selection using add_selection.
      
      It also creates a selector element of a vertical array of circles so the user can select between segment.
      
      Functions called: getSelection(), getBaseChart()
      Called by: Main code
        
      Input: Dataframe with rolling average of cases created by getRollingCaseAverageSegmentLevel()
      Returns: base, make_selector, highlight_segment, radio_select      

    """

    radio_select, change_color_condition = getSelection()

    make_selector = (
        alt.Chart(case_rolling_df)
        .mark_rect()
        .encode(
            y=alt.Y(
                "segmentname:N",
                axis=alt.Axis(title="Pick affiliation", titleFontSize=15),
            ),
            color=change_color_condition,
        )
        .add_selection(radio_select)
    )

    base = getBaseChart(case_rolling_df, ["2020-01-01", "2020-12-31"])

    highlight_segment = (
        base.mark_line(strokeWidth=2)
        .add_selection(radio_select)
        .encode(
            color=change_color_condition,
            strokeDash=alt.condition(
                (alt.datum.segmentname == "To Democrat")
                | (alt.datum.segmentname == "To Republican"),
                alt.value([3, 5]),  # dashed line: 5 pixels  dash + 5 pixels space
                alt.value([0]),  # solid line
            ),
        )
    ).properties(title="Rolling Average Cases Per 100K")

    return base, make_selector, highlight_segment, radio_select


########################################################################################
def getSelection():
    """
      THIS FUNCTION creates a selection element and uses it to 'conditionally' set a color
      for a categorical variable (segment).
      
      It return both the single selection as well as the Category for Color choice set based on selection.
      
      Functions called: None
      Called by: createChart()

      Input: None
      Returns: radio_select, change_color_condition

    """

    radio_select = alt.selection_multi(
        fields=["segmentname", "changecolor"], name="Segment",
    )

    change_color_condition = alt.condition(
        radio_select,
        alt.Color("changecolor:N", scale=None, legend=None),
        alt.value("lightgrey"),
    )

    return radio_select, change_color_condition


########################################################################################
def getBaseChart(case_rolling_df, date_range):
    """
      THIS FUNCTION creates a chart by encoding the date along the X positional axis and rolling mean
      along the Y positional axis. The mark (bar/line..) can be decided upon by the calling function.
      
      Functions called: None
      Called by: createChart()

      Input: Dataframe passed by calling function. The date column is expected to be 'date'
             date_range : a list containing min and max date to be considered for the time series eg["2020-01-01", "2020-12-31"]
      Returns: Base chart
      
    """

    # Set the date range for which the timeseries has to be graphed
    domain = date_range

    source = case_rolling_df[
        (case_rolling_df["date"] >= date_range[0])
        & (case_rolling_df["date"] <= date_range[1])
    ].copy()

    base = (
        alt.Chart(source)
        .encode(
            x=alt.X(
                "date:T",
                timeUnit="yearmonthdate",
                scale=alt.Scale(domain=domain),
                axis=alt.Axis(
                    title=None,
                    # format=("%b %Y"),
                    labelAngle=0,
                    # tickCount=6
                ),
            ),
            y=alt.Y(
                "cases_avg_per_100k:Q",
                axis=alt.Axis(title="Cases (rolling mean per 100K)"),
            ),
        )
        .properties(width=600, height=400)
    )
    return base


########################################################################################
def createTooltip(base, radio_select, case_rolling_df):
    """
      THIS FUNCTION uses the 'base' encoding chart and the selection captured to create four elements
      related to selection.
      
      Functions called: None
      Called by: Main code

      Input: base, radio_select
      Returns: selectors, rules, points, tooltip_text
    """

    # Create a selection that chooses the nearest point & selects based on x-value
    nearest = alt.selection(
        type="single", nearest=True, on="mouseover", fields=["date"], empty="none"
    )

    # Transparent selectors across the chart. This is what tells us
    # the x-value of the cursor
    selectors = (
        alt.Chart(case_rolling_df)
        .mark_point()
        .encode(x="date:T", opacity=alt.value(0),)
        .add_selection(nearest)
    )

    # Draw points on the line, and highlight based on selection
    points = (
        base.mark_point(size=5, dy=-10)
        .encode(opacity=alt.condition(nearest, alt.value(1), alt.value(0)))
        .transform_filter(radio_select)
    )

    # Draw text labels near the points, and highlight based on selection
    tooltip_text = (
        base.mark_text(
            align="left",
            dx=-60,
            dy=-15,
            fontSize=15,
            # fontWeight="bold",
            lineBreak="\n",
        )
        .encode(
            text=alt.condition(
                nearest, alt.Text("cases_avg_per_100k:Q", format=".2f"), alt.value(" "),
            ),
        )
        .transform_filter(radio_select)
    )

    # Draw a rule at the location of the selection
    rules = (
        alt.Chart(case_rolling_df)
        .mark_rule(color="darkgrey", strokeWidth=2, strokeDash=[5, 4])
        .encode(x="date:T",)
        .transform_filter(nearest)
    )
    return selectors, rules, points, tooltip_text


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


def createPercentPointChangeAvgDeathsChart():

    """
      THIS FUNCTION showing average COVID deaths versus percent change for each political affiliation.
      
      Functions called: None
      Called by: Main code

      Input: None
      Returns: Percent point change chart
    """

    merged_df = getPercentilePointChageDeathsData()

    input_dropdown = alt.binding_select(
        options=merged_df["segmentname"].unique().tolist(), name="Affiliation: "
    )
    selection = alt.selection_single(
        fields=["segmentname"], bind=input_dropdown, name="Affiliation: "
    )

    perc_point_deaths_chart = (
        alt.Chart(
            merged_df,
            title="Covid deaths in 2020 versus Percentage point difference in votes",
        )
        .mark_circle()
        .encode(
            x=alt.X("pct_increase:Q", title="Percent point change"),
            y=alt.Y("deaths_avg_per_100k:Q", title="Average deaths per 100K"),
            # color=alt.Color("changecolor:N", scale=None),
            color=alt.condition(
                selection,
                alt.Color("changecolor:N", scale=None, legend=None),
                alt.value("lightgray"),
            ),
            # size= alt.Size("totalvotes_2020:Q", scale=alt.Scale(domain=[100,20000]) , legend=None),
            tooltip=[
                alt.Tooltip("CTYNAME:N", title="County Name:"),
                alt.Tooltip("state:N", title="State Name:"),
                alt.Tooltip(
                    "pct_increase:N", title="Percent Point Increase:", format=".2f"
                ),
            ],
        )
        .properties(height=300, width=800)
        .add_selection(selection)
    )

    mark_more_deaths_line1 = (
        alt.Chart(pd.DataFrame({"x": [0]})).mark_rule(strokeDash=[2, 5]).encode(x="x")
    )
    mark_more_deaths_line2 = (
        alt.Chart(pd.DataFrame({"y": [2]})).mark_rule(strokeDash=[2, 5]).encode(y="y")
    )

    annotations = [
        [8, 2.3, "Counties above this line\nhad the highest COVID-19 death rates"]
    ]
    a_df = pd.DataFrame(annotations, columns=["x", "y", "note"])

    more_deaths_text = (
        alt.Chart(a_df)
        .mark_text(align="left", baseline="middle", fontSize=10, dx=7)
        .encode(x="x:Q", y="y:Q", text=alt.Text("note:N"))
    )

    return (
        perc_point_deaths_chart
        + mark_more_deaths_line1
        + mark_more_deaths_line2
        + more_deaths_text
    )


########################################################################################
def createStateVaccinationData():

    """
      THIS FUNCTION obtains vaccination data by state (Questions: by June 26, 2021), merges into it
      state population data, followed by 2020 presidential election data.
      
      Functions called: getStateLevelElectionData2020()
      Called by: createStateVaccinationChart()

      Input: None
      Returns: Dataframe vaccination_df with the following columns:
      
               STATEFP  ...........................................  (state FIPS)
               STNAME
               People with at least One Dose by State of Residence
               Percent with one dose
               Total population
               state  .............................................  (state name again)
               state_po  ..........................................  (2-letter abbreviation)
               state_fips  ........................................  (state FIPS again)
               candidatevotes
               totalvotes
               party_simplified  ..................................  (DEMOCRAT, REPUBLICAN, LIBERTARIAN or OTHER)
               fractionalvotes
    """
    vaccination_df = pd.read_csv(
        DataFolder / r"Dataset 7 Covid/covid19_vaccinations_in_the_united_states.csv",
        skiprows=2,
    )

    # Select columns containing at least one dose per 100K since taking that one dose shows openness
    # to taking the vaccine
    vaccination_df = vaccination_df[
        [
            "State/Territory/Federal Entity",
            "People with at least One Dose by State of Residence",
            "Percent of Total Pop with at least One Dose by State of Residence",
        ]
    ].copy()

    # Calculate Total population assumed by data as per percent and numbers
    vaccination_df["Total population"] = (
        (vaccination_df["People with at least One Dose by State of Residence"] * 100)
        / vaccination_df[
            "Percent of Total Pop with at least One Dose by State of Residence"
        ]
    )

    # Read the county population CSV from local file
    population_df = pd.read_csv(
        DataFolder
        / r"Dataset 3 Population Estimate through 2020/County Data Till 2020 co-est2020-alldata.csv",
        encoding="latin-1",
    )
    state_pop_df = population_df[population_df["SUMLEV"] != 50].copy()
    state_pop_df = state_pop_df[["STATE", "STNAME", "POPESTIMATE2020"]]

    # Merge vaccination and population data on state name
    vaccination_df = vaccination_df.merge(
        state_pop_df,
        how="inner",
        left_on="State/Territory/Federal Entity",
        right_on="STNAME",
    )
    vaccination_df = vaccination_df[
        [
            "STATE",
            "STNAME",
            "People with at least One Dose by State of Residence",
            "Percent of Total Pop with at least One Dose by State of Residence",
            "Total population",
        ]
    ].copy()
    vaccination_df = vaccination_df.rename(
        columns={
            "STATE": "STATEFP",
            "Percent of Total Pop with at least One Dose by State of Residence": "Percent with one dose",
        }
    )

    # Get the presidential election winning party data
    state_election_df = getStateLevelElectionData2020()

    # Merge with vaccinatino and population data
    vaccination_df = vaccination_df.merge(
        state_election_df, how="inner", left_on="STATEFP", right_on="state_fips"
    )

    # for charting purposes
    vaccination_df["Percent with one dose"] = (
        vaccination_df["Percent with one dose"] / 100
    )

    return vaccination_df


########################################################################################
def createStateVaccinationChart():

    """
      THIS FUNCTION creates a chart relating vaccination data to 2020 presidential election winning party.
      
      Functions called: createStateVaccinationData()
      Called by: Main code

      Input: None
      Returns: Chart final_chart
    """

    source = createStateVaccinationData()
    max_value = source["Total population"].max()
    min_value = source["Total population"].min()
    source["y_center"] = (
        (source["Total population"] - min_value) / (max_value - min_value)
    ) + 0.5

    big_chart = (
        alt.Chart(
            source,
            title=[
                "Percentage of state’s population age 18 and older that has received",
                "at least one dose of a COVID-19 vaccine as of June 26th",
            ],
        )
        .mark_point(filled=True, opacity=1,)
        .encode(
            x=alt.X(
                "Percent with one dose:Q",
                axis=alt.Axis(
                    title=None,
                    format="%",
                    orient="top",
                    values=[0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
                ),
                scale=alt.Scale(domain=[0.30, 0.80]),
            ),
            y=alt.Y("y_center:Q", axis=None),
            color=alt.Color(
                "party_simplified:N",
                scale=alt.Scale(
                    domain=["DEMOCRAT", "REPUBLICAN"], range=["#237ABD", "#CD2128"]
                ),
            ),
            tooltip=[alt.Tooltip("state_po:N", title="State: ")],
            size=alt.Size(
                "Total population:Q", scale=alt.Scale(range=[100, 3000]), legend=None
            ),
        )
        .properties(width=700, height=400)
    )

    big_chart_line = (
        alt.Chart(pd.DataFrame({"x": [0.5]}))
        .mark_rule(strokeDash=[10, 10])
        .encode(x="x")
    )

    big_chart_text = (
        alt.Chart(source)
        .mark_text(
            align="left",
            baseline="middle",
            dx=-3,
            fontSize=8,
            fontWeight="bold",
            color="white",
        )
        .encode(
            x=alt.X("Percent with one dose:Q"), y=alt.Y("y_center:Q"), text="state_po"
        )
    )

    small_chart = (
        alt.Chart(source, title="Percentage of people vaccinated wih one dose")
        .mark_point(filled=True, opacity=1)
        .encode(
            x=alt.X(
                "Percent with one dose:Q",
                axis=alt.Axis(
                    format="%", orient="top", values=[0, 0.2, 0.4, 0.6, 0.8, 1]
                ),
                scale=alt.Scale(domain=[0, 1]),
                title=None,
            ),
            y=alt.Y("y_center:Q", axis=None),
            color=alt.Color(
                "party_simplified:N",
                legend=alt.Legend(title="Presidential election choice"),
                scale=alt.Scale(
                    domain=["DEMOCRAT", "REPUBLICAN"], range=["#237ABD", "#CD2128"]
                ),
            ),
            size=alt.Size(
                "Total population:Q", scale=alt.Scale(range=[50, 100]), legend=None
            ),
        )
        .properties(width=400, height=50)
    )

    # Add a rectangle around the data
    box = pd.DataFrame({"x1": [0.3], "x2": [0.8], "y1": [0], "y2": [1.5]})

    rect = (
        alt.Chart(box)
        .mark_rect(fill="white", stroke="black", opacity=0.3)
        .encode(alt.X("x1",), alt.Y("y1",), x2="x2", y2="y2")
    )

    full_x_chart = small_chart + rect

    final_chart = (
        ((big_chart + big_chart_text + big_chart_line) & full_x_chart)
        .resolve_scale(x="independent", y="independent", size="independent")
        .configure_title(fontSize=15)
        .configure_axis(labelColor="#a9a9a9")
    )

    return final_chart


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


########################################################################################
alt.data_transformers.disable_max_rows()


## Color global variables
TO_OTHER = "#556B2F"
TO_DEMOCRAT = "#11A3D6"
TO_REPUBLICAN = "#8C1616"
STAYED_DEMOCRAT = "#0015BC"
STAYED_REPUBLICAN = "#FF0000"
STAYED_OTHER = "#B4D3B2"

# Get the election winners data
election_winners_df = getElectionSegmentsData()

counties = alt.topo_feature(data.us_10m.url, "counties")
us_states = alt.topo_feature(data.us_10m.url, "states")


# Read the persidential election CSV from local disk
population_df = pd.read_csv(
    r"../DataForPresidentialElectionsAndCovid/Dataset 3 Population Estimate through 2020/County Data Till 2020 co-est2020-alldata.csv",
    encoding="latin-1",
)
state_pop_df = population_df[population_df["SUMLEV"] != 50].copy()
state_pop_df = state_pop_df[
    ["STATE", "STNAME", "POPESTIMATE2016", "POPESTIMATE2020", "RNETMIG2020"]
]


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

##########################################################################################
def createFrequentAndInfrequentMaskUsers():
    # Add up groupings of frequent and non frequent
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


###########################################################################################
def createWhiteTheme():
    # back_colors = {"superdark-blue": "rgb(31,38,42)"}
    back_colors = {"white": "rgb(255,255, 255)"}

    # create our custom_dark theme from the plotly_dark template
    plt_io.templates["custom_dark"] = plt_io.templates["presentation"]

    return


createWhiteTheme()


def createSankeyForAffilitionChange():
    """ 
        This function creates a Sankey chart that shows the County affiliation change from 
        2016 to 2019
        The interactive tooltip will offer the actual numbers
    """
    sankey_df = (
        election_winners_df.groupby(["party_winner_2016", "party_winner_2020"])
        .agg(countiesingroup=("totalvotes_2016", "count"))
        .reset_index()
    )

    sankey_df["party_winner_2016"] = sankey_df["party_winner_2016"] + "_2016"
    sankey_df["party_winner_2020"] = sankey_df["party_winner_2020"] + "_2020"

    label_list = (
        sankey_df["party_winner_2016"].unique().tolist()
        + sankey_df["party_winner_2020"].unique().tolist()
    )
    label_idx_dict = {}
    for idx, label in enumerate(label_list):
        label_idx_dict[label] = idx

    label_list = [
        f"Voted {label.split( '_')[0].capitalize()} in {label.split( '_')[1]}"
        for label in label_list
    ]

    sankey_df["2016_idx"] = sankey_df["party_winner_2016"].map(label_idx_dict)
    sankey_df["2020_idx"] = sankey_df["party_winner_2020"].map(label_idx_dict)

    source = sankey_df["2016_idx"].tolist()
    target = sankey_df["2020_idx"].tolist()
    values = sankey_df["countiesingroup"].tolist()
    color_link = ["#BEDDFE", "#FAED27", "#FF0000", "#0000FF", "#FFE9EC"]
    color_node = ["#80CEE1", "#FF6961", "#0000FF", "#96D2B7", "#FF0000"]

    fig = go.Figure(
        data=[
            go.Sankey(
                node=dict(
                    pad=15,
                    thickness=20,
                    line=dict(color="black", width=0.5),
                    label=label_list,
                    color=color_node,
                    customdata=label_list,
                    hovertemplate="%{customdata} has  %{value} counties<extra></extra>",
                ),
                link=dict(
                    source=source,
                    target=target,
                    value=values,
                    color=color_link,
                    hovertemplate="Link from  %{source.customdata}<br />"
                    + "to %{target.customdata}<br />has  %{value} counties<extra></extra>",
                ),
            )
        ]
    )

    fig.update_layout(
        title_text="County affiliation change from 2016 to 2020", font_size=12,
    )

    # Set the theme
    fig.layout.template = "custom_dark"
    return fig


###########################################################################################
from vega_datasets import data


def plotCountyMaskUsage(df, mask_usage_type, color_scheme):

    source = df[df["mask_usage_type"] == mask_usage_type]

    chart = (
        alt.Chart(counties)
        .mark_geoshape()
        .encode(
            color=alt.Color("mask_usage:Q", scale=alt.Scale(scheme=color_scheme)),
            tooltip=[
                alt.Tooltip("CTYNAME:N", title="County name: "),
                alt.Tooltip("mask_usage:Q", title="Never use mask: "),
            ],
        )
        .transform_lookup(
            lookup="id",
            from_=alt.LookupData(
                source,
                "COUNTYFP",
                ["mask_usage", "mask_usage_types", "COUNTYFP", "CTYNAME"],
            ),
        )
        .project(type="albersUsa")
        .properties(width=400, height=300)
    )
    return chart


def plotAllMaskUsageTypesForCounties():
    county_pop_mask_melt_df = county_pop_mask_df.melt(
        id_vars=[
            "STATE",
            "COUNTYFP",
            "CTYNAME",
            "POPESTIMATE2016",
            "POPESTIMATE2020",
            "RNETMIG2020",
        ],
        value_vars=["NEVER", "RARELY", "SOMETIMES", "FREQUENTLY", "ALWAYS",],
        var_name="mask_usage_type",
        value_name="mask_usage",
        col_level=None,
        ignore_index=True,
    )

    colors = ["purplered", "redpurple", "yelloworangebrown", "purpleblue", "darkblue"]
    usage_types = county_pop_mask_melt_df.mask_usage_type.unique()
    chart_list = []
    for color_scheme, usage_type in tuple(zip(colors, usage_types)):
        chart = plotCountyMaskUsage(
            county_pop_mask_melt_df, usage_type, color_scheme
        ).properties(title=f"Mask usage type: {usage_type}")
        chart_list.append(chart)

    return chart_list


def createCombinedElectoralAndMaskUsageCharts():
    source = election_winners_df
    source["COUNTYANDFP"] = (
        election_winners_df["CTYNAME"].str.capitalize()
        + " ("
        + election_winners_df["COUNTYFP"].astype(str)
        + ")"
    )
    source["segmentname"] = election_winners_df["changecolor"].map(color_segment_dict)

    # Create a selection based on COUNTYFP since several states can have counties with same name
    click = alt.selection_multi(fields=["COUNTYANDFP"])

    # input_dropdown = alt.binding_select(options=source['segmentname'].unique().tolist(), name='Affiliation: ')
    input_dropdown = alt.binding_select(
        options=["To Democrat", "To Republican"], name="Affiliation: "
    )
    segment_selection = alt.selection_single(
        fields=["segmentname"], bind=input_dropdown, name="Affiliation: "
    )

    county_winners_chart = (
        alt.Chart(
            counties, title="Counties that changed affiliations in 2020 elections"
        )
        .mark_geoshape()
        .encode(
            color=alt.Color("changecolor:N", scale=None),
            tooltip=[
                alt.Tooltip("state:N", title="State: "),
                alt.Tooltip("CTYNAME:N", title="County name: "),
                alt.Tooltip(
                    "party_winner_2016:N", title="2016 Presidential election winner: "
                ),
                alt.Tooltip(
                    "party_winner_2020:N", title="2020 Presidential election winner: "
                ),
            ],
            # opacity=alt.condition(click, alt.value(0.8), alt.value(0.2)),
            opacity=alt.condition(segment_selection, alt.value(0.8), alt.value(0.2)),
        )
        .transform_lookup(
            lookup="id",
            from_=alt.LookupData(
                source,
                "COUNTYFP",
                [
                    "party_winner_2016",
                    "party_winner_2020",
                    "COUNTYFP",
                    "CTYNAME",
                    "COUNTYANDFP",
                    "changecolor",
                    "state",
                    "state_po",
                    "segmentname",
                ],
            ),
        )
        .add_selection(
            segment_selection  ## Make sure you have added the selection here
        )
        .add_selection(click)  ## Make sure you have added the selection here
        .project(type="albersUsa")
        .properties(width=950, height=500)
    )

    outline = (
        alt.Chart(us_states)
        .mark_geoshape(stroke="grey", fillOpacity=0)
        .project(type="albersUsa")
        .properties(width=950, height=500)
    )
    #############################################
    county_pop_mask_melt_df = createFrequentAndInfrequentMaskUsers()
    county_pop_mask_melt_df["COUNTYANDFP"] = (
        county_pop_mask_melt_df["CTYNAME"].str.replace(" County", "")
        + " ("
        + county_pop_mask_melt_df["COUNTYFP"].astype(str)
        + ")"
    )
    county_pop_mask_melt_df["segmentname"] = county_pop_mask_melt_df["changecolor"].map(
        color_segment_dict
    )

    county_pop_mask_melt_df.sort_values(
        by=["mask_usage_type", "mask_usage"], inplace=True
    )

    county_pop_mask_republican = county_pop_mask_melt_df[
        county_pop_mask_melt_df["changecolor"].isin([TO_REPUBLICAN])
    ].copy()
    county_pop_mask_democrat = county_pop_mask_melt_df[
        county_pop_mask_melt_df["changecolor"].isin([TO_DEMOCRAT])
    ].copy()
    county_pop_mask_republican.sort_values(
        by=["mask_usage_type", "mask_usage"], inplace=True
    )
    county_pop_mask_democrat.sort_values(
        by=["mask_usage_type", "mask_usage"], inplace=True
    )
    county_order_republican = county_pop_mask_republican[
        county_pop_mask_republican["mask_usage_type"] == "FREQUENT"
    ]["COUNTYFP"].unique()
    county_order_democrat = county_pop_mask_democrat[
        county_pop_mask_democrat["mask_usage_type"] == "FREQUENT"
    ]["COUNTYFP"].unique()

    county_pop_mask_stayed_republican = county_pop_mask_melt_df[
        county_pop_mask_melt_df["changecolor"].isin([STAYED_REPUBLICAN])
    ].copy()
    county_pop_mask_stayed_democrat = county_pop_mask_melt_df[
        county_pop_mask_melt_df["changecolor"].isin([STAYED_DEMOCRAT])
    ].copy()
    county_pop_mask_stayed_republican.sort_values(
        by=["mask_usage_type", "mask_usage"], inplace=True
    )
    county_pop_mask_stayed_democrat.sort_values(
        by=["mask_usage_type", "mask_usage"], inplace=True
    )
    county_order_stayed_republican = county_pop_mask_stayed_republican[
        county_pop_mask_stayed_republican["mask_usage_type"] == "FREQUENT"
    ]["COUNTYFP"].unique()
    county_order_stayed_democrat = county_pop_mask_stayed_democrat[
        county_pop_mask_stayed_democrat["mask_usage_type"] == "FREQUENT"
    ]["COUNTYFP"].unique()

    a = (
        alt.Chart(county_pop_mask_republican)
        .mark_bar()
        .encode(
            x=alt.X("COUNTYANDFP:N", title="", sort=county_order_republican),
            y=alt.Y("mask_usage:Q", title=""),
            color=alt.Color(
                "mask_usage_type:N",
                scale=alt.Scale(
                    domain=["FREQUENT", "NOT FREQUENT"],
                    range=["#ddccbb", STAYED_REPUBLICAN],
                ),
                legend=alt.Legend(title="Mask usage type"),
            ),
            opacity=alt.condition(segment_selection, alt.value(0.8), alt.value(0.2)),
        )
        .add_selection(
            segment_selection  ## Make sure you have added the selection here
        )
        .add_selection(click)  ## Make sure you have added the selection here
        .properties(height=100, width=200)
    )

    a1 = (
        alt.Chart(county_pop_mask_stayed_republican)
        .mark_bar()
        .encode(
            x=alt.X("COUNTYANDFP:N", title="", sort=county_order_stayed_republican),
            y=alt.Y("mask_usage:Q", title=""),
            color=alt.Color(
                "mask_usage_type:N",
                scale=alt.Scale(
                    domain=["FREQUENT", "NOT FREQUENT"],
                    range=["#ddccbb", STAYED_REPUBLICAN],
                ),
                legend=alt.Legend(title="Mask usage type"),
            ),
            opacity=alt.condition(segment_selection, alt.value(0.8), alt.value(0.2)),
        )
        .add_selection(
            segment_selection  ## Make sure you have added the selection here
        )
        .properties(height=100, width=200)
    )

    b = (
        alt.Chart(county_pop_mask_democrat)
        .mark_bar()
        .encode(
            alt.X("COUNTYANDFP:N", title="", sort=county_order_democrat),
            alt.Y("mask_usage:Q", title=""),
            alt.Color(
                "mask_usage_type:N",
                scale=alt.Scale(
                    domain=["FREQUENT", "NOT FREQUENT"],
                    range=["#ddccbb", STAYED_DEMOCRAT],
                ),
                legend=alt.Legend(title="Mask usage type"),
            ),
            opacity=alt.condition(segment_selection, alt.value(0.8), alt.value(0.2)),
        )
        .add_selection(
            segment_selection  ## Make sure you have added the selection here
        )
        .properties(height=100, width=700)
    )

    return (
        (county_winners_chart + outline)
        & alt.vconcat(a, b).resolve_scale(color="independent")
    ).configure_concat(spacing=10)


#######################################################################################
def createDailyInteractiveVaccinationChart():
    """
        THIS FUNCTION creates an interactive chart. A slider is provided that starts from the first day any resident 
        received vaccination and can be moved up until June 28, 2021.
        The chart shows the percentage of 18 years and above population of a state that as received at least 
        one vaccine shot. Since some of the vaccines such as J&J require only one shot, this percentage shows the receptivity 
        of the population to a vaccine shot.
            
        The size of the state bubble is proportional to the population of the state.
    
    """

    full_source = getDailyVaccinationPercentData()

    max_value = full_source["Total population"].max()
    min_value = full_source["Total population"].min()
    full_source["y_center"] = (
        (full_source["Total population"] - min_value) / (max_value - min_value)
    ) + 0.5

    # Create Slider
    min_day_num = full_source.day_num.min()
    max_day_num = full_source.day_num.max()
    slider = alt.binding_range(
        min=min_day_num,
        max=max_day_num,
        step=1,
        name="Number of days since first vaccination: ",
    )
    slider_selection = alt.selection_single(
        fields=["day_num"], bind=slider, name="day_num", init={"day_num": max_day_num}
    )

    big_chart = (
        alt.Chart(
            full_source,
            title=[
                "Percentage of state’s population age 18 and older that has received",
                "at least one dose of a COVID-19 vaccine as of June 26th, 2021",
            ],
        )
        .mark_point(filled=True, opacity=1,)
        .transform_filter(slider_selection)
        .encode(
            x=alt.X(
                "Percent with one dose:Q",
                axis=alt.Axis(
                    title=None,
                    format="%",
                    orient="top",
                    values=[0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
                ),
                scale=alt.Scale(domain=[0, 1.0]),
            ),
            y=alt.Y("y_center:Q", axis=None),
            color=alt.Color(
                "party_simplified:N",
                legend=alt.Legend(title="Presidential election choice:"),
                scale=alt.Scale(
                    domain=["DEMOCRAT", "REPUBLICAN"], range=["#237ABD", "#CD2128"]
                ),
            ),
            tooltip=[alt.Tooltip("state_po:N", title="State: ")],
            size=alt.Size(
                "Total population:Q", scale=alt.Scale(range=[150, 3000]), legend=None
            ),
        )
        .add_selection(slider_selection)
        .properties(width=700, height=400)
    )

    big_chart_line = (
        alt.Chart(pd.DataFrame({"x": [0.5]}))
        .mark_rule(strokeDash=[10, 10])
        .encode(x="x")
    )

    big_chart_text = (
        alt.Chart(full_source)
        .mark_text(
            align="left",
            baseline="middle",
            dx=-3,
            fontSize=8,
            fontWeight="bold",
            color="white",
        )
        .transform_filter(slider_selection)
        .encode(
            x=alt.X("Percent with one dose:Q"), y=alt.Y("y_center:Q"), text="state_po"
        )
    )

    small_chart = (
        alt.Chart(full_source, title="Percentage of people vaccinated wih one dose")
        .mark_point(filled=True, opacity=1)
        .transform_filter(slider_selection)
        .encode(
            x=alt.X(
                "Percent with one dose:Q",
                axis=alt.Axis(
                    format="%", orient="top", values=[0, 0.2, 0.4, 0.6, 0.8, 1]
                ),
                scale=alt.Scale(domain=[0, 1]),
                title=None,
            ),
            y=alt.Y("y_center:Q", axis=None),
            color=alt.Color(
                "party_simplified:N",
                legend=alt.Legend(title="Presidential election choice:"),
                scale=alt.Scale(
                    domain=["DEMOCRAT", "REPUBLICAN"], range=["#237ABD", "#CD2128"]
                ),
            ),
            size=alt.Size(
                "Total population:Q", scale=alt.Scale(range=[50, 100]), legend=None
            ),
        )
        .add_selection(slider_selection)
        .properties(width=400, height=50)
    )

    # Add a rectangle around the data
    box = pd.DataFrame({"x1": [0.3], "x2": [0.8], "y1": [0], "y2": [1.5]})

    rect = (
        alt.Chart(box)
        .mark_rect(fill="white", stroke="black", opacity=0.3)
        .encode(alt.X("x1",), alt.Y("y1",), x2="x2", y2="y2")
    )

    full_x_chart = small_chart + rect

    final_chart = (
        ((big_chart + big_chart_text + big_chart_line))
        .configure_title(fontSize=15)
        .configure_axis(labelColor="#a9a9a9")
    )

    return final_chart


#######################################################################################
##############THIS SECTION CLEANS AND SETS UP CHARTS FOR DELTA VARIANT VISUALS
#######################################################################################

from ETLForElectionAndVaccinationData import *


import pandas as pd
import os
import pickle
from vega_datasets import data
import altair as alt


def getStateVaccinationDataWithAPI():

    """ 
        THIS FUNCTION uses API calls to get the latest vaccination data at the state level
        It also gets the latest covid cases and deaths at the state levels from the NYT site that is regularly updated
        Functions called: None
        Called by: charting procedure
        
        Input arguments: None
        Returns: Three Dataframe 
                    state_vaccine_df, 
                    us_case_rolling_df with rolling average of cases at country = US  level 
                    state_case_rolling_df with rolling average of cases at state level
                 Columns: 
        
    """

    ##########################################################################################################
    # # ### Read the pickle file with stored token
    # pickle_in = open("APIToken.pickle","rb")
    # APITokenIn = pickle.load(pickle_in)
    # curr_offset = 0
    # num_file = 1
    # url = f"https://data.cdc.gov/resource/unsk-b7fc.csv?$$app_token={APITokenIn}&$limit=500000&$offset={curr_offset}&$order=date"
    # response = requests.request("GET", url)
    # df = csv_to_dataframe(response)
    # #df.to_csv(r'..\DataForPresidentialElectionsAndCovid\Dataset 9 State Vaccine Data Using API\StateVaccineDataFile1.csv')
    # curr_offset = curr_offset + 500000
    # num_file = num_file + 1
    # state_vaccine_df = df[['date','location','mmwr_week', 'administered_dose1_recip',  'administered_dose1_recip', 'administered_dose1_pop_pct', ]].copy()

    ############### When developing comment above and uncomment out read from file below ###########################
    folder_name = os.listdir(
        DataFolder
        / r"../DataForPresidentialElectionsAndCovid/Dataset 9 State Vaccine Data Using API/"
    )
    path_name = r"../DataForPresidentialElectionsAndCovid/Dataset 9 State Vaccine Data Using API/"

    state_vaccine_df = pd.DataFrame()
    for name in folder_name:
        if name.startswith("StateVaccineDataFile"):
            df = pd.read_csv(path_name + name)
            df = df[
                [
                    "date",
                    "location",
                    "mmwr_week",
                    "administered_dose1_recip",
                    "administered_dose1_pop_pct",
                ]
            ].copy()
            if len(df) > 0:
                state_vaccine_df = state_vaccine_df.append(df, ignore_index=True)
                # print(name)

    #########################################################################################################

    # Read the county population CSV from local file
    population_df = pd.read_csv(
        DataFolder
        / r"Dataset 3 Population Estimate through 2020/County Data Till 2020 co-est2020-alldata.csv",
        encoding="latin-1",
    )
    state_pop_df = population_df[population_df["SUMLEV"] != 50].copy()
    state_pop_df = state_pop_df[["STATE", "STNAME", "POPESTIMATE2020"]]
    state_pop_df["st_abbr"] = state_pop_df["STNAME"].map(US_STATE_ABBRV)

    # Merge vaccination and population data on state name
    state_vaccine_df = state_vaccine_df.merge(
        state_pop_df, how="inner", left_on="location", right_on="st_abbr"
    )
    state_vaccine_df = state_vaccine_df.rename(
        columns={
            "STATE": "STATEFP",
            "administered_dose1_pop_pct": "Percent with one dose",
        }
    )

    state_vaccine_df.drop(columns=["st_abbr"], inplace=True)

    state_vaccine_df["date"] = pd.to_datetime(state_vaccine_df["date"])
    min_vaccine_date = state_vaccine_df[state_vaccine_df["Percent with one dose"] > 0][
        "date"
    ].min()
    max_date = state_vaccine_df[state_vaccine_df["Percent with one dose"] > 0][
        "date"
    ].max()
    state_vaccine_df = state_vaccine_df[
        state_vaccine_df["date"] >= min_vaccine_date
    ].copy()
    state_vaccine_df["day_num"] = (state_vaccine_df["date"] - min_vaccine_date).dt.days

    state_vaccine_df["vacc_rank"] = state_vaccine_df.groupby("date")[
        "Percent with one dose"
    ].rank("dense", ascending=True)
    state_vaccine_df["vacc_rank"] = state_vaccine_df["vacc_rank"].astype(int)
    state_vaccine_df["vacc_rank"] = np.where(
        state_vaccine_df["vacc_rank"] > 5, "", state_vaccine_df["vacc_rank"]
    )
    # state_vaccine_df['vacc_rank'] = state_vaccine_df['state'] + " "  + state_vaccine_df['vacc_rank'].astype(str)

    us_case_rolling_df = pd.read_csv(
        "https://raw.githubusercontent.com/nytimes/covid-19-data/master/rolling-averages/us.csv"
    )
    us_case_rolling_df["date"] = pd.to_datetime(us_case_rolling_df["date"])

    state_case_rolling_df = pd.read_csv(
        "https://raw.githubusercontent.com/nytimes/covid-19-data/master/rolling-averages/us-states.csv"
    )
    # state_case_rolling_df.to_csv(DataFolder / r"Dataset 7 Covid/July_21_rolling_average_us-states.csv")
    state_case_rolling_df["date"] = pd.to_datetime(state_case_rolling_df["date"])
    state_case_rolling_df.sort_values(by=["state", "date"], inplace=True)

    state_case_rolling_df = state_case_rolling_df[
        ["date", "geoid", "state", "cases_avg_per_100k", "deaths_avg_per_100k"]
    ].copy()

    us_case_rolling_df = us_case_rolling_df[
        ["date", "geoid", "cases_avg_per_100k", "deaths_avg_per_100k"]
    ].copy()

    state_case_rolling_df["STATEFP"] = state_case_rolling_df["geoid"].str.slice(4)
    state_case_rolling_df["STATEFP"] = state_case_rolling_df["STATEFP"].astype(int)
    state_case_rolling_df.drop(columns=["geoid"], inplace=True)

    state_case_rolling_df = state_case_rolling_df[
        (state_case_rolling_df.date >= min_vaccine_date)
        & (state_case_rolling_df.date <= max_date)
    ]
    us_case_rolling_df = us_case_rolling_df[
        (us_case_rolling_df.date >= min_vaccine_date)
        & (us_case_rolling_df.date <= max_date)
    ]

    return state_vaccine_df, us_case_rolling_df, state_case_rolling_df


# https://stackoverflow.com/questions/59224026/how-to-add-a-slider-to-a-choropleth-in-altair


#########################################################################################
def plotStateVaccinePct(df, date_in):

    """
        This function generates a US state choropleth map with color scale tuned 
        by the number of vaccinations at the latest time. 

        Input: Dataframe with State fips STATEFP, Percent with one dose and STNAME
        Output: The choropleth and the click select for each state
    """
    from vega_datasets import data

    source = df[df["date"] == date_in].copy()
    min_day_num = source.day_num.min()
    max_day_num = source.day_num.max()

    us_states = alt.topo_feature(data.us_10m.url, "states")

    # Create Slider
    # slider = alt.binding_range(min=min_day_num, max=max_day_num, step=1, name = "Number of days since first vaccination: ")
    # slider_selection = alt.selection_single(fields=['day_num'], bind=slider, name="day_num", init={'day_num':max_day_num})

    click = alt.selection_multi(fields=["STATEFP"], init=[{"STATEFP": 1}])

    chart = (
        alt.Chart(
            us_states,
            title={
                "text": [
                    "Vaccination: Population percent with atleast one vaccine shot"
                ],
                "subtitle": [
                    "Hover for tooltip with state and vaccination percent value",
                    "Shift+Click for multiple state selections",
                ],
                "color": "black",
                "subtitleColor": "lightgrey",
                "fontSize": 14,
                "fontWeight": "bold",
            },
        )
        .mark_geoshape(stroke="lightgrey")
        .encode(
            color=alt.condition(
                click,
                alt.value("lightgray"),
                alt.Color(
                    "Percent with one dose:Q",
                    scale=alt.Scale(scheme="yelloworangebrown"),
                ),
            ),
            tooltip=[
                alt.Tooltip("STNAME:N", title="State name: "),
                alt.Tooltip(
                    "Percent with one dose:Q", title="Population Pct with one shot : "
                ),
            ],
        )
        .transform_lookup(
            lookup="id",
            from_=alt.LookupData(
                source, "STATEFP", ["Percent with one dose", "STATEFP", "STNAME"]
            ),
        )
        .add_selection(click)  ## Make sure you have added the selection here
        .project(type="albersUsa")
        .properties(width=850, height=500)
    )

    return chart, click


#########################################################################################


def createCombinedVaccinationAndDeltaVariantTrend():
    """
                This functions creates a Delta variant timeseries. 
                A dropdown selector is created to select  a state but the timeseries can also display the 
                state selected in a choropleth map displayed above it since the click selector of the choropleth map 
                is added to the timeseries chart.

                Tootltips are created to display the number of cases all through the timeseries       

                Input: None
                Output: 
                vaccine_chart - The choropleth of US geography colored by vaccination population pct.
                us_timeseries - Timeseries of average US covid cases after emergence of Delta variant
                state_cases_delta_chart - Timeseries of State covid cases after emergence of Delta variant
                state_selectors (Hidden selectors for display of tooltip)
                rules - Tooltip rule line
                tooltip_text1 (Tooltip for state 1)
                tooltip_text2 (Tooltip for state 2)
                tooltip_text3 (Tooltip for US timeline)
                points (Points to display the tootip text)  
        """

    # Retrieve the data
    (
        state_vaccine_df,
        us_case_rolling_df,
        state_case_rolling_df,
    ) = getStateVaccinationDataWithAPI()

    # Create the vaccination Choropleth/Geo chart
    vaccine_chart, click = plotStateVaccinePct(
        state_vaccine_df, state_vaccine_df.date.max()
    )

    state_election_df = getStateLevelElectionData2020()

    state_case_rolling_df = state_case_rolling_df.merge(
        state_election_df[["state_fips", "party_simplified"]],
        how="left",
        left_on="STATEFP",
        right_on="state_fips",
    )
    state_case_rolling_df = state_case_rolling_df[
        ~state_case_rolling_df["party_simplified"].isnull()
    ]
    state_case_rolling_df["party_simplified"] = np.where(
        state_case_rolling_df["party_simplified"] == "REPUBLICAN",
        "STAYED_REPUBLICAN",
        "STAYED_DEMOCRAT",
    )
    state_case_rolling_df["party_simplified_color"] = state_case_rolling_df[
        "party_simplified"
    ].map(segment_color_dict)

    # Create a baseline US covid cases chart
    us_base = getBaseChart(
        us_case_rolling_df, [state_vaccine_df.date.min(), state_vaccine_df.date.max()]
    )
    us_timeseries = us_base.mark_line(
        strokeDash=[2, 6], strokeWidth=3, color="black"
    ).encode(tooltip=[alt.Tooltip("geoid:N", title="US Country Average Cases:")])

    # Create a "mean" cases timeseries chart of two segments - Stayed Democrat and Stayed Republican
    party_cases_timeseries_df = (
        state_case_rolling_df.groupby(
            ["date", "party_simplified", "party_simplified_color"]
        )
        .agg(
            cases_avg_per_100k=("cases_avg_per_100k", "mean"),
            deaths_avg_per_100k=("deaths_avg_per_100k", "mean"),
        )
        .reset_index()
    )

    # Democrat Mean
    stayed_democrat_base = getBaseChart(
        party_cases_timeseries_df[
            party_cases_timeseries_df["party_simplified"] == "STAYED_DEMOCRAT"
        ],
        [state_vaccine_df.date.min(), state_vaccine_df.date.max()],
    )

    stayed_democrat_timeseries = stayed_democrat_base.mark_line(
        strokeDash=[6, 2], strokeWidth=4, color="blue"
    ).encode(
        tooltip=[
            alt.Tooltip(
                "cases_avg_per_100k:Q", title="Stayed Democrat State Average Cases:"
            ),
        ]
    )

    # Republican Mean
    stayed_republican_base = getBaseChart(
        party_cases_timeseries_df[
            party_cases_timeseries_df["party_simplified"] == "STAYED_REPUBLICAN"
        ],
        [state_vaccine_df.date.min(), state_vaccine_df.date.max()],
    )

    stayed_republican_timeseries = stayed_republican_base.mark_line(
        strokeDash=[6, 2], strokeWidth=4, color="red"
    ).encode(
        tooltip=[
            alt.Tooltip(
                "cases_avg_per_100k:Q", title="Stayed Republican State Average Cases:"
            ),
        ]
    )

    # Create the dropdown selector for state names
    input_dropdown = alt.binding_select(
        options=[None] + state_case_rolling_df["state"].unique().tolist(),
        labels=["None"] + state_case_rolling_df["state"].unique().tolist(),
        name="State: ",
    )
    dropdown_selection = alt.selection_single(
        fields=["state"], bind=input_dropdown, name="State: ", init={"state": "None"}
    )

    # Each timeline will be colored by state affiliation
    state_color_tuples = list(
        state_case_rolling_df.groupby(["state", "party_simplified_color"]).groups.keys()
    )
    state_domain, color_range = zip(*state_color_tuples)

    line_base = alt.Chart(
        state_case_rolling_df,
        title="Covid cases after emergence of Delta variant in the US",
    )

    # Create the line chart base to plot tooltip points
    just_line_state_cases_delta = (
        line_base.mark_line()
        .encode(
            x=alt.X("date:T"),
            y=alt.Y("cases_avg_per_100k:Q"),
            detail="party_simplified",
            # color=alt.Color("changecolor:N", scale=None),
            color=alt.condition(
                dropdown_selection | click,
                alt.Color(
                    "state:N",
                    legend=None,
                    scale=alt.Scale(domain=state_domain, range=color_range),
                ),
                alt.value("lightgray"),
            ),
            opacity=alt.condition(
                dropdown_selection | click, alt.value(1), alt.value(0.2)
            ),
        )
        .properties(height=300, width=800)
    )

    ## This is the plot of the timeseries with selections added
    state_cases_delta_chart = (
        line_base.mark_line()
        .encode(
            x=alt.X("date:T"),
            y=alt.Y("cases_avg_per_100k:Q"),
            detail="party_simplified",
            # color=alt.Color("changecolor:N", scale=None),
            color=alt.condition(
                dropdown_selection | click,
                alt.Color(
                    "state:N",
                    legend=None,
                    scale=alt.Scale(domain=state_domain, range=color_range),
                ),
                alt.value("lightgray"),
            ),
            opacity=alt.condition(
                dropdown_selection | click, alt.value(1), alt.value(0.2)
            ),
            tooltip=[
                alt.Tooltip("state:N", title="State Name:"),
                alt.Tooltip("cases_avg_per_100k:Q", title="Cases per 100K:"),
            ],
        )
        .properties(height=300, width=800)
        .add_selection(dropdown_selection)
        .add_selection(click)
    )

    ####################################################################################################
    ## CREATE THE TOOLTIP COMPONENTS
    ####################################################################################################

    # Create a selection that chooses the nearest point & selects based on x-value
    base = line_base

    nearest = alt.selection(
        type="single", nearest=True, on="mouseover", fields=["date"], empty="none"
    )

    # Transparent selectors across the chart. This is what tells us
    # the x-value of the cursor
    state_selectors = (
        alt.Chart(state_case_rolling_df)
        .mark_point()
        .encode(x="date:T", opacity=alt.value(0),)
        .add_selection(nearest)
    )

    # Draw points on the line, and highlight based on selection
    points = just_line_state_cases_delta.mark_point(size=10, dy=-10).encode(
        opacity=alt.condition(nearest, alt.value(1), alt.value(0))
    )
    # .transform_filter(dropdown_selection)

    # Draw text labels near the points, and highlight based on selection
    tooltip_text1 = (
        just_line_state_cases_delta.mark_text(
            align="left",
            dx=-60,
            dy=-15,
            fontSize=15,
            # fontWeight="bold",
            lineBreak="\n",
        )
        .encode(
            text=alt.condition(
                nearest, alt.Text("cases_avg_per_100k:Q", format=".2f"), alt.value(" "),
            ),
        )
        .transform_filter(dropdown_selection)
    )

    # Draw text labels near the points, and highlight based on selection
    tooltip_text2 = (
        just_line_state_cases_delta.mark_text(
            align="left",
            dx=-60,
            dy=-15,
            fontSize=15,
            # fontWeight="bold",
            lineBreak="\n",
        )
        .encode(
            text=alt.condition(
                nearest, alt.Text("cases_avg_per_100k:Q", format=".2f"), alt.value(" "),
            ),
        )
        .transform_filter(click)
    )

    # US Time series Draw text labels near the points, and highlight based on selection
    tooltip_text3 = us_timeseries.mark_text(
        align="left",
        dx=-60,
        dy=-15,
        fontSize=15,
        # fontWeight="bold",
        lineBreak="\n",
    ).encode(
        text=alt.condition(
            nearest, alt.Text("cases_avg_per_100k:Q", format=".2f"), alt.value(" "),
        ),
    )

    # Stayed Democrat Time series Draw text labels near the points, and highlight based on selection
    tooltip_text4 = stayed_democrat_timeseries.mark_text(
        align="left",
        dx=-60,
        dy=-15,
        fontSize=15,
        # fontWeight="bold",
        lineBreak="\n",
    ).encode(
        text=alt.condition(
            nearest, alt.Text("cases_avg_per_100k:Q", format=".2f"), alt.value(" "),
        ),
    )

    tooltip_text5 = stayed_republican_timeseries.mark_text(
        align="left",
        dx=-60,
        dy=-15,
        fontSize=15,
        # fontWeight="bold",
        lineBreak="\n",
    ).encode(
        text=alt.condition(
            nearest, alt.Text("cases_avg_per_100k:Q", format=".2f"), alt.value(" "),
        ),
    )

    # Draw a rule at the location of the selection
    rules = (
        alt.Chart(state_case_rolling_df)
        .mark_rule(color="darkgrey", strokeWidth=2, strokeDash=[5, 4])
        .encode(x="date:T",)
        .transform_filter(nearest)
    )

    return (
        vaccine_chart,
        us_timeseries,
        stayed_democrat_timeseries,
        stayed_republican_timeseries,
        state_cases_delta_chart,
        state_selectors,
        rules,
        tooltip_text1,
        tooltip_text2,
        tooltip_text3,
        tooltip_text4,
        tooltip_text5,
        points,
    )


############################################################################################################
######################Mask Data Charts with interactive legend
############################################################################################################
def getMaskUsageRange(mask_usage):
    """This function creates ranges for percentage mask usage
       The three ranges created are "Below average (<=50%)", "Average (50%-80%)" and "Exceptional (> 80%)"

    Args:
        mask_usage ([float]): [Estimated mask usage value]

    Returns:
        [string]: [Range of usage]
    """
    if mask_usage <= 0.5:
        return "Below average (<=50%)"
    elif mask_usage > 0.5 and mask_usage <= 0.8:
        return "Average (50%-80%)"
    else:
        return "Exceptional (> 80%)"


def getColorRangeMaskUsage(segmentname, mask_usage_range):
    """[This function comverts a combination of political affiliation and mask usage range into a color]

    Args:
        segmentname ([String]): [Democrat/Republican]
        mask_usage_range ([type]): [Ranges of mask uasge percentage]

    Returns:
        [string]: [Hex Code of color]
    """
    legend_dict = {
        ("Democrat", "Below average (<=50%)"): "#C5DDF9",
        ("Democrat", "Average (50%-80%)"): "#3CA0EE",
        ("Democrat", "Exceptional (> 80%)"): "#0015BC",
        ("Republican", "Below average (<=50%)"): "#F2A595",
        ("Republican", "Average (50%-80%)"): "#EE8778",
        ("Republican", "Exceptional (> 80%)"): "#FE0000",
    }
    return legend_dict[(segmentname, mask_usage_range)]


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


#########################################################################################


def createFreqCountyMaskUsageWithRanges(type):
    """[This function accepts the type of Mask usage - Frequent or Infrequent and creates a
        Geo chart with colors based o nrange of frequent mask usage]

    Args:
        type ([string]): ["FREQUENT"/"NON FREQUENT"]

    Returns:
        [type]: [description]
    """

    counties = alt.topo_feature(data.us_10m.url, "counties")
    # Setup interactivty
    click = alt.selection_single(
        fields=["range_color"], init={"range_color": "#FE0000"}
    )

    # Get data
    (
        county_pop_mask_df,
        county_pop_mask_freq_df,
        county_pop_mask_infreq_df,
    ) = createDataForFreqAndInFreqMaskUse()

    if type == "FREQUENT":
        source = county_pop_mask_freq_df
    else:
        source = county_pop_mask_infreq_df

    county_mask_chart = (
        alt.Chart(
            counties,
            title={
                "text": [
                    f"{type.capitalize()} Mask Usage From Survey Response by County and Political Affiliation"
                ],
                "subtitle": [
                    "Hover for tooltip with state and mask usage percent value",
                    "Click on legend colors for selecting counties with given political affiliation",
                    "and chosen range of mask usage",
                ],
            },
        )
        .mark_geoshape(stroke="#706545", strokeWidth=0.1)
        .encode(
            color=alt.condition(
                click, alt.value("#b38449"), alt.Color("range_color:N", scale=None),
            ),
            tooltip=[
                # alt.Tooltip('state:N', title='State: '),
                alt.Tooltip("CTYNAME:N", title="County name: "),
                alt.Tooltip("mask_usage_type:N", title="Mask Usage Type: "),
                alt.Tooltip("mask_usage:Q", title="Mask Usage Percent: ", format=".0%"),
            ],
        )
        .transform_lookup(
            lookup="id",
            from_=alt.LookupData(
                source,
                "COUNTYFP",
                [
                    "COUNTYFP",
                    "CTYNAME",
                    "mask_usage_type",
                    "mask_usage",
                    "mask_usage_range",
                    "range_color",
                ],
            ),
        )
        .add_selection(click)
        .project(type="albersUsa")
        .properties(width=750, height=500)
    )

    # Create interactive model name legend
    legend_democrat = (
        alt.Chart(source[source["segmentname"] == "Democrat"], title="Democrat(2020)",)
        .mark_point(size=100, filled=True)
        .encode(
            y=alt.Y(
                "mask_usage_range:N",
                axis=alt.Axis(orient="right"),
                title=None,
                sort=[
                    "Below average (<=50%)",
                    "Average (50%-80%)",
                    "Exceptional (> 80%)",
                ],
            ),
            color=alt.condition(
                click,
                alt.value("#b38449"),
                alt.Color(
                    "mask_usage_range:N",
                    scale=alt.Scale(
                        domain=[
                            "Below average (<=50%)",
                            "Average (50%-80%)",
                            "Exceptional (> 80%)",
                        ],
                        range=["#C5DDF9", "#3CA0EE", "#0015BC"],
                    ),
                    legend=None,
                ),
            ),
        )
        .add_selection(click)
        .properties(width=40)
    )

    legend_republican = (
        alt.Chart(
            source[source["segmentname"] == "Republican"],
            title="Select: Republican(2020)",
        )
        .mark_point(size=100, filled=True)
        .encode(
            y=alt.Y(
                "mask_usage_range:N",
                axis=alt.Axis(orient="right"),
                title=None,
                sort=[
                    "Below average (<=50%)",
                    "Average (50%-80%)",
                    "Exceptional (> 80%)",
                ],
            ),
            color=alt.condition(
                click,
                alt.value("#b38449"),
                alt.Color(
                    "mask_usage_range:N",
                    scale=alt.Scale(
                        domain=[
                            "Below average (<=50%)",
                            "Average (50%-80%)",
                            "Exceptional (> 80%)",
                        ],
                        range=["#F2A595", "#EE8778", "#FE0000"],
                    ),
                    legend=None,
                ),
            ),
        )
        .add_selection(click)
        .properties(width=40)
    )

    # create an average chart
    average_mask_chart = (
        alt.Chart(source, title=f"Average {type.capitalize()} mask usage")
        .mark_bar()
        .encode(
            y=alt.Y("mask_usage_range:N", title=None),
            x=alt.X("mean(mask_usage):Q", title=None),
            color=alt.Color(
                "segmentname:N",
                scale=alt.Scale(
                    domain=["Republican", "Democrat"], range=["#FE0000", "#0015BC"]
                ),
                legend=alt.Legend(title="Voted(2020)"),
            ),
        )
    ).properties(width=100)

    return county_mask_chart, legend_republican, legend_democrat, average_mask_chart


#########################################################################################
#########################################################################################
#########################################################################################
#########################################################################################
#########################################################################################
#########################################################################################
#########################################################################################


########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################
########################################################################################


########################################################################################
# This plots a hover with all states
########################################################################################

# def createTooltip():
#     """
#         This function creates a tooltip containing the date and all stock prices displayed upon hover

#     """

#     states = sorted(state_case_rolling_df.state.unique())

#     hover = alt.selection_single(
#         fields=["date"],
#         nearest=True,
#         on="mouseover",
#         empty="none",
#         clear="mouseout",
#     )
#     tooltips = alt.Chart(state_case_rolling_df).transform_pivot(
#         "state", "cases_avg_per_100k", groupby=["date"]
#     ).mark_rule(color="darkgrey", strokeWidth=2, strokeDash=[5, 4]).encode(
#         x='date:T',
#         opacity=alt.condition(hover, alt.value(1), alt.value(0)),
#         tooltip=[alt.Tooltip(state, type='quantitative') for state in states]
#     ).add_selection(hover)
#     return tooltips

# tooltips =  createTooltip()


########################################################################################
# This plots the county level vaccinations
########################################################################################

# import pandas as pd
# import requests
# import io
# import pickle


# import io
# def csv_to_dataframe(response):
#     """
#     Convert response to dataframe

#     """
#     return pd.read_csv(io.StringIO(response.text))


# ### Read the pickle file with stored token
# pickle_in = open("APIToken.pickle","rb")
# APITokenIn = pickle.load(pickle_in)
# curr_offset = 0
# num_file = 1
# url = f"https://data.cdc.gov/resource/8xkx-amqh.csv?$$app_token={APITokenIn}&$limit=500000&$offset={curr_offset}&$order=date"
# response = requests.request("GET", url)
# df = csv_to_dataframe(response)
# df.to_csv(f"CountyVaccineDataFile{num_file}.csv")
# curr_offset = curr_offset + 500000
# num_file = num_file + 1


# while pd.to_datetime(df.date.max()) <= pd.to_datetime('2021-07-31'):

#     url = f"https://data.cdc.gov/resource/8xkx-amqh.csv?$$app_token={APITokenIn}&$limit=500000&$offset={curr_offset}&$order=date"
#     response = requests.request("GET", url)
#     df = csv_to_dataframe(response)
#     df.to_csv(f"CountyVaccineDataFile{num_file}.csv")
#     curr_offset +=  500000
#     num_file = num_file + 1

# import pandas as pd
# import os

# folder_name =  os.listdir(DataFolder / r"../DataForPresidentialElectionsAndCovid/Dataset 8 County Vaccine Data Using API/")
# path_name = r"../DataForPresidentialElectionsAndCovid/Dataset 8 County Vaccine Data Using API/"

# full_df = pd.DataFrame()
# for name in folder_name:
#     if name.startswith("CountyVaccineDataFile"):
#         df = pd.read_csv(path_name+name)
#         df = df[['date','fips','mmwr_week', 'recip_county','recip_state','administered_dose1_recip', 'administered_dose1_pop_pct',  'administered_dose1_recip_18plus', 'administered_dose1_recip_18pluspop_pct']].copy()
#         if len(df) > 0:
#             full_df = full_df.append(df, ignore_index=True)
#             #print(name)


# full_df.date = pd.to_datetime(full_df.date)
# full_df = full_df[full_df['fips'] != 'UNK' ].copy()
# full_df.fips = full_df.fips.astype('int')
# full_df = full_df.rename(columns={'administered_dose1_recip_18pluspop_pct' : 'Percent with one dose', 'date':'Date'})
# min_date = full_df[full_df['Percent with one dose'] > 0]['Date'].min()
# max_date = full_df[full_df['Percent with one dose'] > 0]['Date'].max()
# full_df['day_num'] = (full_df['Date'] - min_date).dt.days

# from vega_datasets import data

# def plotCountyVaccinePct(df, date_in):

#     source = df[df['Date']==date_in].copy()
#     counties = alt.topo_feature(data.us_10m.url, 'counties')
#     us_states = alt.topo_feature(data.us_10m.url, 'states')


#     click = alt.selection_multi(fields=['recip_state'])
#     chart = alt.Chart(counties).mark_geoshape().encode(
#         color = alt.Color(
#             'Percent with one dose:Q',
#             scale=alt.Scale(scheme='yelloworangebrown')
#     ),
#       tooltip=[alt.Tooltip('recip_state:N',  title='State name: '),
#               alt.Tooltip('recip_county:N',  title='County name: '),
#               alt.Tooltip('Percent with one dose:Q', title='Population Pct over 18 with one shot: ')]
#     ).transform_lookup(
#         lookup='id',
#         from_=alt.LookupData(source, 'fips', ['Percent with one dose', 'fips', 'recip_county', 'recip_state' ])
#     ).add_selection(click   ## Make sure you have added the selection here
#     ).project(
#         type='albersUsa'
#     ).properties(
#         width=850,
#         height=500
#     )

#     outline = alt.Chart(us_states).mark_geoshape(stroke='grey', fillOpacity=0).project(
#         type='albersUsa'
#     ).properties(
#         width=850,
#         height=500
#     )

#     return  chart + outline
# plotCountyVaccinePct(full_df, max_date)

