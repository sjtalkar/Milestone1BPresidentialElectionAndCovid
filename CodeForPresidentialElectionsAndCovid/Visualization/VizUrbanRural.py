
import pandas as pd
import numpy as np
import altair as alt
import sys

sys.path.append("../ETL")
from ETL.EtlBase import DataFolder
from ETL.EtlUrbanRural import *
from ETL.EtlCovid import *
from Visualization.VizBase import *
from Visualization.VizCovid import *

#######################################################################################################

def ElectionUrbanRuralDensityPlot(PEUrbanRuralDF:pd.DataFrame()=None):
    '''
    Plots the urban/rural designation of the counties merged with the county-level
        presidential election results as a density plot
    Returns a chart showing:
        x-axis: party - categorical - Winning candidate's party
        y-axis: PctRural - quantitative - Percentage of "how rural" a county is

    Called by: Main code
    Functions called: MergeElectionUrbanRural()
    '''

    party_domain = ["DEMOCRAT", "REPUBLICAN"]
    party_range = ["#030D97", "#970D03"]

    if PEUrbanRuralDF is None:
        PEUrbanRuralDF = MergeElectionUrbanRural()

    PctRuralDomain = [0, int(PEUrbanRuralDF["PctRural"].max() / 10) * 10]

    densityplot = alt.Chart(
        PEUrbanRuralDF,
        width=500,
        height=400,
        title={
            "text": [
                "Distribution of counties by percent rural"
            ],
            "subtitle": ["Counties won by democratic candidates vs won by republicans",],
        }
    ).transform_density(
        density="PctRural",
        groupby=["party"],
        as_=["Percent Rural", "Density"]
    ).mark_area(orient="vertical", opacity=0.8).encode(
        x=alt.X("Percent Rural:Q",
                scale=alt.Scale(domain=PctRuralDomain)),
        y=alt.Y("Density:Q",
                scale=alt.Scale(domain=[0, 0.02])),
        color=alt.Color("party:N",
                        scale=alt.Scale(domain=party_domain, range=party_range),
                        title="Party", legend=None)
    ).configure_title(
        align="left",
        anchor="start"
    )
    return densityplot

#######################################################################################################

def UrbanRuralCorrelation(PEUrbanRuralDF:pd.DataFrame()=None):
    '''
    Plots the "percent rural" of counties vs fraction of the vote
        for the winning party, a separate chart for each party
    Returns two charts, one for each party, showing:
        x-axis: Fraction of the vote - quantitative
        y-axis: PctRural - quantitative

    Called by: Main code
    Functions called: MergeElectionUrbanRural()
    '''


    party_domain = ["DEMOCRAT", "REPUBLICAN"]
    party_range = ["#030D97", "#970D03"]

    # Get the data and split into democrat and republican
    if PEUrbanRuralDF is None:
        PEUrbanRuralDF = MergeElectionUrbanRural()


    PEUrbanRuralDF2 = PEUrbanRuralDF.copy()
    PEUrbanRuralDF2['fractionvotes'] = PEUrbanRuralDF2['candidatevotes'] / PEUrbanRuralDF2['totalvotes']

    PEUrbanRuralDFDem = PEUrbanRuralDF2.copy()
    PEUrbanRuralDFDem = PEUrbanRuralDFDem[PEUrbanRuralDFDem['party'] == 'DEMOCRAT']

    PEUrbanRuralDFRep = PEUrbanRuralDF2.copy()
    PEUrbanRuralDFRep = PEUrbanRuralDFRep[PEUrbanRuralDFRep['party'] == 'REPUBLICAN']


    PctRuralDomain = [0, int(PEUrbanRuralDF2["PctRural"].max() / 10) * 10]

    # Prepare the democrat chart
    voteDEMplot = alt.Chart(
        PEUrbanRuralDFDem,
        width=400,
        height=200,
        title={
            "text": [
                "Correlation between percent rural and fraction of county vote"
            ],
            "subtitle": ["Counties won by democratic candidates",],
            "align": "left",
            "anchor": "start"
        }
    ).mark_point(filled=True, size=30).encode(
        y=alt.Y(
            "PctRural:Q",
            title="Percent Rural",
            scale=alt.Scale(
                domain=PctRuralDomain)
        ),
        x=alt.X(
            "fractionvotes:Q",
            title="Fraction of Vote",
            scale=alt.Scale(
                domain=[0, 1.0])),
        color=alt.Color("party:N", scale=alt.Scale(domain=party_domain, range=party_range),
                        title="Party", legend=None)
    )

    # Add a regression line
    finalDEMchart=(voteDEMplot + voteDEMplot.transform_regression("fractionvotes",
                                                          "PctRural").mark_line(
        color="black").encode(color=alt.value("black"))
    )

    # Prepare the republican chart
    voteREPplot = alt.Chart(
        PEUrbanRuralDFRep,
        width=400,
        height=200,
        title={
            "text": [
                "Correlation between percent rural and fraction of county vote"
            ],
            "subtitle": ["Counties won by republican candidates",],
            "align": "left",
            "anchor": "start"
        }
    ).mark_point(filled=True, size=30).encode(
        y=alt.Y(
            "PctRural:Q",
            title=None,
            scale=alt.Scale(
                domain=PctRuralDomain)
        ),
        x=alt.X(
            "fractionvotes:Q",
            title="Fraction of Vote",
            scale=alt.Scale(
                domain=[0, 1.0])),
        color=alt.Color("party:N", scale=alt.Scale(domain=party_domain, range=party_range),
                        title="Party", legend=None)
    )

    # Add a regression line
    finalREPchart=(voteREPplot + voteREPplot.transform_regression("fractionvotes",
                                                          "PctRural").mark_line(
        color="black").encode(color=alt.value("black"))
    )

    finalchart = alt.ConcatChart(concat=[finalDEMchart, finalREPchart])

    return finalchart

#######################################################################################################

def UrbanRuralRollingAvgCompChart(FullDF:pd.DataFrame()=None, UrbanDF:pd.DataFrame()=None, RuralDF:pd.DataFrame()=None):
    '''
    Forms a comparison chart showing rolling average of Covid cases for all counties,
    urban counties and rural counties.

    Returns a vertical concatenation of the three charts.

    Called by: Main code
    Functions called: UrbanRuralRollingAvgSingleChart()
    '''

    if (FullDF is None) and (UrbanDF is None) and (RuralDF is None):
        # Get three county election dataframes
        FullDF, UrbanDF, RuralDF = CountyElecUrbanRuralSplit(getUrbanRuralElectionRollingData)

    # Get chart for all counties
    FullChart = UrbanRuralRollingAvgSingleChart(FullDF)

    # Get chart for urban counties
    UrbanChart = UrbanRuralRollingAvgSingleChart(UrbanDF)

    # Get chart for rural counties
    RuralChart = UrbanRuralRollingAvgSingleChart(RuralDF)

    # Concatenate the charts vertically
    RollingAvgComparisonChart  = alt.vconcat(FullChart, UrbanChart, RuralChart)


    return RollingAvgComparisonChart

#######################################################################################################

def UrbanRuralRollingAvgSingleChart(case_rolling_df:pd.DataFrame()=None):    # Code mostly copied from Covid notebook
    '''
    Forms a chart of Covid case rolling average.

    Returns a single chart.

    Called by: UrbanRuralCompChart()
    Functions called: getRollingCaseAverageSegmentLevel()
    '''

    if (case_rolling_df is None):
        # Get rolling average of cases by segment
        case_rolling_df = getRollingCaseAverageSegmentLevel()

    # Create the chart
    base, make_selector, highlight_segment, radio_select  = createCovidConfirmedTimeseriesChart(case_rolling_df)
    selectors, rules, points, tooltip_text  = createTooltip(base, radio_select, case_rolling_df)

    # Bring all the layers together with layering and concatenation
    SingleChart = ( alt.layer(
        highlight_segment, selectors, points,rules, tooltip_text ) | make_selector
    )

    return SingleChart

#######################################################################################################

def UrbanRuralAvgDeathsCompChart(FullDF:pd.DataFrame()=None, UrbanDF:pd.DataFrame()=None, RuralDF:pd.DataFrame()=None):
    '''
    Forms a comparison chart showing average Covid deaths for all counties,
    urban counties and rural counties.

    Returns a vertical concatenation of the three charts.

    Called by: Main code
    Functions called: CountyElecUrbanRuralSplit()
                      createPercentPointChangeAvgDeathsChart()
    '''

    if (FullDF is None) and (UrbanDF is None) and (RuralDF is None):
        # Get three county election dataframes
        FullDF, UrbanDF, RuralDF = CountyElecUrbanRuralSplit(getUrbanRuralAvgDeathsData)

    # Get chart for all counties
    FullChart = createPercentPointChangeAvgDeathsChart(FullDF)

    # Get chart for urban counties
    UrbanChart = createPercentPointChangeAvgDeathsChart(UrbanDF)

    # Get chart for rural counties
    RuralChart = createPercentPointChangeAvgDeathsChart(RuralDF)

    # Concatenate the charts vertically
    AvgDeathsComparisonChart  = alt.vconcat(FullChart, UrbanChart, RuralChart).configure_title(
        align="left",
        anchor="start"
    )

    return AvgDeathsComparisonChart

#######################################################################################################
