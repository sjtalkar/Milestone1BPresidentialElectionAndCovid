import pandas as pd
import altair as alt
import sys

sys.path.append("../ETL")
from ETL.EtlBase import segment_color_dict
from ETL.EtlUnemployment import (getUnemploymentRate,
                                 getUnemploymentRateSince122019,
                                 getJuly2020UnemploymentAndMask,
                                 getUnemploymentAndVaccine)
from ETL.EtlElection import getStateLevelElectionData2020
from ETL.EtlCovid import *

party_domain = ["DEMOCRAT", "REPUBLICAN"]
party_range = ["#030D97", "#970D03"]


def timestamp(t):
    return pd.to_datetime(t).timestamp() * 1000

def createMonthSlider(df:pd.DataFrame(), month_text: str):
    # Create the slider for the month
    min_month_nb = df["month_since_start"].min()
    max_month_nb = df["month_since_start"].max()
    month_slider = alt.binding_range(
        name=month_text,
        min=min_month_nb,
        max=max_month_nb,
        step=1)
    month_selector = alt.selection_single(name="month_selector",
                                          fields=["month_since_start"],
                                          bind=month_slider,
                                          init={"month_since_start": min_month_nb})
    return month_selector

def createUnemploymentChart(df:pd.DataFrame() = None):
    if df is None:
        df = getUnemploymentRateSince122019()
    unemployment_domain = [0, int(df["unemployment_rate"].max() / 10 + 1) * 10]
    # Create the slider for the month
    month_selector = createMonthSlider(df=df, month_text="Elapsed months: ")
    # Prepare the plot itself
    unemployment_chart = alt.Chart(
        df,
        width=500,
        height=400,
        title={
            "text": [
                "Distribution of counties' monthly unemployment rate since 2019"
            ],
            "subtitle": ["Move the month slider at the bottom (1 = December 2019)",],
        }
    ).transform_filter(month_selector).transform_density(
        density="unemployment_rate",
        groupby=["party"],
        as_=["unemployment_rate", "density"]
    ).mark_area(orient="vertical", opacity=0.8).encode(
        x=alt.X("unemployment_rate:Q",
                scale=alt.Scale(domain=unemployment_domain)),
        y=alt.Y("density:Q",
                scale=alt.Scale(domain=[0, 0.6])),
        color=alt.Color("party:N",
                        scale=alt.Scale(domain=party_domain, range=party_range),
                        title="Party", legend=None)
    ).add_selection(
        month_selector
    ).configure_title(
        align="left",
        anchor="start"
    )
    return unemployment_chart

def createUnemploymentCovidCasesChart(df:pd.DataFrame()=None):
    if df is None:
        df = getUnemploymentRate("county")
        df.dropna(inplace=True)
        df.drop(columns=["month"], inplace=True)
    unemployment_domain = [0, int(df["unemployment_rate"].max() / 10 + 1) * 10]
    covid_cases_domain = [0, int(df["cases_avg_per_100k"].max()/1000+1)*1000]
    month_selector = createMonthSlider(df=df,
                month_text="Elapsed months since the beginning of the pandemic: ")
    # Prepare the plot itself
    unemployment_vs_covid_plot = alt.Chart(
        df,
        width=800,
        height=400,
        title={
            "text": [
                "Correlation between monthly unemployment rate and average Covid-19 cases per 100k"
            ],
            "subtitle": ["on a square root scale",
                         "Move the month slider at the bottom (1 = January 2020)",],
        }
    ).transform_filter(alt.datum.cases_avg_per_100k > 0).mark_point(filled=True, size=30).encode(
        y=alt.Y(
            "unemployment_rate:Q",
            title="Unemployment Rate",
            scale=alt.Scale(
                domain=unemployment_domain,
                type="sqrt"
            )
        ),
        x=alt.X(
            "cases_avg_per_100k:Q",
            title="Monthly Total of Daily Average Covid Cases per 100k",
            scale=alt.Scale(
                domain=covid_cases_domain,
                type="sqrt"
            )
        ),
        color=alt.Color("party:N", scale=alt.Scale(domain=party_domain, range=party_range),
                        title="Party", legend=None)
    ).configure_title(
        align="left",
        anchor="start"
    )

    # final_chart=(unemployment_vs_covid_plot + unemployment_vs_covid_plot.transform_regression("cases_avg_per_100k", "unemployment_rate").mark_line(color="black").encode(color=alt.value("black"))).add_selection(
    #    month_selector
    # ).transform_filter(month_selector)
    final_chart=unemployment_vs_covid_plot.add_selection(month_selector).transform_filter(month_selector)
    return final_chart

def createUnemploymentCovidDeathChart(df:pd.DataFrame() = None):
    if df is None:
        df = getUnemploymentRate("county")
        df.dropna(inplace=True)
        df.drop(columns=["month"], inplace=True)
    unemployment_domain = [0, int(df["unemployment_rate"].max() / 10 + 1) * 10]
    covid_deaths_domain=[0,int(df["deaths_avg_per_100k"].max()/100+1)*100]
    month_selector = createMonthSlider(df=df,
                month_text="Elapsed months since the beginning of the pandemic: ")
    # Prepare the plot itself
    unemployment_vs_covid_plot = alt.Chart(
        df,
        width=800,
        height=400,
        title={
            "text": [
                "Correlation between monthly unemployment rate and average Covid-19 deaths per 100k"
            ],
            "subtitle": ["on a square root scale",
                         "Move the month slider at the bottom (1 = January 2020)",],
        }
    ).transform_filter(alt.datum.deaths_avg_per_100k > 0).mark_point(filled=True, size=30).encode(
        y=alt.Y(
            "unemployment_rate:Q",
            title="Unemployment Rate",
            scale=alt.Scale(domain=unemployment_domain,
                            type="sqrt"
            )
        ),
        x=alt.X(
            "deaths_avg_per_100k:Q",
            title="Monthly Total of Daily Average Covid Deaths per 100k",
            scale=alt.Scale(domain=covid_deaths_domain,
                            type="sqrt"
            )
        ),
        color=alt.Color("party:N", scale=alt.Scale(domain=party_domain, range=party_range),
                        title="Party", legend=None)
    ).add_selection(
        month_selector
    ).transform_filter(month_selector).configure_title(
        align="left",
        anchor="start"
    )
    return unemployment_vs_covid_plot

def createUnemploymentMaskChart(df:pd.DataFrame() = None):
    if df is None:
        df = getJuly2020UnemploymentAndMask("county", getUnemploymentRate("county"))
    unemployment_domain = [0, int(df["unemployment_rate"].max() / 10 + 1) * 10]
    never_wear_mask_domain = [0, 1]
    # Prepare the plot base for the 5 plots (one for each mask usage)
    mask_plot_base = alt.Chart(df).mark_point(filled=True, size=30).encode(
        color=alt.Color(
            "party:N",
            scale=alt.Scale(domain=party_domain, range=party_range),
            title="Party",
            legend=None)
    ).properties(
        width=400, height=400,
    )
    # Prepare the 5 plots
    unemployment_vs_mask_plot = alt.vconcat()
    for row_values in [["NEVER", "ALWAYS"], ["RARELY", "FREQUENTLY"], ["SOMETIMES"]]:
        row = alt.hconcat()
        for chart_value in row_values:
            corr = df["unemployment_rate"].corr(df[chart_value])
            mask_plot = mask_plot_base.encode(
                x=alt.X(
                    chart_value,
                    title=f"Percentage of people saying they '{chart_value.lower()}' wear a mask"
                ),
                y=alt.Y(
                    "unemployment_rate:Q",
                    title="Unemployment Rate",
                    scale=alt.Scale(domain=unemployment_domain))
            )
            mask_plot = mask_plot.properties(
                title={
                    "text": [""],
                    "subtitle": [f"Pearson correlation {corr:.3f}"],
                }
            )

            corr_line = mask_plot.transform_regression(chart_value, "unemployment_rate").mark_line(
                color="black").encode(color=alt.value("black"))
            row |= mask_plot + corr_line
        unemployment_vs_mask_plot &= row

    unemployment_vs_mask_plot = unemployment_vs_mask_plot.properties(
        title="Correlation between monthly unemployment rate and mask usage in July 2020"
    ).configure_title(
                align="left",
                anchor="start"
            )

    return unemployment_vs_mask_plot


def createUnemploymentVaccineChart(df:pd.DataFrame() = None):
    if df is None:
        df = getUnemploymentAndVaccine(unemployment_covid_df=getUnemploymentRate("county"))
    unemployment_domain = [0, int(df["unemployment_rate"].max() / 5 + 1) * 5]
    # Create the slider for the month
    month_selector = createMonthSlider(df=df,
                month_text="Elapsed months since the beginning of the pandemic: ")
    # Prepare the chart itself
    unemployment_vs_vaccine_plot = alt.Chart(
        df,
        width=800,
        height=400,
        title={
            "text": [
                "Correlation between monthly unemployment rate and percentage of people with at least 1 vaccine dose"
            ],
            "subtitle": ["Move the month slider at the bottom (1 = January 2020)",],
        }
    ).transform_filter(alt.datum.percent_with_1_dose > 0).mark_point(filled=True, size=30).encode(
        y=alt.Y(
            "unemployment_rate:Q",
            title="Unemployment Rate",
            scale=alt.Scale(
                domain=unemployment_domain,
            )
        ),
        x=alt.X(
            "percent_with_1_dose:Q",
            title="Percentage of population with at least 1 dose of vaccine",
            scale=alt.Scale(
                domain=[0, 100])),
        color=alt.Color("party:N", scale=alt.Scale(domain=party_domain, range=party_range),
                        title="Party", legend=None)
    )

    # final_chart=unemployment_vs_vaccine_plot.add_selection(month_selector).transform_filter(month_selector)
    final_chart=(unemployment_vs_vaccine_plot + unemployment_vs_vaccine_plot.transform_regression("percent_with_1_dose",
                                                                                      "unemployment_rate").mark_line(
        color="black").encode(color=alt.value("black"))).add_selection(
        month_selector
    ).transform_filter(month_selector).configure_title(
        align="left",
        anchor="start"
    )
    return final_chart