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
                scale=alt.Scale(domain=unemployment_domain),
                title="Unemployment Rate"),
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
    # Get a matrix of the covid cases/unemployment correlation per month
    corr_series=df[["month_since_start", "unemployment_rate", "cases_avg_per_100k"]].groupby(
        ["month_since_start"])[["unemployment_rate", "cases_avg_per_100k"]].corr().iloc[0::2, -1]
    corr_df = corr_series.reset_index()
    corr_df.drop(columns=["level_1"], inplace=True)
    corr_df.rename(columns={"cases_avg_per_100k": "correlation"}, inplace=True)

    df = pd.merge(df, corr_df, how="left", on=["month_since_start"])

    unemployment_domain = [0, int(df["unemployment_rate"].max() / 10 + 1) * 10]
    covid_cases_domain = [0, int(df["cases_avg_per_100k"].max()/1000+1)*1000]
    month_selector = createMonthSlider(df=df,
                month_text="Elapsed months since the beginning of the pandemic: ")
    # Prepare the plot itself
    unemployment_vs_covid_plot = alt.Chart(
        df,
        width=600,
        height=600,
        title={
            "text": [
                "Correlation, per month, and average Covid-19 cases per 100k and unemployment rate"
            ],
            "subtitle": ["on a square root scale",
                         "Move the month slider at the bottom (1 = January 2020)"],
        }
    ).transform_filter(alt.datum.cases_avg_per_100k > 0).mark_point(filled=True, size=30).encode(
        x=alt.X(
            "unemployment_rate:Q",
            title="Unemployment Rate",
            scale=alt.Scale(
                domain=unemployment_domain,
                type="sqrt"
            )
        ),
        y=alt.Y(
            "cases_avg_per_100k:Q",
            title="Monthly Total of Daily Average Covid Cases per 100k",
            scale=alt.Scale(
                domain=covid_cases_domain,
                type="sqrt"
            )
        ),
        color=alt.Color("party:N", scale=alt.Scale(domain=party_domain, range=party_range),
                        title="Party", legend=None)
    )

    unemployment_vs_covid_corr = alt.Chart(df).mark_text(
        align='left', baseline='bottom', dx=+5, dy=-5, fontSize=12,
    ).encode(
        x=alt.value(df["correlation"].max()),
        text="_label:N",
    ).transform_calculate(
        _label='"Pearson correlation = " + format(datum.x, ".2f")',
    )

    #final_chart=(unemployment_vs_covid_plot + unemployment_vs_covid_plot.transform_regression(
    #    "unemployment_rate",
    #    "cases_avg_per_100k"
    #).mark_line(color="black").encode(color=alt.value("black")) + unemployment_vs_covid_corr).add_selection(
    #   month_selector
    #).transform_filter(month_selector).configure_title(
    #    align="left",
    #    anchor="start"
    #)
    final_chart=(unemployment_vs_covid_plot+unemployment_vs_covid_corr).add_selection(month_selector).transform_filter(month_selector).configure_title(
        align="left",
        anchor="start"
    )
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
                "Correlation, per month, between and average Covid-19 deaths per 100k and unemployment rate"
            ],
            "subtitle": ["on a square root scale",
                         "Move the month slider at the bottom (1 = January 2020)"],
        }
    ).transform_filter(alt.datum.deaths_avg_per_100k > 0).mark_point(filled=True, size=30).encode(
        x=alt.X(
            "unemployment_rate:Q",
            title="Unemployment Rate",
            scale=alt.Scale(domain=unemployment_domain,
                            type="sqrt"
            )
        ),
        y=alt.Y(
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

def createUnemploymentMaskChart(freq_df:pd.DataFrame() = None, infreq_df:pd.DataFrame() = None):
    if (freq_df is None) or (infreq_df is None):
        freq_df, infreq_df = getJuly2020UnemploymentAndMask("county", getUnemploymentRate("county"))

    unemployment_domain = [0, max(
        int(freq_df["unemployment_rate"].max() / 10 + 1) * 10,
        int(infreq_df["unemployment_rate"].max() / 10 + 1) * 10
    )]
    never_wear_mask_domain = [0, 1]
    # Prepare the plot for FREQUENT mask usage
    freq_mask_corr = freq_df["unemployment_rate"].corr(freq_df["mask_usage"])
    freq_mask_plot = alt.Chart(
        freq_df,
        width=400,
        height=400,
        title={
            "text": [""],
            "subtitle": [f"Pearson correlation {freq_mask_corr:.3f}"],
        }
    ).mark_point(filled=True, size=30).encode(
        color=alt.Color(
            "party:N",
            scale=alt.Scale(domain=party_domain, range=party_range),
            title="Party",
            legend=None)
    ).encode(
        y=alt.Y(
            "mask_usage:Q",
            title=f"Percentage of people saying they 'frequently' wear a mask"
        ),
        x=alt.X(
            "unemployment_rate:Q",
            title="Unemployment Rate",
            scale=alt.Scale(domain=unemployment_domain))
    )

    # Prepare the plot for NOT FREQUENT mask usage
    infreq_mask_corr = infreq_df["unemployment_rate"].corr(infreq_df["mask_usage"])
    infreq_mask_plot = alt.Chart(
        infreq_df,
        width=400,
        height=400,
        title={
            "text": [""],
            "subtitle": [f"Pearson correlation {infreq_mask_corr:.3f}"],
        }
    ).mark_point(filled=True, size=30).encode(
        color=alt.Color(
            "party:N",
            scale=alt.Scale(domain=party_domain, range=party_range),
            title="Party",
            legend=None)
    ).encode(
        y=alt.Y(
            "mask_usage:Q",
            title=f"Percentage of people saying they do 'not frequently' wear a mask"
        ),
        x=alt.X(
            "unemployment_rate:Q",
            title="Unemployment Rate",
            scale=alt.Scale(domain=unemployment_domain))
    )


    #corr_line = mask_plot.transform_regression("unemployment_rate", chart_value).mark_line(
    #    color="black").encode(color=alt.value("black"))
    #unemployment_vs_mask_plot |= mask_plot + corr_line

    unemployment_vs_mask_plot = alt.hconcat(freq_mask_plot, infreq_mask_plot).properties(
        title="Correlation between mask usage and unemployment rate in July 2020"
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
        width=600,
        height=600,
        title={
            "text": [
                "Correlation, per month, between percentage of people with at least 1 vaccine dose and unemployment rate"
            ],
            "subtitle": ["Move the month slider at the bottom (12 = January 2021)",],
        }
    ).transform_filter(alt.datum.percent_with_1_dose > 0).mark_point(filled=True, size=30).encode(
        x=alt.X(
            "unemployment_rate:Q",
            title="Unemployment Rate",
            scale=alt.Scale(
                domain=unemployment_domain,
            )
        ),
        y=alt.Y(
            "percent_with_1_dose:Q",
            title="Percentage of population with at least 1 dose of vaccine",
            scale=alt.Scale(
                domain=[0, 100])),
        color=alt.Color("party:N", scale=alt.Scale(domain=party_domain, range=party_range),
                        title="Party", legend=None)
    )

    final_chart=unemployment_vs_vaccine_plot.add_selection(month_selector).transform_filter(month_selector).configure_title(
        align="left",
        anchor="start"
    )
    #final_chart=(unemployment_vs_vaccine_plot + unemployment_vs_vaccine_plot.transform_regression(
    #    "unemployment_rate",
    #    "percent_with_1_dose"
    #).mark_line(
    #    color="black").encode(color=alt.value("black"))).add_selection(
    #    month_selector
    #).transform_filter(month_selector).configure_title(
    #    align="left",
    #    anchor="start"
    #)
    return final_chart