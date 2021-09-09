import pandas as pd
from pathlib import Path

from ETL.EtlElection import getStateLevelElectionData2020
from ETL.EtlCovid import (getRollingCaseAverageSegmentLevel,
                          getPercentilePointChageDeathsData)
from ETL.EtlVaccine import (getDailyVaccinationPercentData,
                            getStateVaccinationDataWithAPI)
from ETL.EtlMask import (createDataForMaskUsageDistribution,
                            createDataForFreqAndInFreqMaskUse)
from ETL.EtlUnemployment import (getUnemploymentRateSince122019,
                                getUnemploymentCovidBase,
                                getUnemploymentCovidCorrelationPerMonth,
                                getJuly2020UnemploymentAndMask,
                                getUnemploymentVaccineCorrelationPerMonth)
from ETL.EtlUrbanRural import (MergeElectionUrbanRural)
#
# This script runs all the functions used to processed all the datasets used in the different visualizaionts
# It then saves all those processed datasets in files to be used in Streamlit. This is done to speed-up the loading time
# of the streamlit page by avoiding processing the data
#

if __name__ == '__main__':

    Path("./streamlit_data").mkdir(exist_ok=True)

    case_rolling_df = getRollingCaseAverageSegmentLevel()
    case_rolling_df.to_csv(path_or_buf ="./streamlit_data/case_rolling_df.csv", index=False)

    election_change_and_covid_death_df = getPercentilePointChageDeathsData()
    election_change_and_covid_death_df.to_csv(path_or_buf ="./streamlit_data/election_change_and_covid_death_df.csv",
                                              index=False)

    daily_vaccination_percent_df = getDailyVaccinationPercentData()
    daily_vaccination_percent_df.to_csv(path_or_buf ="./streamlit_data/daily_vaccination_percent_df.csv",
                                        index=False)

    state_vaccine_df, us_case_rolling_df, state_case_rolling_df = getStateVaccinationDataWithAPI()
    state_vaccine_df.to_csv(path_or_buf ="./streamlit_data/state_vaccine_df.csv", index=False)
    us_case_rolling_df.to_csv(path_or_buf ="./streamlit_data/us_case_rolling_df.csv", index=False)
    state_case_rolling_df.to_csv(path_or_buf ="./streamlit_data/state_case_rolling_df.csv", index=False)

    state_election_df = getStateLevelElectionData2020()
    state_election_df.to_csv(path_or_buf ="./streamlit_data/state_election_df.csv", index=False)

    mask_distribution_df = createDataForMaskUsageDistribution()
    mask_distribution_df.to_csv(path_or_buf ="./streamlit_data/mask_distribution_df.csv", index=False)

    county_pop_mask_df, county_pop_mask_freq_df, county_pop_mask_infreq_df = createDataForFreqAndInFreqMaskUse()
    county_pop_mask_df.to_csv(path_or_buf ="./streamlit_data/county_pop_mask_df.csv", index=False)
    county_pop_mask_freq_df.to_csv(path_or_buf ="./streamlit_data/county_pop_mask_freq_df.csv", index=False)
    county_pop_mask_infreq_df.to_csv(path_or_buf ="./streamlit_data/county_pop_mask_infreq_df.csv", index=False)

    #
    # Package unemployments dataframse
    #

    unemployment_rate_since_2019_df = getUnemploymentRateSince122019()
    unemployment_rate_since_2019_df.to_csv(path_or_buf="./streamlit_data/unemployment_rate_since_2019_df.csv",
                                           index=False)

    unemployment_covid_df = getUnemploymentCovidBase()
    unemployment_covid_df.to_csv(path_or_buf="./streamlit_data/unemployment_covid_df.csv", index=False)

    unemployment_covid_correlation_df = getUnemploymentCovidCorrelationPerMonth(unemployment_covid_df)
    unemployment_covid_correlation_df.to_csv(path_or_buf="./streamlit_data/unemployment_covid_correlation_df.csv",
                                             index=False)

    unemployment_freq_mask_july_df, unemployment_infreq_mask_july_df = getJuly2020UnemploymentAndMask(
        unemployment_covid_df)
    unemployment_freq_mask_july_df.to_csv(path_or_buf="./streamlit_data/unemployment_freq_mask_july_df.csv",
                                          index=False)
    unemployment_infreq_mask_july_df.to_csv(path_or_buf="./streamlit_data/unemployment_infreq_mask_july_df.csv",
                                          index=False)

    unemployment_vaccine_correlation_df = getUnemploymentVaccineCorrelationPerMonth(df=unemployment_rate_since_2019_df)
    unemployment_vaccine_correlation_df.to_csv(path_or_buf="./streamlit_data/unemployment_vaccine_correlation_df.csv",
                                               index=False)

    #
    # Package urban/rural dataframse
    #
    election_urban_rural_df = MergeElectionUrbanRural()
    election_urban_rural_df.to_csv(path_or_buf="./streamlit_data/election_urban_rural_df.csv",
                                               index=False)