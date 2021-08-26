import pandas as pd
from pathlib import Path

from ETL.EtlElection import getStateLevelElectionData2020
from ETL.EtlCovid import (getRollingCaseAverageSegmentLevel,
                          getPercentilePointChageDeathsData)
from ETL.EtlVaccine import (getDailyVaccinationPercentData,
                            getStateVaccinationDataWithAPI)
from ETL.EtlMask import (createDataForMaskUsageDistribution,
                            createDataForFreqAndInFreqMaskUse)

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