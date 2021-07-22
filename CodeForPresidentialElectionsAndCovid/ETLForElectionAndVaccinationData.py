import pandas as pd
import numpy as np
import altair as alt
from vega_datasets import data
from pathlib import Path


##########################################################################################
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





from vega_datasets import data


def plotStateVaccinePct(df, date_in):

    #source = df[df['Date']==date_in].copy()
    source = df.copy()
    us_states = alt.topo_feature(data.us_10m.url, 'states')
    capitals = data.us_state_capitals.url
    
    
    # Create Slider
    min_day_num = source.day_num.min()
    max_day_num = source.day_num.max()
    slider = alt.binding_range(min=min_day_num, max=max_day_num, step=1, name = "Number of days since first vaccination: ")
    slider_selection = alt.selection_single(fields=['day_num'], bind=slider, name="day_num", init={'day_num':max_day_num})
                
    click = alt.selection_multi(fields=['recip_state']) 
   
    chart = alt.Chart(us_states).mark_geoshape().encode(
        color = alt.Color(
            'Percent with one dose:Q',
            scale=alt.Scale(scheme='yelloworangebrown')
    ),
      tooltip=[alt.Tooltip('STNAME:N',  title='State name: '),
              alt.Tooltip('Percent with one dose:Q', title='Population Pct with one shot : ')]
    ).transform_lookup(
        lookup='id',
        from_=alt.LookupData(source, 'STATEFP', ['Percent with one dose', 'STATEFP',  'STNAME' ])
    ).add_selection(slider_selection   ## Make sure you have added the selection here
    ).project(
        type='albersUsa'
    ).properties(
        width=850,
        height=500
    )
       
    
    capitals_base = alt.Chart(state_vaccine_df).encode(longitude="lon:Q", latitude="lat:Q")
    name_text = capitals_base.mark_text( 
                                    align='center',
                                    fontSize=10
                                   # fontWeight='light'
                                 ).encode(
                                    alt.Text('vacc_rank'),
                                    
                                )


 
                
    return  chart + name_text
