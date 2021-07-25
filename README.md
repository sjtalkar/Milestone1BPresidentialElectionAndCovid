# Milestone1BPresidentialElectionAndCovid
Data and code for Presidential Election and Covid study

## Datasets

| Name | Description | Key Variables | Size | Shape | Format | Access |
|---|---|---|---|---|---|---|
| State presidential election results dataset | *"This data file contains constituency (state-level) returns for elections to the U.S. presidency from 1976 to 2020"* | `year, candidatevotes, totalvotes` | 500KB | 4287 x 15 | CSV | [Harvard Dataverse website](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/42MVDX) |
| County presidential election results dataset | *"This dataset contains county-level returns for presidential elections from 2000 to 2020"* | `year, county_fips, county_name, party` | 7.4MB | 72603 x 12 | CSV | [Harvard Dataverse website](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/VOQCHQ) |
| COVID-19 cases and death rolling averages|This dataset issued by the New York Times *"contains the daily number of new cases and deaths, the seven-day rolling average and the seven-day rolling average per 100,000 residents"* for all counties in the U.S. | `date, geoid, county, state, cases_avg_per_100k, deaths_avg_per_100k` | >85MB | >146M x 10 | CSV | [The New York Times GitHub page](https://github.com/nytimes/covid-19-data/tree/master/rolling-averages)|
| State level total COVID-19 vaccine dataset | This dataset issued by the US Centers for Disease Control and Prevention (CDC) contains the total COVID-19 Vaccine deliveries and administration data at the state level.| `State/Territory/Federal Entity, People with at least One Dose by State of Residence, Percent of Total Pop with at least One Dose by State of Residence` | 28KB | 63 x 62 | CSV | [The U.S. Centers for Disease Control website](https://covid.cdc.gov/covid-data-tracker/#vaccinations) |
| County level daily COVID-19 vaccine dataset | This dataset issued by the US Centers for Disease Control and Prevention (CDC) contains the daily COVID-19 Vaccine deliveries and administration data at the county level. | `date, location, mmwr_week, administered_dose1_recip, administered_dose1_pop_pct` | 4.4MB | >14,400 x 69 | CSV | [The U.S. Centers for Disease Control website](https://data.cdc.gov/Vaccinations/COVID-19-Vaccinations-in-the-United-States-Jurisdi/unsk-b7fc) |
| Mask-wearing survey dataset | This dataset is an estimate of mask usage by county in the United States released by The New York Times. It “comes from a large number of interviews conducted online“ in 2020 between July 2nd and July 14th. | `ever, rarely, sometimes, frequently, always` | 109KB | 3143 x 6 | CSV | [The New York Times GitHub page](https://github.com/nytimes/covid-19-data/tree/master/mask-use) |
| Census Bureau population census and estimates dataset | This dataset contains the 2010 population census data per county and the 2011~2020 population estimates. We are mainly interested in the 2020 estimates | `SUMLEV, STATE, STNAME, CTYNAME, POPESTIMATE2020` | 3.7MB | 3195 x 180 | CSV | [U.S. Census Bureau website](https://www.census.gov/programs-surveys/popest/technical-documentation/research/evaluation-estimates/2020-evaluation-estimates/2010s-counties-total.html) |
| Unemployment rate dataset | The dataset is the collection of labor force county data tables for 2020 issued by the U.S. Bureau of Labor Statistics | `State FIPS Code, County FIPS Code, county name, Period unemployment rate` | 2.68MB | 45066 x 9 | XLS | [Bureau of Labor Statistics website](https://www.bls.gov/web/metro/laucntycur14.zip) |
| Census Urban and Rural dataset | The dataset classifies all the counties in the U.S. as rural or urban areas |`2015 GEOID, State, 2015 Geography Name, 2010 Census Percent Rural`| 302KB | 3142 x 8 | XLS | [U.S. Census Bureau website](https://www.census.gov/programs-surveys/geography/guidance/geo-areas/urban-rural.html) |
|-|-| `` |-| x | CSV | [-]() |
|-|-| `` |-| x | CSV | [-]() |

## Further analysis thoughts

Vaccination is the best protection against Delta.“All viruses evolve over time and undergo changes as they spread and replicate,” 

>**Sources**
> - https://www.yalemedicine.org/news/5-things-to-know-delta-variant-covid
> -    https://covid.cdc.gov/covid-data-tracker/#variant-proportions
   
1. As per the articles above,  
`The first Delta case in the United States was diagnosed in March and it is now the dominant strain in the U.S.`
2. Delta is the name for the B.1.617.2. variant, a SARS-CoV-2 mutation that originally surfaced in India. 
The first Delta case was identified in December 2020, and the strain spread rapidly, 
soon becoming the dominant strain of the virus in both India and then Great Britain. 
3. Delta is spreading 50% faster than Alpha, which was 50% more contagious than the original strain of SARS-CoV-2, he says. “In a completely unmitigated environment—where no one is vaccinated or wearing masks—it’s estimated that the average person infected with the original coronavirus strain will infect 2.5 other people,” Dr. Wilson says. “In the same environment,
 Delta would spread from one person to maybe 3.5 or 4 other people.”
4. Create a choropleth of vaccinated and unvaccinated people.
In the U.S., there is a disproportionate number of unvaccinated people in Southern and Appalachian states including Alabama, 
Arkansas, Georgia, Mississippi, Missouri, and West Virginia, where vaccination rates are low (in some of these states, the number of cases 
is on the rise even as some other states are lifting restrictions because their cases are going down).
5. Link it to a bar chart of percentile points gained in last elections bar chart with red and blue bars

https://www.standardco.de/notes/heatmaps-vs-choropleths
Choropleths are thematic maps where a geographic region has a uniform color based on a metric. Colors correspond to categories defined by numeric ranges. These are often called heatmaps, but that isn't entirely accurate. The key difference between choropleth maps and heat maps is the shape of colored areas. In choropleths, shapes are defined by standard geographical boundaries, not by the data itself. A true geographic heatmap is an isopleth map (has data drawn shapes) 
that depict hotspots on a map to present concentrations of values.

> When describing or labeling data visualized on a map, consider whether the colored areas are defined by the data or if the shapes are predetermined to know if you should call it a heatmap or choropleth. If the data being presented has no connection to location at all, then you're probably dealing with a heatmap matrix. Calling it simply a heatmap is appropriate in that context.
