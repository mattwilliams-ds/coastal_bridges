#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import pandas as pd
import statsmodels.api as sm


# In[2]:


def get_coastal_areas(file, col):
    # A function that returns a list of coastal counties
    coastal = pd.read_csv(file, dtype=str)
    counties = coastal[col].unique().copy()
    return counties

def get_fips(file, areas):
    # A function for retrieving a dataframe describing the relationship between counties
    #  and forecast zones
    zones_counties = pd.read_csv(file, dtype=str, sep = '|', header = None)

    # drop unwanted columns
    keep_columns = [4, 6]
    zones_counties = zones_counties[keep_columns].copy()
    
    # Rename headers
    headers = ['ST_ZONE', 'CNTY_FIPS']
    zones_counties.columns = headers

    # Concatenate state abbreviation & county fips
    zones_counties['ST_CNTY'] =  zones_counties.ST_ZONE.str[:2] + zones_counties.CNTY_FIPS.str[-3:]

    # Drop all records not in a coastal county
    zones_counties = zones_counties[zones_counties.CNTY_FIPS.isin(areas)].copy()
    return zones_counties

def glm_slope(storms, years):
    # A function that returns the slope and p_value for a line of best fit to count
    #  data using the GLM Poisson regressn
    
    # create a dataframe of input data
    df_dict = {'year': years, 'storms': storms}
    df = pd.DataFrame(df_dict)
    
    # drop years with no events
    df.dropna(subset = ['storms'], inplace = True)
    
    # build a linear model from input data
    y = df.storms
    X = df.year
    X = sm.add_constant(X)

    glm = sm.GLM(y, X, family=sm.families.Poisson()).fit()
    return glm.params.year, glm.pvalues.year


# In[3]:


input_path = 'output/processed_weather/'
output_path = 'output/processed_weather/'


# In[4]:


print('Calculating storm events by county...')

# Read in county events
all_events = pd.read_csv(input_path+'total_counts.csv')

# List of events to disregard
drop_events = ['Rip Current', 'High Surf', 'Heat', 'Dense Fog', 'Sneakerwave',
               'Freezing Fog', 'Astronomical Low Tide', 'Drought']

# Create new attribute to hold county data for all records (including forecast zones)
all_events.insert(2, 'ST_CNTY', '')

# drop unwanted event types
for event in drop_events:
    all_events = all_events.drop(all_events[all_events.EVENT_TYPE == event].index)

# Create a list of years in the weather event history
years_str = all_events.columns[4:]
years = list(map(int, years_str))

# Create a list of coastal counties
coastal_areas = get_coastal_areas('input/Coastal_Counties.csv', 'countyfips')

# Create a dataframe with coastal counties & forecast zones
fczones = get_fips('input/bp05mr24.dbx', coastal_areas)

# Create dictionary  of forecast zones & counties
fc_dict = dict(zip(fczones['ST_ZONE'], fczones['ST_CNTY']))

# Map counties to forecast zones
all_events.ST_CNTY = all_events[all_events.CZ_TYPE == 'Z']['STATE_CZ'].map(fc_dict).copy()

# Create a mask for county CZ_TYPE
mask = all_events['CZ_TYPE'] == 'C'

# Set ST_CNTY equl to State CZ for all CZ_TYPE = C
all_events.loc[mask, 'ST_CNTY'] = all_events.loc[mask, 'STATE_CZ']

# Count number of weather events by county
total_cnty_events = all_events.groupby(['ST_CNTY'])[years_str].sum().reset_index()
total_cnty_events.replace(0, np.nan, inplace = True)

# get number of events per county
num = total_cnty_events.iloc[:,2:].count(axis=1)

# Create a dataframe of only records with 20 years or more of event history
hist = total_cnty_events.loc[total_cnty_events.iloc[:,2:].count(axis=1) >= 10].copy()
hist = hist.reset_index()

# For each county, calculate slope for line of best fit through event counts over time
slope = []
p_val = []
for index, row in hist.iterrows():
    values = row.values[2:].tolist()
    slope_i, p_val_i = glm_slope(values, years)
    slope.append(slope_i)
    p_val.append(p_val_i)

hist.insert(2, 'STORM_RATE', slope)
hist.insert(3, 'P_VAL', p_val)
hist.pop('index');

hist.to_csv(output_path+'cnty_storm_history.csv', index = False)
print('Finished')

