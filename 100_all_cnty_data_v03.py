#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np


# In[2]:


# Read all county data into memory
structure_age = pd.read_csv('output/structure_age/ages_by_county.csv')
weather_counts = pd.read_csv('output/processed_weather/cnty_storm_history.csv')
bridge_condition = pd.read_csv('output/county_groups/avg_county_rating.csv')
population = pd.read_csv('output/census/population_by_county.csv')

# Create a dataframe to store all county data
county_data = structure_age.copy()

# Rename columns to be more descriptive
new_names = {'MEAN_AGE': 'Mean Age', 'MED_AGE': 'Median Age'}
county_data.rename(columns = new_names, inplace = True)

bridge_condition = bridge_condition[['ST_CNTY', 'BR_RATE']].copy()
bridge_condition.rename(columns = {'BR_RATE': 'Bridge Rating'}, inplace = True)

weather_counts = weather_counts[['ST_CNTY', 'STORM_RATE', 'P_VAL']].copy()
weather_counts.rename(columns = {'STORM_RATE': 'Storm Frequency'}, inplace = True)

population = population[['ST_CNTY', 'RATIO']].copy()
population.rename(columns = {'RATIO': 'Population Ratio'}, inplace = True)

# Merge all county data into one dataframe
county_data = pd.merge(county_data,
                  bridge_condition,
                  left_on = 'ST_CNTY',
                  right_on = 'ST_CNTY',
                  how = 'left')

county_data = pd.merge(county_data,
                  weather_counts,
                  left_on = 'ST_CNTY',
                  right_on = 'ST_CNTY',
                  how = 'left')

county_data = pd.merge(county_data,
                  population,
                  left_on = 'ST_CNTY',
                  right_on = 'ST_CNTY',
                  how = 'left')


# In[3]:


# Filter 
county_data['SIG_STRM_F'] = county_data['Storm Frequency'].copy()
county_data.loc[county_data['P_VAL'] >= 0.05, 'SIG_STRM_F'] = np.nan

# Create flags
county_data['Aging Bridges'] = (county_data['Median Age'] > county_data['Mean Age']) * 1
county_data['Rating Decreasing'] = (county_data['Bridge Rating'] < 0.0)  * 1
county_data['Storms Increasing'] = (county_data['SIG_STRM_F'] > 0) * 1
county_data['Population Increasing'] = (county_data['Population Ratio'] > 0.0) * 1


# Create an attribute that sums the number of flags
county_data['SUM_FLAGS'] = county_data.iloc[:,8:].sum(axis = 1)

# Show counties with all four risk factors
county_data.loc[county_data['SUM_FLAGS'] == 4]

county_data.to_csv('output/all_county_data.csv', index = False)


# In[4]:


county_data.loc[county_data['SUM_FLAGS'] == 3]


# In[5]:


county_data.head(10)


# In[6]:


county_data.loc[county_data.ST_CNTY == 'AK016']


# # BONEYARD
