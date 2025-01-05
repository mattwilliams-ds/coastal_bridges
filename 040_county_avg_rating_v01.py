#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import pandas as pd
import numpy as np
import scipy.stats as stats
import statsmodels.api as sm


# In[2]:


# Set folder paths
input_path = 'output/time_series/'
output_path = 'output/county_groups/'


# In[3]:


# Functions
def get_files(directory, ext):
    # A function that returns a list of files in the specified directory
    files = [i for i in os.listdir(directory) if ext in i]
    return files

def get_slope(y_values, years):
    # A function that returns the slope of a line of best fit through the storm frequency data
    
    # create a dataframe of input data
    df_dict = {'year': years, 'y_values': y_values}
    df = pd.DataFrame(df_dict)

    df.dropna(subset = ['y_values'], inplace = True)
    
    # build a linear model from input data
    y = df.y_values
    X = df.year
    X = sm.add_constant(X)

    linear_model = sm.OLS(y, X).fit()

    # find the slope of the line of best fit
    m = linear_model.params.iloc[1]

    return m


# In[4]:


# Create list of files
list_of_files = get_files(input_path, '.csv')


# In[5]:


print('Calculating rate of bridge rating change by county...')

# Load delta series & count number of records
time_series = pd.read_csv(input_path+'rating_time_series.csv')
    
years_str = time_series.columns[6:]
years = list(map(int, years_str))

# calculate average bridge rating of current br_component by county
county_avg = time_series.groupby('ST_CNTY')[years_str].mean()

slope = []
for index, row in county_avg.iterrows():
   values = row.values.tolist()
   slope.append(get_slope(values, years))

county_avg.insert(0, 'BR_RATE', slope)

county_avg.to_csv(output_path+'avg_county_rating.csv')

print('Finished')

