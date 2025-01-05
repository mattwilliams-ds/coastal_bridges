#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import mwlib as mw


# In[2]:


def get_years(files):
    years = []
    for file in sorted(files, reverse=False):
        years.append(file[6:10])
    return years      

def get_state_fips(file):
    # A function that takes a file input and returns a dictionary of state fips numbers
    #  and the state two letter abbreviation
    
    df = pd.read_csv(file)

    # Truncate dataframe down to just state abbreviations & fips numbers
    st_fips = df[['STATE', 'FIPS']].copy()

    # Convert fips numbers to strings and slice off first 1-2 characters (state fips)
    st_fips['FIPS'] = st_fips['FIPS'].apply(str).str.slice(start = 0, stop = -3).copy()

    # de-dup dataframe
    st_fips = st_fips[~st_fips.STATE.duplicated(keep = 'first')]

    # Convert to a dictionary & return
    #st_fips.set_index('FIPS', inplace = True)
    
    return dict(zip(st_fips['FIPS'], st_fips['STATE']))

def get_coastal_locations(path):
    # A function that returns a list of coastal states & counties
    coastal = pd.read_csv(path+'Coastal_Counties.csv', dtype=str)
    states = coastal['stateusps'].unique().copy()
    counties = coastal['countyfips'].unique().copy()
    return states, counties

def pop_change(df, pop_attribute):
    # List of columns to keep in second year file
    keep_cols = ['STATE', 'COUNTY', pop_attribute]
    df = df[keep_cols].copy()

    # Convert county fips to strings & fill with 0s to 3 characters
    df['COUNTY'] = df['COUNTY'].apply(str)
    df['COUNTY'] = df['COUNTY'].apply(lambda x: x.zfill(3))

    # Convert state fips to strings
    df['STATE'] = df['STATE'].apply(str)

    df['STATE_ABV'] = df['STATE'].map(state_fips)

    # Fill state fips to two digits
    df['STATE'] = df['STATE'].apply(lambda x: x.zfill(2))

    # Create state fips + county fips attribute
    df['CNTY_FIPS'] = df['STATE'] + df['COUNTY']

    # Drop counties not in coastal_counties
    df = df[df.CNTY_FIPS.isin(coastal_counties)].copy()

    # Create a new attribute of state abbreviation concatenated with county fips code
    df['ST_CNTY'] = df['STATE_ABV'] + df['COUNTY']
    return df


# In[3]:


input_path = 'input/census/'
output_path = 'output/census/'

# Get a list of files in the input path
list_of_files = mw.get_files(input_path, '.csv')

# Read in census data
first_year = pd.read_csv(input_path+sorted(list_of_files, reverse = False)[0], encoding='unicode_escape')
second_year = pd.read_csv(input_path+sorted(list_of_files, reverse = False)[1], encoding='unicode_escape')

# Create a dataframe with state abbreviations & state fips numbers
state_fips = get_state_fips('input/state_zone.csv')

# Get a list of coastal counties

coastal_states, coastal_counties = get_coastal_locations('input/')


# In[4]:


# Process first & second year data
first_year = pop_change(first_year, 'POPESTIMATE2011')
second_year = pop_change(second_year, 'POPESTIMATE2021')


# In[5]:


# Merge datasets into a single dataframe
population_df = pd.merge(second_year,
                  first_year[['POPESTIMATE2011', 'ST_CNTY']],
                  left_on = 'ST_CNTY',
                  right_on = 'ST_CNTY',
                  how = 'left')

# Calculate population change from two datasets
population_df['POP_CHANGE'] = population_df['POPESTIMATE2021'] - population_df['POPESTIMATE2011']

# Calculate ratio of population change
population_df['RATIO'] = population_df['POP_CHANGE'].divide(population_df['POPESTIMATE2021'])

# Reorganize dataframe
population_df.insert(0, 'ST_CNTY', population_df.pop('ST_CNTY'))
population_df.pop('STATE');
population_df.pop('COUNTY');
population_df.pop('STATE_ABV');
population_df.pop('CNTY_FIPS');

# Write dataframe to file
population_df.to_csv(output_path+'population_by_county.csv', index=False)

