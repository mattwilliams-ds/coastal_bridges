#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Import Libraries
import pandas as pd
import geopandas as gpd
from shapely import wkt
import mwlib as mw
import os


# In[2]:


def init_time_ser(folder, files):
    # Create time series dataframe

    # List of columns to have in final output
    cols = ['STATE_STR',
            'geometry',
            'ST_CNTY',
           ]

    # Initialize DataFrame
    time_df = pd.DataFrame(columns = ['STATE_STR'])

    for file in sorted(files, reverse = True):
        next_df = pd.read_csv(folder+file)
        next_df = next_df[cols].copy()
        time_df = pd.concat([time_df, next_df]).copy()
        time_df = time_df[~time_df.index.duplicated(keep='first')]
    return time_df

def get_years(directory, ext):
    list_of_years = [i[-8:-4] for i in os.listdir(directory) if ext in i]
    return list_of_years


# In[3]:


# Get list of files to process
input_directory = 'output/nbi_clean/'
list_of_files = mw.get_files(input_directory,'.csv')

# Directory to write output to
output_directory = 'output/time_series/'


# In[4]:


# Initialize time series with bridge ID and geometry data
time_ser_initial = init_time_ser(input_directory, list_of_files)

# Create a list of each year's worth of data in the output directory 
years = get_years(input_directory, '.csv')

structure_ages = pd.read_csv('output/structure_age/structure_ages.csv', index_col=False)


# In[5]:


print('Processing time series...')

attribute = 'MEAN_RATING'

# List of columns to keep
cols = ['STATE_STR', attribute]

# Initialize time_ser for the next attribute
time_ser = time_ser_initial.copy()
    
# Loop through each out19XX.csv/out20XX.csv
for file in sorted(list_of_files, reverse = False):
        
    # Get the year from the file name
    year = file[-8:-4]

    print('\t'+year)
    
    # read in the next file
    next_df = pd.read_csv(input_directory+file)

    # trim DataFrame down to desired columns
    next_df = next_df[cols].copy()

    # Rename current attribute column as current year
    next_df.rename(columns = {attribute:str(year)}, inplace=True)
        
    # Remove duplicate records, keeping the first one
    next_df = next_df[~next_df.index.duplicated(keep='first')]

    # Merge next_df into time series
    time_ser = pd.merge(time_ser, next_df, how = 'left', on = ['STATE_STR'])

    # Drop all records with 999 (NaN) values
    drop_bridges = time_ser[(time_ser[year] == 999)].index 
    time_ser.drop(drop_bridges, inplace = True)

# Remove duplicate entries
time_ser = time_ser[~time_ser.index.duplicated(keep='first')]

# Forward Fill missing data
time_ser[sorted(years)] = time_ser[sorted(years)].ffill(axis = 1).copy()

# Merge structure age & FC_ZONE
#time_ser = pd.merge(time_ser, structure_ages['AGE'],
#                    on = 'STATE_STR', how = 'left')

# Move ST_ZONE & AGE so that they show up before the time history columns
#time_ser.insert(3, 'FC_ZONE', time_ser.pop('FC_ZONE'))
#time_ser.insert(4, 'ZN_TYPE', time_ser.pop('ZN_TYPE'))
#time_ser.insert(5, 'AGE', time_ser.pop('AGE'))
    
# Write time series to a csv file
time_ser.to_csv(output_directory+'rating_time_series.csv', index=False)

print('Finished')

