#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Import Libraries
import os
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely import wkt


# In[2]:


# Functions for reading & cleaning inventory files
def get_files(directory, ext):
    # A function that returns a list of files in the specified directory
    files = [i for i in os.listdir(directory) if ext in i]
    return files

def init_time_ser(folder, files):
    # Create time series dataframe

    # List of columns to have in final output
    cols = ['STATE_STR',
            'geometry',
            'ST_CNTY'
           ]

    # Initialize DataFrame
    time_df = pd.DataFrame(columns = ['STATE_STR'])

    for file in sorted(files, reverse = True):
        next_df = pd.read_csv(folder+file)
        next_df = next_df[cols].copy()
        time_df = pd.concat([time_df, next_df]).copy()
        time_df = time_df[~time_df.index.duplicated(keep='first')]
    return time_df


# In[3]:


# Get list of files to process
input_directory = 'output/nbi_clean/'
list_of_files = get_files(input_directory,'.csv')

# Directory to write output to
output_directory = 'output/structure_age/'

# Directory to write GIS Shape files
gis_directory = 'output/shape_files/'


# In[4]:


# Initialize time series with bridge ID and geometry data
time_ser_initial = init_time_ser(input_directory, list_of_files)


# In[5]:


print('Working on:')

# Initialize time_ser for the next attribute
time_ser = time_ser_initial.copy()

# Loop through each bridge rating attribute
for file in sorted(list_of_files, reverse = False):

    # Get the year from the file name
    year = file[-8:-4]

    print('\t'+year)

    cols = ['STATE_STR', 'YEAR_BUILT_027']
    
    # read in the next file
    next_df = pd.read_csv(input_directory+file)

    # trim DataFrame down to desired columns
    next_df = next_df[cols].copy()

    # Rename current attribute column as current year
    next_df.rename(columns = {'YEAR_BUILT_027':str(year)}, inplace=True)
    
    # Merge next_df into time series
    time_ser = time_ser.merge(next_df, on=['STATE_STR'], how = 'left').copy()

time_ser['MIN_YR_BUILT'] = time_ser.iloc[:,3:].min(axis=1)
time_ser['AGE'] = 2024-time_ser.MIN_YR_BUILT

# Prepare data for exporting to shape file
keep_cols = ['STATE_STR',
             'geometry',
             'AGE',
             'ST_CNTY'
            ]
time_ser = time_ser[keep_cols].copy()

# Calculate average & median structure ages by county
avg_ages_county    = time_ser.groupby(['ST_CNTY'])['AGE'].mean()
avg_ages_county = pd.DataFrame(avg_ages_county).reset_index()
avg_ages_county.rename(columns = {'AGE':'MEAN_AGE'}, inplace=True)

median_ages_county = time_ser.groupby(['ST_CNTY'])['AGE'].median()
median_ages_county = pd.DataFrame(median_ages_county).reset_index()
median_ages_county.rename(columns = {'AGE':'MED_AGE'}, inplace=True)

age_stats = pd.merge(avg_ages_county, median_ages_county, on='ST_CNTY')

# Covert string type geometry column back into geometry type fro GeoPandas
time_ser['geometry'] = time_ser['geometry'].apply(wkt.loads)

# Convert dataframe to a GeoPandas dataframe
time_ser = gpd.GeoDataFrame(time_ser, crs='epsg:4326')

print('\twriting shapefile & csv output')

# Export df to a GeoPandas shapefile
time_ser.to_file(gis_directory+'bridge_ages.shp')

# Write time series to a csv file
time_ser.to_csv(output_directory+'structure_ages.csv', index=False)

# Write mean & median ages to csv
age_stats.to_csv(output_directory+'ages_by_county.csv', index=False)
#median_ages_county.to_csv(output_directory+'median_county_ages.csv', index=True)

print('Finished')

