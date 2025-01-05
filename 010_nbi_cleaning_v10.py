#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Import Libraries
import os
import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import math as m


# In[2]:


# Functions for reading & cleaning inventory files
def get_files(directory, ext):
    # A function that returns a list of files in the specified directory
    files = [i for i in os.listdir(directory) if ext in i]
    return files

def get_directories(path):
    directories = []
    for root, dirs, files in os.walk(path):
        for name in dirs:
            if name[0] != '.':
                directories.append(name)
    return directories

def get_coastal_locations(path):
    # A function that returns a list of coastal states & counties
    coastal = pd.read_csv(path+'Coastal_Counties.csv', dtype=str)
    states = coastal['stateusps'].unique().copy()
    counties = coastal['countyfips'].unique().copy()
    return states, counties

def get_state_year(file):
    # This function discerns the state and year from the input file name
    state = file[0:2]
    year = file[2:4]
    return state, year

def log_progress(step, state, year):
    # A function for recording the successful completion of a step
    # used to troubleshoot code & locate point of crash
    with open('output/logs/log_file.csv', 'a') as file:
        output = step + ',' + state + ',' + year + '\n'
        file.write(output)
    return

def str_to_decimal_degrees(value):
    # A function that converts string type Lat & Long into decimal form
    degrees = value.str[:-6].astype('int64')
    minutes = value.str[-6:-4].astype('int64')
    seconds = value.str[-4:].astype('int64')
    decimal_degrees = degrees + (minutes + (seconds/100)/60)/60
    return decimal_degrees

def dms(value):
    # A function used to convert degrees, minutes, & seconds to decimaldegrees
    degrees = np.floor(value/1e6)
    minutes = np.floor((value - degrees*1e6)/1e4)
    seconds = (value - degrees*1e6 - minutes*1e4)/100
    decimal_degrees = degrees + (minutes + seconds/60)/60
    return decimal_degrees

def geometric_mean(df, rating_cols):
    # Calculate geometric mean & return df
    df['MEAN_RATING'] = df[rating_cols].product(axis=1) **  (1/(df[rating_cols].isnull().sum(axis=1) - 6)*-1)
    return df

def fix_early_nc(df):
    # A function for updating old NC NBI structure numbers to the new format used
    #  from the year 2000 and onwards
    
    # Copy structure numbers and strip leading zeros
    df.STRUCTURE_NUMBER_008 = df.STRUCTURE_NUMBER_008.str.lstrip('0').copy()

    # create a series with structure numbers less than 6 characters long
    old_rec = df.STRUCTURE_NUMBER_008[df.STRUCTURE_NUMBER_008.str.len() <= 6]

    # save left 2 to 3 digits and right 3 digits separately
    left = old_rec.str[:-3]
    right = old_rec.str[-3:]

    # new record is the concatenation of left and right with an additional zero in between
    new_rec = left + '0' + right

    # fill new structure number out to 15 digits & convert to a dictionary
    new_rec = new_rec.str.zfill(15)
    rec_dict = dict(zip(old_rec, new_rec))

    # create a mask with true for each original structure number that is in the dictionary
    mask = df['STRUCTURE_NUMBER_008'].isin(rec_dict.keys())

    # update structure numbers using the mask
    df.loc[mask, 'STRUCTURE_NUMBER_008'] = df.loc[mask, 'STRUCTURE_NUMBER_008'].map(rec_dict)
    return df

def clean_nbi(file_name, columns_to_keep, ratings):
    # A funciton used to clean out records with missing location data &/or bridge ratings
    
    # Read inventory file into DataFrame
    try:
        nbi = pd.read_csv(file_name, dtype=str, skipinitialspace=True, on_bad_lines='skip')
    except:
        nbi = pd.read_csv(file_name, dtype=str, skipinitialspace=True, encoding='unicode_escape',
                          on_bad_lines='skip')
    log_progress('clean_read', state, year)
    
    # Create an attribute with state and county codes concatenated
    nbi['STATE_COUNTY'] = nbi['STATE_CODE_001'] + nbi['COUNTY_CODE_003']
    log_progress('clean_state_county', state, year)
    
    # Drop bridges not located in coastal counties
    nbi = nbi[nbi.STATE_COUNTY.isin(coastal_counties)].copy()
    log_progress('clean_drop state_county', state, year)
    
    # Drop unwanted attributes
    nbi = nbi[columns_to_keep].copy()
    log_progress('clean_drop_cols', state, year)
    
    # Drop bridges with COUNTY_CODE_003 == nan
    nbi.dropna(subset=['COUNTY_CODE_003'], inplace = True)
    log_progress('clean_county_code_drop', state, year)
    
    # Drop Lat & Long NaN values
    nbi = nbi[nbi['LAT_016'].str.contains('-') == False].copy()
    nbi = nbi[nbi['LONG_017'].str.contains('-') == False].copy()
    nbi = nbi[nbi['LAT_016'].str.contains(r'\.') == False].copy()
    nbi = nbi[nbi['LONG_017'].str.contains(r'\.') == False].copy()
    nbi.dropna(subset=['LAT_016', 'LONG_017'], inplace=True)
    log_progress('clean_latlong_nan_drop', state, year)

    # Append state name
    nbi['STATE'] = state
    log_progress('clean_add_state_col', 'nbi', year)

    # Combine state & county
    nbi['COUNTY_CODE_003'] = nbi['COUNTY_CODE_003'].apply(int).apply(str)
    nbi['COUNTY_CODE_003'] = nbi['COUNTY_CODE_003'].apply(lambda x: x.zfill(3))
    nbi['ST_CNTY'] = nbi['STATE'] + nbi['COUNTY_CODE_003']
    
    # Convert Lat & Long to integers
    nbi['LAT_016'] = nbi['LAT_016'].astype('int64').copy()
    nbi['LONG_017'] = nbi['LONG_017'].astype('int64').copy()
    nbi = nbi[nbi['LONG_017'] > 999999].copy()
    nbi = nbi[nbi['LAT_016'] > 999999].copy()
    log_progress('clean_drop_bad_lat_long', state, year)
    
    # Drop records with missing ratings
    nbi[ratings] = nbi[ratings].replace('', np.nan).copy()
    nbi.dropna(subset=ratings, inplace=True)
    log_progress('clean_drop_missing_ratings', state, year)
    
    return nbi

def process_nbi(nbi_to_process, ratings):
    # A function used to create new attributes, convert Lat & Long to decimal values,
    #   and to convert the pandas DataFrame input into a geopandas DataFrame

    # Convert structure ratings of N, T, & U to 999 so they can be ignored by the min function
    nbi_to_process[ratings] = nbi_to_process[ratings].replace('N', '999').copy()
    nbi_to_process[ratings] = nbi_to_process[ratings].replace('n', '999').copy()
    nbi_to_process[ratings] = nbi_to_process[ratings].replace('T', '999').copy()
    nbi_to_process[ratings] = nbi_to_process[ratings].replace('U', '999').copy()
    log_progress('proc_nan_replace', 'nbi', year)
    
    # Convert ratings to int type
    nbi_to_process[ratings] = nbi_to_process[ratings].astype('int64')
    log_progress('proc_ratings_to_int', 'nbi', year)

    # Convert 999 to np.nan
    nbi_to_process[ratings] = nbi_to_process[ratings].replace(999, np.nan).copy()

    # Count number of non-null values
    nbi_to_process['NUM_RATINGS'] = (nbi_to_process[ratings].isnull().sum(axis=1) - 6)*-1

    # Calculate geometric mean prodct(n_ratings)^(1/n)
    #nbi_to_process['MEAN_RATING'] = nbi_to_process[ratings].product(axis=1) **  (1/(nbi_to_process[ratings].isnull().sum(axis=1) - 6)*-1)
    nbi_to_process = geometric_mean(nbi_to_process, ratings)
    
    # Calculate LOWEST_RATING attribute as culvert condition on minimum bridge condition
    nbi_to_process['LOWEST_RATING'] = nbi_to_process[ratings].min(axis=1)
    log_progress('proc_min_lowest_rating_axis=1', 'nbi', year)
    nbi_to_process = nbi_to_process[nbi_to_process.LOWEST_RATING <= 9].copy()
    log_progress('proc_min_lowest_rating<=9', 'nbi', year)

    # Create Lat & Long in decimal format
    nbi_to_process['LAT_DEC'] = dms(nbi_to_process['LAT_016'])
    nbi_to_process['LONG_DEC'] = dms(nbi_to_process['LONG_017'])
    nbi_to_process['LONG_DEC'] = nbi_to_process['LONG_DEC']*(-1)
    log_progress('proc_str_to_dec_LAT', 'nbi', year)

    # Create a unique identifier for each bridge (assumes structure number may not
    #   be unique from state to state)
    nbi_to_process['STATE_STR'] = nbi_to_process['STATE_CODE_001'] + nbi_to_process['STRUCTURE_NUMBER_008']
    log_progress('proc_state_struct', 'nbi', year)

    # Convert nbi_time_series to geopandas DataFrame
    processed_nbi = gpd.GeoDataFrame(nbi_to_process, geometry = gpd.points_from_xy(
        nbi_to_process.LONG_DEC, nbi_to_process.LAT_DEC, crs='epsg:4326'))
    log_progress('proc_gpd', 'nbi', year)
    return processed_nbi


# In[3]:


# List all attributes needed for the analysis
keep_columns = ['STATE_CODE_001',           #0
                'STRUCTURE_NUMBER_008',     #1
                'COUNTY_CODE_003',          #2
                'LAT_016', 'LONG_017',      #3
                'YEAR_BUILT_027',           #4
                'DECK_COND_058',            #5
                'SUPERSTRUCTURE_COND_059',  #6
                'SUBSTRUCTURE_COND_060',    #7
                'CHANNEL_COND_061',         #8
                'CULVERT_COND_062',         #9
                'SCOUR_CRITICAL_113'        #10
                ]

# Make a list of columns that contain structural ratings
rating_cols = ['DECK_COND_058', 'SUPERSTRUCTURE_COND_059', 'SUBSTRUCTURE_COND_060',
               'CHANNEL_COND_061', 'CULVERT_COND_062', 'SCOUR_CRITICAL_113']


# In[4]:


# AGGREGATE RELEVANT BRIDGES INTO A SINGLE DATAFRAME

# Input & output paths
input_path = 'input/nbi_files/'
output_path = 'output/nbi_clean/'

# Retrieve lists of coastal states and coastal counties
coastal_states, coastal_counties = get_coastal_locations('input/')

# Get a list of directories
list_of_dirs = get_directories(input_path)

# Status
print('Working on:')

for directory in sorted(list_of_dirs):
    # Create an empty DataFrame to store aggregate inventory data
    nbi_time_series = pd.DataFrame(columns = keep_columns)

    # Print current year
    print('\t'+directory[:4])
    
    # Get a list of files in the current directory to process
    list_of_files = get_files(input_path+directory, '.txt')

    # Loop through all state NBI files in directory
    for current_file in sorted(list_of_files):

        # Determine state & year for the current file
        state, year = get_state_year(current_file)

        # If the current file is a coastal state, aggregate it
        if state in coastal_states:
            log_progress('start_state_loop', 'next_nbi', year)
            # Clean current file
            next_nbi = clean_nbi(input_path+directory+'/'+current_file, keep_columns, rating_cols)
            log_progress('clean_nbi', 'next_nbi', year)

            if state == 'NC' and int(year) >= 25:
                # call fix_early_nc
                next_nbi = fix_early_nc(next_nbi)
            
            # Append current data to nbi_time_series dataframe
            nbi_time_series = pd.concat([nbi_time_series, next_nbi])
            log_progress('concat', 'next_nbi', year)
    
    # Process nbi_time_series
    nbi_time_series = process_nbi(nbi_time_series, rating_cols)

    # WRITE DATAFRAME TO CSV
    nbi_time_series.to_csv(output_path+'out' + directory[:4] + '.csv', index=False)

print('Finished')    

