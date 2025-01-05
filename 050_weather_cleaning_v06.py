#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import os
import mwlib as mw


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

def get_state_codes(file):
    # A function that builds a dictionary of state abbreviations & fips codes
    county_data = pd.read_csv(file)
    codes = county_data[['statefips','stateusps']]
    codes = codes.drop_duplicates(subset=['statefips'], keep='first')
    codes['statefips'] = codes['statefips'].astype(str)
    codes = codes.set_index('statefips').to_dict()['stateusps']
    return codes

def cz_to_fz(stormdf, zones):
    # This function takes county storm data and replaces STATE_CZ value
    #  witih the forecast zone number
    
    # Create an empty copy of the storm dataframe
    c_to_z = pd.DataFrame(columns = stormdf.columns)

    # Create a new, empty attribute
    c_to_z['STATE_FZ'] = ''
    stormdf['STATE_FZ'] = ''

    storm_counties = stormdf.STATE_CZ.unique()

    for c in storm_counties:
        # Create a list of forecast zones in each coastal county
        fc_zones = zones.ST_ZONE[zones.ST_CNTY == c].to_list()
        
        for z in fc_zones:
            # For each zone in a county, copy the storm data
            tempdf = stormdf[stormdf.STATE_CZ == c].copy()

            # Populate forecast zone attribute
            tempdf.STATE_FZ = z

            # Concatenate current zone data with full dataframe
            c_to_z = pd.concat([c_to_z, tempdf], ignore_index=True)

    # Remove state-county attribute
    c_to_z.drop('STATE_CZ', axis=1, inplace=True)

    # Rename column
    c_to_z.rename(columns = {'STATE_FZ': 'STATE_CZ'}, inplace=True)
    return c_to_z

def clean_weather(c_z_type, keep_columns, zones):

    # Initialize a DataFrame to append event data to
    counts = pd.DataFrame(columns = ['STATE_CZ', 'EVENT_TYPE', 'CZ_TYPE'])
    counts = counts.set_index(['STATE_CZ', 'EVENT_TYPE'])

    # Get a list of state fips numbers
    state_fips = get_state_codes('input/Coastal_Counties.csv')

    # get a list of input files to process
    list_of_files = mw.get_files(input_folder, '.csv')

    # Print initial status message depending on c_z_type, test for unrecognized types
    if c_z_type == 'C':
        print('Working on counties for:')
    elif c_z_type == 'Z':
        print('Working on forecast zones for:')
    else:
        print('fips type not recognized')
        return

    for file in sorted(list_of_files):

        year = file[30:34]

        print('\t'+year)
    
        # Read current file into DataFrame
        storms = pd.read_csv(input_folder+file, dtype=str)

        # Reduce storms dataframe down to only the columns of interest
        storms = storms[keep_columns].copy()
    
        # Remove storms not in a county/parish
        storms = storms[storms['CZ_TYPE'] == c_z_type]

        # Replace fips for PR, GU, & VI to match NBI
        storms['STATE_FIPS'] = storms['STATE_FIPS'].replace('98', '66')
        storms['STATE_FIPS'] = storms['STATE_FIPS'].replace('96', '78')
        storms['STATE_FIPS'] = storms['STATE_FIPS'].replace('99', '72')
    
        # Convert county fips to 3 character strings (filling w/ zeroes where necessary)
        storms['STATE_FIPS2'] = storms['STATE_FIPS'].apply(lambda x: x.zfill(2))
        storms['CZ_FIPS'] = storms['CZ_FIPS'].apply(lambda x: x.zfill(3))

        # Concatenate state fips & county fips
        storms['ST_CZ'] = storms['STATE_FIPS2'] + storms['CZ_FIPS']

        # Add state abbreviations to dataframe
        storms['STATE'] = storms['STATE_FIPS'].copy()

        # Replace fips codes with state abbreviations
        storms.replace({'STATE':state_fips}, inplace=True)
    
        # Concatenate state abbreviation & county fips to use as a key with bridge condition data
        storms['STATE_CZ'] = storms['STATE'] + storms['CZ_FIPS']

        # Drop all records not in a coastal area
        if c_z_type == 'C':
            storms = storms[storms.STATE_CZ.isin(zones.ST_CNTY)].copy()
            
            # Convert counties to forecast zones
            storms = cz_to_fz(storms, zones)
        elif c_z_type == 'Z':
            storms = storms[storms.STATE_CZ.isin(zones.ST_ZONE)].copy()
        else:
            print('fips type not recognized')
            break
    
        new_counts = pd.DataFrame(storms.groupby(['STATE_CZ','EVENT_TYPE']).size())
        new_counts.rename(columns={0:year}, inplace=True)

        # Merge in new counts
        counts = counts.merge(new_counts, on=['STATE_CZ', 'EVENT_TYPE'], how = 'right').copy()

    counts['CZ_TYPE'] = c_z_type
    print('Finished')
    return counts, storms


# In[3]:


# Set folder paths for input & output
input_folder = 'input/noaa_data/'
output_folder = 'output/processed_weather/'

# Create a list of coastal counties
coastal_areas = get_coastal_areas('input/Coastal_Counties.csv', 'countyfips')

# Create a dataframe with coastal counties & forecast zones
fczones = get_fips('input/bp05mr24.dbx', coastal_areas)

# List all attributes needed for the analysis
keep_columns = ['EVENT_ID',                  #1
                'STATE_FIPS',                #2
                'EVENT_TYPE',                #3
                'CZ_TYPE',                   #4
                'CZ_FIPS'                    #5
                ]

# Count extreme events by county & project onto forecast zones
county_counts, cstorms = clean_weather('C', keep_columns, fczones)
#county_counts.to_csv(output_folder+'county_event_counts.csv', index=True)

# Count extreme events by NWS forecast zone
zone_counts, zstorms = clean_weather('Z', keep_columns, fczones)
#zone_counts.to_csv(output_folder+'zone_event_counts.csv', index=True)

#marine_counts, mstorms = clean_weather('M', keep_columns, fczones)

total_counts = pd.concat([county_counts, zone_counts])
total_counts.to_csv(output_folder+'total_counts.csv', index=True)


# # BONEYARD
