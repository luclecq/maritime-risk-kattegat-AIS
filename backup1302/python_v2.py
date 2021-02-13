# -*- coding: utf-8 -*-

import pandas as pd
import os
import random

# random samples of three months before and three months after 1 July 2020

daterange_before = [d.strftime("%Y-%m-%d") for d in pd.date_range('2020-04-01','2020-06-30')]
daterange_after = [d.strftime("%Y-%m-%d") for d in pd.date_range('2020-07-01','2020-09-30')]

sample_days_before = sorted(random.sample(daterange_before, 20))
sample_days_after = sorted(random.sample(daterange_after, 20))

# preprocess daily AIS text files from DMA for import in postgis

def process_aisdk(file_input_path, bb_latmin, bb_latmax, bb_longmin, bb_longmax):
    with open(file_input_path) as file_input:
        df = pd.read_csv(file_input, usecols=['# Timestamp', 'Type of mobile', 'MMSI', 'Latitude', 'Longitude', 'Navigational status', 'Ship type'])
 
    df = df[((df['Latitude'] >= bb_latmin) & (df['Latitude'] <= bb_latmax)) & ((df['Longitude'] >= bb_longmin) & (df['Longitude'] <= bb_longmax))]
    df = df[~df['Navigational status'].isin(['Moored', 'At anchor', 'Aground'])]
    df = df[df['Type of mobile'].isin(['Class A', 'Class B'])]
    df = df[~df['Ship type'].isin(['Pilot', 'Tug', 'Towing', 'Towing long/wide'])]
    
    output_path = os.path.splitext(file_input_path)[0] + "_processed" + os.path.splitext(file_input_path)[1]   
    with open(output_path, 'w', newline='\n') as file_output:
        df.to_csv(file_output, columns = ['# Timestamp', 'MMSI', 'Latitude', 'Longitude'], index=False, header=False)

process_aisdk('D:/Thesis/Data/aisdk_20210121.csv', 57.2, 57.5, 11.2, 11.9)


### test

with open('D:/Thesis/Data/trajectories.csv') as file_input:
        df_20 = pd.read_csv(file_input, usecols=['mmsi'])
        

with open('D:/Thesis/Data/aisdk_20201021.csv') as file_input:
        df = pd.read_csv(file_input, usecols=['Type of mobile', 'MMSI', 'Callsign', 'Name', 'Ship type'])

df_202 = df_20.merge(df.dropna().drop_duplicates(),how='left',left_on='mmsi',right_on='MMSI')



