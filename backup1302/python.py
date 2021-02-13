# -*- coding: utf-8 -*-

import pandas as pd
import os

# preprocess daily AIS text files from DMA for import in postgis

def process_aisdk(file_input_path):
    with open(file_input_path) as file_input:
        df = pd.read_csv(file_input, usecols=['# Timestamp', 'MMSI', 'Latitude', 'Longitude'])
       
    output_path = os.path.splitext(file_input_path)[0] + "_processed" + os.path.splitext(file_input_path)[1]

    with open(output_path, 'w', newline='\n') as file_output:
        df.to_csv(file_output, index=False, header=False)

process_aisdk('D:/Thesis/Data/aisdk_20201021.csv')

###

with open('D:/Thesis/Data/trajectories.csv') as file_input:
        df_20 = pd.read_csv(file_input, usecols=['mmsi'])
        

with open('D:/Thesis/Data/aisdk_20201021.csv') as file_input:
        df = pd.read_csv(file_input, usecols=['Type of mobile', 'MMSI', 'Callsign', 'Name', 'Ship type'])

df_202 = df_20.merge(df.dropna().drop_duplicates(),how='left',left_on='mmsi',right_on='MMSI')



