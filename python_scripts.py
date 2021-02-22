# -*- coding: utf-8 -*-

# --- Vessel traffic analysis ---
# determine ship type group with highest value per cell (ArcGIS)
from arcpy.sa import *
from arcpy.management import *

agg_max = ArgStatistics(rasters=[Raster(r"agg_cargo"),Raster(r"passenger"),Raster(r"fishing"),Raster(r"agg_other")],stat_type="max",multiple_occurrence_value=99)
CopyRaster(agg_max, r"agg_max")

# --- Maritime risk analysis ---
# random samples of three months before and three months after 1 July 2020
import pandas as pd
import random
sample_days_before = sorted(random.sample([d.strftime("%Y-%m-%d") for d in pd.date_range('2020-04-01','2020-06-30')], 20))
sample_days_after = sorted(random.sample([d.strftime("%Y-%m-%d") for d in pd.date_range('2020-07-01','2020-09-30')], 20))

# preprocess daily AIS text files from DMA for import in postgis
import os
import pandas as pd

def process_aisdk(file_input_path):
    with open(file_input_path) as file_input:
        df = pd.read_csv(file_input, usecols=['# Timestamp', 'Type of mobile', 'MMSI', 'Latitude', 'Longitude', 'Navigational status', 'ROT', 'SOG', 'COG', 'Ship type'])

        # bounding box filters
        df = df[((df['Latitude'] >= 57.3) & (df['Latitude'] <= 57.7)) & ((df['Longitude'] >= 10.7) & (df['Longitude'] <= 11.7))]
        df = df.loc[~((df['Latitude'] >= 57.6) & (df['Longitude'] >= 11.6))]
        df = df.loc[~((df['Latitude'] <= 57.35) & ((df['Longitude'] >= 10.8) & (df['Longitude'] <= 11.2)))]
        # exclude messages with these 'stationary' statuses
        df = df[~df['Navigational status'].isin(['Moored', 'At anchor', 'Aground'])]
        # only include messages from class A or class B equipment
        df = df[df['Type of mobile'].isin(['Class A', 'Class B'])]
        # exclude messages from vessels that are close to other vessels by definition
        df = df[~df['Ship type'].isin(['Pilot', 'Tug', 'Towing', 'Towing long/wide'])]

    output_path = os.path.splitext(file_input_path)[0] + "_processed" + os.path.splitext(file_input_path)[1]
    with open(output_path, 'w', newline='\n') as file_output:
        df.to_csv(file_output, columns = ['# Timestamp', 'MMSI', 'Latitude', 'Longitude', 'ROT', 'SOG', 'COG', 'Ship type'], index=False, header=False)