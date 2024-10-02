import pandas as pd
import numpy as np
import pyspark.pandas as ps
import matplotlib.pyplot as plt
# import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("GPS Heatmap").getOrCreate()

# Create a list to collect DataFrames for all car IDs
all_data = []

# Loop through the IDs from 2 to 10354
for car_id in range(2, 1000):
    file_path = f'taxi_log_2008_by_id/{car_id}.csv'  # Adjust the path as necessary
    try:
        # Load CSV data using PySpark Pandas
        df = ps.read_csv(file_path, ',', "infer", ["id", "time_stamp", "longtitude", "latitude"])
        df['id'] = car_id  # Add the car ID to the DataFrame
        all_data.append(df)
    except Exception as e:
        print(f"Could not load file for car ID {car_id}: {e}")

# Concatenate all DataFrames
combined_df = ps.concat(all_data)

# Create bins for longitude and latitude
bin_size = 0.01  # Adjust the bin size as needed

# Create longitude and latitude bins
combined_df['longitude_bin'] = (combined_df['longtitude'] // bin_size) * bin_size
combined_df['latitude_bin'] = (combined_df['latitude'] // bin_size) * bin_size

# Group by the bins and count occurrences
aggregated_df = combined_df.groupby(['longitude_bin', 'latitude_bin']).size().reset_index(name='count')

# Collect the aggregated data
longitude_bins = aggregated_df['longitude_bin'].to_numpy()
latitude_bins = aggregated_df['latitude_bin'].to_numpy()
counts = aggregated_df['count'].to_numpy()

# Create a grid for the heatmap
heatmap_data = np.zeros((len(set(latitude_bins)), len(set(longitude_bins))))

# Map counts to the heatmap grid
for i in range(len(longitude_bins)):
    long_idx = list(set(longitude_bins)).index(longitude_bins[i])
    lat_idx = list(set(latitude_bins)).index(latitude_bins[i])
    heatmap_data[lat_idx, long_idx] = counts[i]

# Create the heatmap
plt.figure(figsize=(10, 8))
# sns.heatmap(heatmap_data, 
#             xticklabels=sorted(set(longitude_bins)), 
#             yticklabels=sorted(set(latitude_bins)), 
#             cmap='YlGnBu', 
#             cbar_kws={'label': 'Count'})

plt.title('Heatmap of GPS Locations for Cars 2 to 10354')
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.show()
