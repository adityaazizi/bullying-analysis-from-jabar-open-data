
# # Import Dependencies
from google.colab import drive
import os
import warnings
import requests
import pandas_gbq
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from geopy.geocoders import Nominatim

warnings.filterwarnings("ignore")
drive.mount('/content/gdrive')

# # Scrap Data from Endpoint
dir = '/content/gdrive/MyDrive/Capstone Project/Dataset/'
api_url = "https://data.jabarprov.go.id/api-backend/bigdata/dp3akb/od_15436_jml_korban_kekerasan__tingkat_pendidikan_jk_kabupatenk"
skip = 0
limit = 100
total_records = 2214
# those value above is based on metadata

all_data = []

while skip < total_records:
    response = requests.get(api_url, params={'skip': skip, 'limit': limit})

    if response.status_code == 200:
        data = response.json()
        all_data.extend(data['data'])
        skip += limit

    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")
        break

df = pd.DataFrame(all_data)
df

# # Exploratory Data Analysis
df.isna().sum()
df.info()
# delete unused columns
delete_columns = [
    'id', 'kode_provinsi', 'nama_provinsi', 'kode_kabupaten_kota', 'satuan'
]

df.drop(columns=delete_columns, inplace=True)

# delete data with missing values
df.dropna(axis=0, inplace=True)
df
df['nama_kabupaten_kota'].value_counts()
# based above result we should modify our code to see each total regencies cities victim count
# same with another columns


def count_based_on(column_name):
    filtered_df = df.groupby(column_name)['jumlah'].sum(
    ).reset_index().sort_values(by='jumlah', ascending=False)
    sns.barplot(x=column_name, y='jumlah', data=filtered_df)
    plt.xticks(rotation=90)
    plt.show()
    return filtered_df


count_based_on('nama_kabupaten_kota')
count_based_on('jenis_kelamin')
count_based_on('tahun')
count_based_on('kategori_pendidikan')

# # Handdle Type and Delete Unused Value
replacements = {
    'TIDAK \nSEKOLAH': 'TIDAK SEKOLAH',
    'PERGURUAN \nTINGGI': 'PERGURUAN TINGGI',
    'SLTP': 'SMP',
    'SLTA': 'SMA',
    'TK': 'TK/PAUD',
    'PAUD': 'TK/PAUD',
    'NA': 'TIDAK SEKOLAH',
    'TIDAK/BELUM PERNAH SEKOLAH': 'TIDAK SEKOLAH'

}

df.replace(replacements, inplace=True)
# we want to analyze students only
df = df[df['kategori_pendidikan'] != 'TIDAK SEKOLAH']

count_based_on('kategori_pendidikan')

# # Add New Useful Feature
# add column umur as request by team


def calculate_umur(row):
    if (row['kategori_pendidikan'] == 'TK/PAUD'):
        return '<5'
    elif row['kategori_pendidikan'] == 'SD':
        return '6-11'
    elif row['kategori_pendidikan'] == 'SMP':
        return '12-14'
    elif row['kategori_pendidikan'] == 'SMA':
        return '15-17'
    elif row['kategori_pendidikan'] == 'PERGURUAN TINGGI':
        return '>18'
    else:
        return 'Unknown'


df['umur'] = df.apply(calculate_umur, axis=1)
# add column latitude longitude to perform some map chart in looker studio
unique_locations = df['nama_kabupaten_kota'].unique()

geolocator = Nominatim(user_agent="MyApp")
location_data = {}


def get_lat_long(location):
    location_info = geolocator.geocode(location)
    if location_info:
        return location_info.latitude, location_info.longitude
    else:
        return None, None


for location in unique_locations:
    location_data[location] = get_lat_long(location)

df[['latitude', 'longitude']] = df['nama_kabupaten_kota'].apply(
    lambda x: pd.Series(location_data.get(x, (None, None))))
df['location'] = df['latitude'].astype(str) + ',' + df['longitude'].astype(str)
df.drop(['latitude', 'longitude'], axis=1, inplace=True)
# cek final data
df

# # Save Cleaned Data into Drive and BigQuerry
df.to_csv(os.path.join(dir, 'jabar-open-data/cleaned_dataset.csv'), index=False)
pandas_gbq.to_gbq(df, destination_table='capstone-399306.jabar_open_dataset.dataset',
                  project_id='capstone-399306', if_exists='replace')
