import subprocess
import sys
import tensorflow as tf
import numpy as np

def run_pip_commands():
    # Uninstall TensorFlow if it's installed
    subprocess.check_call([sys.executable, "-m", "pip", "uninstall", "-y", "tensorflow"])

    # Upgrade pip to ensure latest version
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip"])

    #upgradenumpy
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "numpy"])

    # Install a specific version of TensorFlow
    subprocess.check_call([sys.executable, "-m", "pip", "install", "tensorflow==2.16.2"])

    # Install additional packages required for your environment
    packages = [
        "tqdm",           # Progress bar
        "pandas",         # Data manipulation
        "boto3",          # AWS SDK for Python
        "opencv-python",  # Computer vision library
        "scikit-learn",   # Machine learning
        "plotly",         # Interactive plotting
        "influxdb-client",# InfluxDB client
        "pvlib",          # Solar library
        "h5py",           # HDF5 file support
        "matplotlib",     # Plotting library
        "keras",          # Deep learning framework
        "numpy",          # Numerical computations
        "scipy",          # Scientific computations
        "pillow",         # Image processing
        "seaborn"         # Statistical data visualization
    ]

    # Install each required package
    for package in packages:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])

if __name__ == "__main__":
    # Run pip commands to install packages
    run_pip_commands()

    # Print TensorFlow version
    print(f"TensorFlow version: {tf.__version__}")

    # List available GPUs
    gpus = tf.config.list_physical_devices('GPU')
    if gpus:
        print(f"GPUs available: {[gpu.name for gpu in gpus]}")
    else:
        print("No GPUs available")

    # Now you can safely import other libraries after installation
    import pandas as pd
    import time
    import os
    import boto3
    import cv2
    import pvlib
    from pvlib import location
    import datetime
    from datetime import datetime
    import pytz
    latitude = 46.518404934146574
    longitude = 6.565174801663707
    batiment = location.Location(latitude, longitude, tz='UTC', altitude=400, name='ELD')
    from datetime import datetime, timedelta
    from sklearn.metrics import mean_squared_error, mean_absolute_error, mean_absolute_percentage_error
    import math
    import matplotlib.pyplot as plt
    import bisect
    from datetime import datetime, timedelta, timezone
    import plotly.graph_objects as go
    import h5py
    from sklearn.linear_model import Ridge, ElasticNet, LinearRegression
    from sklearn.model_selection import GridSearchCV, train_test_split
    from tensorflow.keras.optimizers import Adam
    from tensorflow.keras.layers import Conv2D, LSTM, Flatten, BatchNormalization, TimeDistributed, Dense, \
        MaxPooling2D, GlobalMaxPooling2D, Dropout, Reshape, concatenate, Input
    from tensorflow.keras.layers import (
    Input, Conv2D, BatchNormalization, MaxPooling2D, Dropout, Add, Flatten, Dense, Subtract, Multiply, Concatenate)
    from tensorflow.keras.models import Model
    from collections import defaultdict
    from tensorflow.keras.regularizers import l2
    from tensorflow.keras.utils import Sequence
    import itertools
    from tqdm import tqdm
    import concurrent.futures
    import random as python_random
    from influxdb_client import InfluxDBClient
    np.random.seed(12345)
    python_random.seed(12345)
    access_key = ***
    secret_key = ***
    bucket_id  = ***
    endpoint = ***
    # Initialize the S3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint
    )
    print("Setup complete.")


import pandas as pd
import time
import os
import boto3
import cv2
import pvlib
from pvlib import location
import datetime
from datetime import datetime
import pytz
latitude = 46.518404934146574
longitude = 6.565174801663707
batiment = location.Location(latitude, longitude, tz='UTC', altitude=400, name='ELD')
from datetime import datetime, timedelta
from sklearn.metrics import mean_squared_error, mean_absolute_error, mean_absolute_percentage_error
import math
import matplotlib.pyplot as plt
import bisect
from datetime import datetime, timedelta, timezone
import plotly.graph_objects as go
import h5py
from sklearn.linear_model import Ridge, ElasticNet, LinearRegression
from sklearn.model_selection import GridSearchCV, train_test_split
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.layers import Conv2D, LSTM, Flatten, BatchNormalization, TimeDistributed, Dense, \
    MaxPooling2D, GlobalMaxPooling2D, Dropout, Reshape, concatenate, Input
from tensorflow.keras.layers import (
Input, Conv2D, BatchNormalization, MaxPooling2D, Dropout, Add, Flatten, Dense, Subtract, Multiply, Concatenate)
from tensorflow.keras.models import Model
from tensorflow.keras.regularizers import l2
from tensorflow.keras.utils import Sequence
from tqdm import tqdm
import concurrent.futures
import random as python_random
import concurrent.futures
from tqdm import tqdm
import itertools
from collections import defaultdict
from copy import deepcopy
from influxdb_client import InfluxDBClient
np.random.seed(12345)
python_random.seed(12345)

# Initialize the S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    endpoint_url=endpoint
)





class InfluxDBQueryClient:
    def __init__(self, ip, port, token, org):
        self.url = f"http://{ip}:{port}"
        self.token = token
        self.org = org
        self.timeout=5e5
        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org,timeout=self.timeout)

    def query_measured_ghi(self, start_time, end_time, resolution):
        """
        Query the measured GHI from the InfluxDB database
        :param start_time: datetime.datetime
        :param end_time: datetime.datetime
        :param resolution: str
        :return: pd.DataFrame
        """
        bucket = "***"

        # Convert datetime to Unix timestamp
        t0 = round(start_time.timestamp())
        tf = round(end_time.timestamp())

        # Prepare the Flux query
        flux_query = f"""
        from(bucket: "{bucket}")
        |> range(start: {t0}, stop: {tf})
          |> filter(fn: (r) => r["_measurement"] == "microgrid")
          |> filter(fn: (r) => r["Resource"] == "meteobox_roof")
          |> filter(fn: (r) => r["_field"] == "GHI")
          |> aggregateWindow(every: {resolution}, fn: mean, createEmpty: false)
          |> yield(name: "mean")
          |> pivot(rowKey:["_time"], columnKey: ["Resource"], valueColumn: "_value")
        """

        # Query the data
        query_api = self.client.query_api()
        measured_GHI_api15 = query_api.query_data_frame(org=self.org, query=flux_query)
        return measured_GHI_api15[0]

    def query_ghi_DA_forecast(self, start_time, end_time, resolution):
        """
        Query the forecast GHI from solcast day ahead from the InfluxDB database
        :param start_time: datetime.datetime
        :param end_time: datetime.datetime
        :param resolution: str
        :return: pd.DataFrame
        """
        bucket = "logging"

        # Convert datetime to Unix timestamp
        t0 = round(start_time.timestamp())
        tf = round(end_time.timestamp())

        # Prepare the Flux query
        flux_query = f"""
        from(bucket: "{bucket}")
        |> range(start: {t0}, stop: {tf})
          |> filter(fn: (r) => r["_measurement"] == "WAday1")
          |> filter(fn: (r) => r["_field"] == "value")
          |> filter(fn: (r) => r["solcast_weekahead"] == "GHI" or r["solcast_weekahead"] == "GHI10" or r["solcast_weekahead"] == "GHI90" or r["solcast_weekahead"] == "airT")
          |> aggregateWindow(every: {resolution}, fn: mean, createEmpty: false)
          |> yield(name: "mean")
          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        """

        # Query the data
        query_api = self.client.query_api()
        ghi_DA_forecast = query_api.query_data_frame(org=self.org, query=flux_query)
        ghi_DA_forecast = ghi_DA_forecast.pivot(index='_time', columns='solcast_weekahead', values='_value').reset_index()
        ghi_DA_forecast = ghi_DA_forecast.rename(columns={"_time":'time'}).set_index('time')
        ghi_DA_forecast.columns.name = None
        return ghi_DA_forecast
    
    
    def query_nissan_leaf_voltage(self, start_time, end_time, resolution):
            """
            Query the CellVoltage of NissanLeaf from the InfluxDB database
            :param start_time: datetime.datetime
            :param end_time: datetime.datetime
            :param resolution: str
            :return: pd.DataFrame
            """
            bucket = "microgrid_ST"
            t0 = round(start_time.timestamp())
            tf = round(end_time.timestamp())

            flux_query = f"""
            from(bucket: "microgrid_ST")
            |> range(start: {t0}, stop: {tf})
            |> filter(fn: (r) => r["_measurement"] == "microgrid")
            |> filter(fn: (r) => r["Resource"] == "NissanLeaf")
            |> filter(fn: (r) => r["_field"] == "CellVoltage")
            |> aggregateWindow(every: {resolution}, fn: mean, createEmpty: false)
            |> yield(name: "mean")
            """

            query_api = self.client.query_api()
            nissan_leaf_voltage = query_api.query_data_frame(org=self.org, query=flux_query)
            nissan_leaf_voltage = nissan_leaf_voltage.pivot(index='_time', columns='Cell', values='_value').reset_index()
            return nissan_leaf_voltage 


    def pull_solcast_forecast(self, forecast_time):
        """
        Pull Solcast forecast data for GHI from InfluxDB.
        :param forecast_time: datetime.datetime, start of the query range
        :return: pd.DataFrame
        """
        to = forecast_time + timedelta(minutes=1)
        # Convert datetime to Unix timestamp
        t0 = round(forecast_time.timestamp())
        tf = round(to.timestamp())

        bucket = "Forecasting_ST"

        # Prepare the Flux query
        flux_query = f"""
        from(bucket: "{bucket}")
        |> range(start: {t0}, stop: {tf})
        |> filter(fn: (r) => r["type"] == "Solcast")
        |> filter(fn: (r) => r["_field"] == "ghi")
        |> pivot(rowKey:["_time"], columnKey: ["prediction_time"], valueColumn: "_value")
        |> yield(name: "mean")
        """
        # Query the data
        query_api = self.client.query_api()
        solcast_forecast = query_api.query_data_frame(org=self.org, query=flux_query)

        return solcast_forecast
    
    def pull_solcast_forecast_bulk(self, start_time, end_time):
        """
        Pull Solcast forecast data for GHI from InfluxDB over a time range.
        :param start_time: datetime.datetime
        :param end_time: datetime.datetime
        :return: pd.DataFrame
        """
        bucket = "Forecasting_ST"
        t0 = round(start_time.timestamp())
        tf = round(end_time.timestamp())
    
        flux_query = f"""
        from(bucket: "{bucket}")
        |> range(start: {t0}, stop: {tf})
        |> filter(fn: (r) => r["type"] == "Solcast")
        |> filter(fn: (r) => r["_field"] == "ghi")
        |> pivot(rowKey:["_time"], columnKey: ["prediction_time"], valueColumn: "_value")
        |> yield(name: "mean")
        """
    
        query_api = self.client.query_api()
        try:
            df = query_api.query_data_frame(org=self.org, query=flux_query)
        except Exception as e:
            print("Error al hacer la consulta:", e)
            raise

        #df = query_api.query_data_frame(org=self.org, query=flux_query)
        return df
    def pull_solcast_forecast_bulk2(self, start_time, end_time):
        bucket = "Forecasting_ST"
        t0 = round(start_time.timestamp())
        tf = round(end_time.timestamp())
    
        flux_query = f"""
        from(bucket: "{bucket}")
        |> range(start: {t0}, stop: {tf})
        |> filter(fn: (r) => r["type"] == "Solcast")
        |> filter(fn: (r) => r["_field"] == "ghi" or r["_field"] == "ghi10" or r["_field"] == "ghi90")
        |> pivot(rowKey:["_time"], columnKey: ["prediction_time", "_field"], valueColumn: "_value")
        |> yield(name: "mean")
        """
    
        query_api = self.client.query_api()
        try:
            df = query_api.query_data_frame(org=self.org, query=flux_query)
        except Exception as e:
            print("Error al hacer la consulta:", e)
            raise
    
        return df

def smart_persistent_predictions(timestamps, y_test, prediction_horizon=10):
    """
    Generate GHI predictions using a smart persistence model based on PVlib clear-sky GHI values.

    Parameters:
        timestamps (list or pd.DatetimeIndex): Timestamps for which to generate predictions.
        y (list or np.ndarray): Actual GHI values (for comparison or model adjustment).
        prediction_horizon (int): Number of time steps to forecast ahead for each timestamp.

    Returns:
        list of lists: Predicted GHI values for each timestamp over the prediction horizon.
    """
    ghi_predictions = []

    # Cache clear-sky values for the initial timestamps
    pvlib_ghi = batiment.get_clearsky(pd.DatetimeIndex(timestamps)).ghi.values
    future_timestamps = pd.DatetimeIndex(timestamps) + pd.Timedelta(minutes=prediction_horizon)
    future_pvlib_ghi = batiment.get_clearsky(pd.DatetimeIndex(future_timestamps)).ghi.values
    #check how to handle zero
    coeffs=future_pvlib_ghi/(pvlib_ghi+1)
    coeffs[coeffs>100]=100
    ghi_predictions=y_test*(coeffs)
    
    return ghi_predictions, future_timestamps


def calculate_picp_and_pinaw(y_test, y_pred, val_residuals, alpha=0.1):
    """
    Calculate PICP and PINAW for prediction intervals based on residuals.

    Args:
        y_test (array-like): Actual observed values from the test set.
        y_pred (array-like): Predicted values for the test set.
        val_residuals (array-like): Residuals from the validation set.
        alpha (float): Significance level for the prediction interval (default is 0.05 for 95% PI).

    Returns:
        tuple: (PICP, PINAW) values.
    """
    y_test = np.array(y_test)
    y_pred = np.array(y_pred)
    val_residuals = np.array(val_residuals)

    M = len(y_test)  # Number of test points

    # Calculate the lower and upper quantiles of residuals
    lower_quantile = np.quantile(val_residuals, alpha / 2)
    upper_quantile = np.quantile(val_residuals, 1 - alpha / 2)
    print(f'lower quantile:{lower_quantile}, upper:{upper_quantile}')
    # Generate prediction interval bounds centered around y_pred
    y_lower = y_pred + lower_quantile
    y_upper = y_pred + upper_quantile

    # Ensure y_lower and y_upper are 1D arrays
    y_lower = np.squeeze(y_lower)
    y_upper = np.squeeze(y_upper)

    # PICP Calculation
    coverage = np.logical_and(y_test >= y_lower, y_test <= y_upper)
    PICP = np.mean(coverage)  # Average proportion in range

    # PINAW Calculation
    interval_widths = y_upper - y_lower
    y_range = np.max(y_test) - np.min(y_test)  # Range of observed values
    if y_range == 0:
        raise ValueError("Range of observed values is zero, cannot calculate PINAW.")
    PINAW = np.mean(interval_widths) / y_range

    return PICP, PINAW, y_lower, y_upper


def download_and_process_image(image, max_retries=3, backoff_factor=2):
    """
    Download and decode an image from S3, retrying up to `max_retries` times on error.
    If all attempts fail, return a placeholder ("z4ros") and print error.
    """
    for attempt in range(1, max_retries + 1):
        try:
            response = s3.get_object(Bucket=bucket_id, Key=image)
            image_data = response['Body'].read()
            image_np = np.frombuffer(image_data, np.uint8)
            img = cv2.imdecode(image_np, cv2.IMREAD_COLOR)
            return (image, img)
        except (ReadTimeoutError, EndpointConnectionError, BotoCoreError) as e:
            print(f"[Attempt {attempt}/{max_retries}] Error downloading {image}: {e}")
            if attempt < max_retries:
                sleep_time = backoff_factor ** (attempt - 1)
                print(f"→ Retrying in {sleep_time}s...")
                time.sleep(sleep_time)
            else:
                print(f"→ Failed to download {image} after {max_retries} attempts. Error with image.")
                # Aquí puedes devolver un placeholder:
                # Por ejemplo, un array de ceros con la forma que esperas, o un string especial.
                placeholder_img =np.zeros((250,250,3), dtype=np.uint8)
                return (image, placeholder_img)



def get_images_from_s3(images_every,channels):
    from urllib3.exceptions import ReadTimeoutError
    from botocore.exceptions import EndpointConnectionError, BotoCoreError


    
    # Initialize the S3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint
    )
    # Initialize S3 client
    latitude = 46.518404934146574
    longitude = 6.565174801663707
    timezone = 'Europe/Zurich'
    
    folder_name = 'Pedro/All_sky_camera/'
    # List objects in the spe S3 folder
    response = s3.list_objects_v2(Bucket=bucket_id, Prefix=folder_name)
    images = []
    header='mean'
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_id, Prefix=folder_name):
        images.extend([obj['Key'] for obj in page.get('Contents', []) if obj['Key'].startswith(folder_name + header)])
    images.sort()
    print('all images viewed,now downloading')
    #images=images[0:15000]
    images=images[::images_every]

    image_dict = {}

    # Parallel image downloading and processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=int(os.cpu_count()*0.5)) as executor:
        results = list(tqdm(executor.map(download_and_process_image, images), total=len(images)))

    # Populate dictionary with image key as index
    image_dict = {key: img for key, img in results}

    return image_dict

def extract_timestamp_from_image(image_key):
    """
    Extracts the timestamp from the image key (of the form 'All_sky_camera/meanYYYY-MM-DD HH:MM:SS.ssssss.png'),
    and localizes it to the Europe/Zurich timezone.
    """
    timestamp_str = image_key.split('/')[2][4:-4]  # Extract timestamp string
    timestamp = pd.to_datetime(timestamp_str)
    localized_timestamp = timestamp.tz_localize('Europe/Zurich')
    localized_timestamp=localized_timestamp.tz_convert('UTC')
    return localized_timestamp
def create_merged_data_dict(image_keys,
                            threshold_minutes=15,
                            num_previous_images=3,
                            image_delta_t=1,
                            Hdelta_t=3,
                            delta_t=1):
    """
    Builds a dict mapping each image timestamp to:
      - image_key
      - raw and normalized GHI sequences for t..t+Hdelta_t
      - raw and normalized clear-sky GHI sequences
      - max daily clear-sky GHI
      - list of current + previous image keys

    Future GHI is sampled at exact minute offsets from each image time,
    without any spillover from subsequent images.

    Returns:
        merged_data_dict: dict of {timestamp: {...}}
        deleted_timestamps: list of timestamps dropped due to missing data
    """
    start_total = time.time()

    # --- 1) Prepare image_df ---
    t1 = time.time()
    image_timestamps = [extract_timestamp_from_image(k) for k in image_keys]
    image_df = pd.DataFrame({
        'image_timestamp': pd.to_datetime(image_timestamps),
        'image_key': image_keys
    }).sort_values('image_timestamp').reset_index(drop=True)
    image_df['img_idx'] = image_df.index
    print(f"Step 1 (Prepare image_df) took {time.time() - t1:.2f}s")

    # --- 2) Time range for GHI query ---
    t2 = time.time()
    dates = image_df['image_timestamp'].dt.date.unique()
    starts = [pd.Timestamp(d).tz_localize('UTC') - pd.Timedelta(hours=4) for d in dates]
    ends =   [pd.Timestamp(d).tz_localize('UTC') + pd.Timedelta(days=1, hours=4) for d in dates]
    start_time, end_time = min(starts), max(ends) + pd.Timedelta(minutes=1)
    print(f"Step 2 (Compute query window) took {time.time() - t2:.2f}s")

    # --- 3) Query GHI from InfluxDB ---
    t3 = time.time()
    client = InfluxDBQueryClient(
        ip='***',
        port=***,
        token=***,
        org=***,
    )
    
    ghi_df = client.query_measured_ghi(start_time, end_time, '1m')

    ghi_df = (
        ghi_df.rename(columns={'_time':'time','_value':'GHI'})
              .assign(time=lambda df: pd.to_datetime(df['time']).dt.tz_convert('UTC'))
              .set_index('time')
    )
    ghi_df['GHI_15min_avg'] = ghi_df['GHI'].rolling(window=15,center=True,min_periods=1).mean()
    print(f"Step 3 (Query GHI) took {time.time() - t3:.2f}s")

    # --- 4) Clear-sky calculation ---
    t4 = time.time()
    time_range = pd.date_range(start_time, end_time, freq='1min', tz='UTC')
    cs = batiment.get_clearsky(time_range).ghi.rename('clear_sky_GHI').to_frame()
    print(f"Step 4 (Clear-sky calc) took {time.time() - t4:.2f}s")

    # --- 5) Merge asof measured GHI at image times ---
    t5 = time.time()
    merged = pd.merge_asof(
        image_df,
        ghi_df[['GHI','GHI_15min_avg']].reset_index().rename(columns={'time':'closest_time'}),
        left_on='image_timestamp', right_on='closest_time',
        direction='nearest', tolerance=pd.Timedelta(minutes=threshold_minutes)
    )
    merged['time_diff'] = (merged['image_timestamp'] - merged['closest_time']).abs().dt.total_seconds()/60
    merged.loc[merged['time_diff']>threshold_minutes, ['GHI','GHI_15min_avg']] = np.nan
    merged = merged.dropna(subset=['GHI'])
    print(f"Step 5 (As-of merge GHI) took {time.time() - t5:.2f}s")

    # --- 6) Merge asof clear-sky at image times ---
    t6 = time.time()
    merged = pd.merge_asof(
        merged.sort_values('image_timestamp'),
        cs.reset_index().rename(columns={'index':'closest_time'}),
        on='closest_time', direction='nearest', tolerance=pd.Timedelta(minutes=threshold_minutes)
    ).dropna(subset=['clear_sky_GHI'])
    print(f"Step 6 (As-of merge clear-sky) took {time.time() - t6:.2f}s")

    # --- 7) Daily max clear-sky ---
    t7 = time.time()
    merged['date'] = merged['image_timestamp'].dt.date
    merged['max_clear_sky_ghi'] = merged.groupby('date')['clear_sky_GHI'].transform('max')
    print(f"Step 7 (Max clear-sky) took {time.time() - t7:.2f}s")

    # --- 8) Normalize ---
    t8 = time.time()
    merged['GHI_normalized'] = merged['GHI'] / merged['max_clear_sky_ghi']
    merged['clear_sky_ghi_normalized'] = merged['clear_sky_GHI'] / merged['max_clear_sky_ghi']
    print(f"Step 8 (Normalization) took {time.time() - t8:.2f}s")

    # --- 9) Past images list ---
    t9 = time.time()
    for i in range(1, num_previous_images+1):
        merged[f'prev_img_{i}'] = merged['image_key'].shift(i * image_delta_t)
    merged['image_keys'] = merged.apply(
        lambda r: [r['image_key']] + [r[f'prev_img_{i}'] for i in range(1,num_previous_images+1) if pd.notna(r[f'prev_img_{i}'])], axis=1
    )
    print(f"Step 9 (Past images) took {time.time() - t9:.2f}s")

    # --- 10) Build future-offset table ---
    t10 = time.time()
    img = merged[['img_idx','image_timestamp','max_clear_sky_ghi']]
    steps = np.arange(0, Hdelta_t+1) * delta_t
    offsets = pd.DataFrame({
        'img_idx': np.repeat(img['img_idx'].values, len(steps)),
        'step': np.tile(steps, len(img))
    })
    offsets['time'] = offsets['img_idx'].map(img.set_index('img_idx')['image_timestamp']) + pd.to_timedelta(offsets['step'], unit='m')
    print(f"Step 10 (Build offsets) took {time.time() - t10:.2f}s")

    # --- 11) Bulk asof for GHI and clear-sky at offsets ---
    t11 = time.time()
    ghi_lu = ghi_df.reset_index()[['time','GHI']]
    cs_lu  = cs.reset_index().rename(columns={'index':'time'})[['time','clear_sky_GHI']]
    offsets = pd.merge_asof(offsets.sort_values('time'), ghi_lu.sort_values('time'), on='time', direction='nearest', tolerance=pd.Timedelta(minutes=threshold_minutes))
    offsets = pd.merge_asof(offsets.sort_values('time'), cs_lu.sort_values('time'), on='time', direction='nearest', tolerance=pd.Timedelta(minutes=threshold_minutes))
    print(f"Step 11 (Bulk asof) took {time.time() - t11:.2f}s")

    # --- 12) Group into sequences ---
    t12 = time.time()
    seqs = (offsets.sort_values(['img_idx','step'])
            .groupby('img_idx')
            .agg({
                'GHI': lambda x: x.tolist(),
                'clear_sky_GHI': lambda x: x.tolist()
            })
           )
    seqs['max_cs'] = img.set_index('img_idx')['max_clear_sky_ghi']
    seqs['ghi_values_normalized'] = seqs.apply(lambda r: [v/r['max_cs'] if pd.notna(v) else np.nan for v in r['GHI']], axis=1)
    seqs['clear_sky_ghi_normalized_seq'] = seqs.apply(lambda r: [v/r['max_cs'] if pd.notna(v) else np.nan for v in r['clear_sky_GHI']], axis=1)
    seqs = seqs.rename(columns={'GHI':'ghi_values','clear_sky_GHI':'clear_sky_ghi_seq'})
    print(f"Step 12 (Group sequences) took {time.time() - t12:.2f}s")

    # --- 13) Join sequences back ---
    t13 = time.time()
    merged = merged.join(seqs, on='img_idx')
    print(f"Step 13 (Join sequences) took {time.time() - t13:.2f}s")

    # --- 14) Build final dict (vectorized) ---
    t14 = time.time()
    merged_clean = merged.dropna(subset=['ghi_values'])

    # Convert to list of records once
    records = merged_clean[['image_timestamp', 'image_key', 'ghi_values',
                            'ghi_values_normalized', 'clear_sky_ghi_seq',
                            'clear_sky_ghi_normalized_seq', 'max_clear_sky_ghi',
                            'image_keys']].to_dict(orient='records')

    # Build dictionary via comprehension (avoids iterrows)
    merged_data_dict = {
        rec['image_timestamp']: {
            'image_key': rec['image_key'],
            'ghi_values': rec['ghi_values'],
            'ghi_values_normalized': rec['ghi_values_normalized'],
            'clear_sky_ghi_seq': rec['clear_sky_ghi_seq'],
            'clear_sky_ghi_normalized_seq': rec['clear_sky_ghi_normalized_seq'],
            'max_clear_sky_ghi': rec['max_clear_sky_ghi'],
            'image_keys': rec['image_keys'],
        }
        for rec in records
    }

    deleted = merged['image_timestamp'][~merged['image_timestamp'].isin(merged_clean['image_timestamp'])].tolist()
    print(f"Step 14 (Build dict) took {time.time() - t14:.2f}s")

    print(f"Total execution time: {time.time() - start_total:.2f}s; Dropped {len(deleted)} images")

    
    new_dict = {}

    for i, (old_key, value) in enumerate(merged_data_dict.items()):
        # Copy the value and add 'key_timestamp'
        value_with_key = value.copy()
        value_with_key['key_timestamp'] = old_key
        # Assign to new dict with int key
        new_dict[i] = value_with_key

    
    
    def has_nan(value):
        try:
            if isinstance(value, float):
                return np.isnan(value)
            elif isinstance(value, (list, np.ndarray)):
                return any(np.isnan(x) for x in value if isinstance(x, (float, int)))
            else:
                return False
        except:
            return False
    
    cleaned_dict = {
        k: v for k, v in new_dict.items()
        if not any(has_nan(val) for val in v.values())
    }
    
    merged_data_dict = cleaned_dict

    return merged_data_dict
def load_image_from_dict(image_key, image_dict):
    """
    Fetch the image from the image_dict by key.
    Converts it from uint8 to float32 [0,1] if found, else returns zeros.
    """
    image = image_dict.get(image_key, None)
    if image is not None:
        #print('load image from dict oaky')
        return image
        
    else:
        print('cuidao')
        return np.zeros((250, 250, 3), dtype=np.float32)
    
       
def process_sample(timestamp, merged_data_dict, image_dict, expected_num_images,Hdelta_t):#warining for dummy data, if not error, remove
    """
    Processes a sample by loading images from the image_dict and retrieving GHI measurements directly.
    Loads images as uint8 to optimize memory and conversion will happen later in the pipeline.
    """
    try:
        sample = merged_data_dict[int(timestamp)]
    except KeyError as e:
        print(f"Timestamp {timestamp} not found in merged_data_dict: {e}")
        return (np.zeros((expected_num_images, 250, 250, 3), dtype=np.uint8),
                np.zeros((Hdelta_t+1,), dtype=np.float32),
                np.zeros((Hdelta_t+1,), dtype=np.float32))

    # Load images as uint8 without processing
    image_keys = sample.get('image_keys', [])
    images = [load_image_from_dict(key, image_dict).astype(np.uint8) for key in image_keys]
    images = np.stack(images, axis=0) 
    '''
    # Pad if necessary
    if len(image_keys) < expected_num_images:
        
        pad = np.zeros((expected_num_images - len(image_keys), 250, 250, 3), dtype=np.uint8)
        images = np.concatenate([images, pad], axis=0)
    '''
    # Retrieve GHI values directly
    clear_sky_ghi = np.array(sample['clear_sky_ghi_normalized_seq'], dtype=np.float32)
    ghi_measurements = np.array(sample['ghi_values_normalized'], dtype=np.float32)

    return images, clear_sky_ghi, ghi_measurements

def tf_process_sample(timestamp, merged_data_dict, image_dict, expected_num_images, Hdelta_t):
    """
    TensorFlow wrapper to process sample using tf.py_function.
    Returns images as uint8 and handles normalization later in the pipeline.
    """
    images, clear_sky_ghi, ghi_measurements = tf.py_function(
        func=lambda ts: process_sample(ts.numpy(), merged_data_dict, image_dict, expected_num_images, Hdelta_t),
        inp=[timestamp],
        Tout=(tf.uint8, tf.float32, tf.float32) 
    )
    images.set_shape((expected_num_images, 250, 250, 3))
    clear_sky_ghi.set_shape((Hdelta_t+1,))
    ghi_measurements.set_shape((Hdelta_t+1,))
    return (images, clear_sky_ghi), ghi_measurements

import tensorflow as tf
from tensorflow.autograph.experimental import do_not_convert

def make_map_process_sample(merged_data_dict, image_dict, expected_num_images, Hdelta_t):
    def map_process_sample(ts):
        return tf_process_sample(ts, merged_data_dict, image_dict, expected_num_images, Hdelta_t)
    return map_process_sample

def map_cast_and_normalize(x, y):
    return ((tf.cast(x[0], tf.float16) / 255.0, x[1]), y)

def batch_generator_tf_from_keys(keys, merged_data_dict, image_dict, batch_size, expected_num_images, Hdelta_t):
    map_process_sample = make_map_process_sample(merged_data_dict, image_dict, expected_num_images, Hdelta_t)

    ds = tf.data.Dataset.from_tensor_slices(np.array(keys, dtype=np.int64))
    ds = ds.map(map_process_sample, num_parallel_calls=tf.data.AUTOTUNE)
    ds = ds.batch(batch_size, drop_remainder=True)
    ds = ds.map(map_cast_and_normalize, num_parallel_calls=tf.data.AUTOTUNE)
    ds = ds.prefetch(tf.data.AUTOTUNE)

    print("Data preparation completed for", len(keys), "samples.")
    return ds




def compute_rmse(y_true, y_pred):
    return np.sqrt(np.mean((y_true - y_pred)**2, axis=0))
def plot_metrics(history):
    fig, axs = plt.subplots(2, 1, figsize=(10, 10), sharex=True)

    # Loss
    axs[0].plot(history.history['loss'], label='Training Loss')
    axs[0].plot(history.history['val_loss'], label='Validation Loss')
    axs[0].set_ylabel('Loss')
    axs[0].set_title('Model Loss Over Epochs')
    axs[0].legend()
    axs[0].grid()

    # MAE
    axs[1].plot(history.history['mae'], label='Training MAE')
    axs[1].plot(history.history['val_mae'], label='Validation MAE')
    axs[1].set_xlabel('Epoch')
    axs[1].set_ylabel('MAE')
    axs[1].set_title('Mean Absolute Error Over Epochs')
    axs[1].legend()
    axs[1].grid()

    plt.tight_layout()
    plt.show()
def plot_metrics(history):
    fig, axs = plt.subplots(2, 1, figsize=(10, 10), sharex=True)

    # Loss (log scale)
    axs[0].plot(history.history['loss'], label='Training Loss')
    axs[0].plot(history.history['val_loss'], label='Validation Loss')
    axs[0].set_ylabel('Loss')
    axs[0].set_title('Model Loss Over Epochs (Log Scale)')
    axs[0].set_yscale('log')
    axs[0].legend()
    axs[0].grid(True, which='both')

    # MAE (log scale)
    axs[1].plot(history.history['mae'], label='Training MAE')
    axs[1].plot(history.history['val_mae'], label='Validation MAE')
    axs[1].set_xlabel('Epoch')
    axs[1].set_ylabel('MAE')
    axs[1].set_title('Mean Absolute Error Over Epochs (Log Scale)')
    axs[1].set_yscale('log')
    axs[1].legend()
    axs[1].grid(True, which='both')

    plt.tight_layout()
    plt.show()
from datetime import timedelta


import boto3
import os
import boto3
import os
import inspect
from datetime import datetime
from botocore.config import Config

def upload_results(model, history, rmse_real,create_model,params):
    """Uploads the model code, weights, training history, RMSE, and figure to EPFL S3
       with chunking/payload-signing disabled so binaries remain valid."""
    # —– S3 credentials & disable chunked signing —–
    boto_cfg = Config(
        signature_version='s3',
        s3={'payload_signing_enabled': False}
    )
    s3 = boto3.client(***
    )
    bucket = '***'
    prefix = datetime.now().strftime("Pedro/results/%Y%m%d_%H%M%S/")

    print("Uploading results to:", prefix)

    # 1) Upload model definition (source code)
    src = inspect.getsource(create_model)
    s3.put_object(
        Bucket=bucket,
        Key=prefix + 'create_model.py',
        Body=src.encode('utf-8'),
        ContentType='text/x-python'
    )
    # 1.5) Upload params
    src = inspect.getsource(params)
    s3.put_object(
        Bucket=bucket,
        Key=prefix + 'params.py',
        Body=src.encode('utf-8'),
        ContentType='text/x-python'
    )
    # 2) Save & upload weights
    weights_file = 'model.weights.h5'
    model.save_weights(weights_file)
    with open(weights_file, 'rb') as f:
        data = f.read()
    s3.put_object(
        Bucket=bucket,
        Key=prefix + weights_file,
        Body=data,
        ContentType='application/octet-stream',
        ContentLength=len(data)
    )
    '''
----------
    # 3) Save & upload history CSV
    history_file = 'history.csv'
    pd.DataFrame(history.history).to_csv(history_file, index=False)
    s3.upload_file(history_file, bucket, prefix + history_file)
---------------
'''
    # Verifica el tipo de history y convierte apropiadamente
    if isinstance(history, dict):
        history_df = pd.DataFrame(history)
    elif hasattr(history, 'history') and isinstance(history.history, dict):
        history_df = pd.DataFrame(history.history)
    else:
        raise ValueError("El objeto `history` no es un diccionario ni un objeto de historial de Keras válido.")
    
    # Guardar en CSV
    history_file = 'history.csv'
    history_df.to_csv(history_file, index=False)
    
    # Subir a S3
    s3.upload_file(history_file, bucket, prefix + history_file)

    # 4) Save & upload RMSE
    rmse_file = 'rmse_real.txt'
    with open(rmse_file, 'w') as f:
        f.write(f"{rmse_real}\n")
    s3.upload_file(rmse_file, bucket, prefix + rmse_file)

    # 5) Upload figure (figure1.png)
    fig_path = 'figure1.png'
    with open(fig_path, 'rb') as f:
        data = f.read()
    s3.put_object(
        Bucket=bucket,
        Key=prefix + 'figure1.png',
        Body=data,
        ContentType='image/png',
        ContentLength=len(data)
    )

    print("✅ All artifacts uploaded successfully.")
    return

import time
import inspect
def try_upload(model, history,rmse_real,create_model,params):
    max_retries = 6
    for attempt in range(max_retries):
        try:
            upload_results(model, history,rmse_real,create_model,params)
            print("Upload successful.")
            break
        except Exception as e:
            print(f"Upload failed on attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)  # optional: wait before retrying
            else:
                print("All upload attempts failed.")
                raise
    return


def solcast_forecast(timestamps,  H_delta):
    client = InfluxDBQueryClient(
        ip=***,
        port=***,
        token=***,
        org=***,
    )
    delta_t=15
    start_time = min(timestamps)
    end_time = max(timestamps) + timedelta(minutes=H_delta * delta_t)

    data = client.pull_solcast_forecast_bulk(start_time, end_time)

    # Asegúrate de convertir las columnas de prediction_time a float si están en string
    columns = [str(float(i * delta_t)) for i in range(1, H_delta + 1)]
    columns = ['_time'] + columns
    data = data[[col for col in columns if col in data.columns]]

    return data
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def forecast_skill_plot(merged_data_dict, test_keys, Hdelta_t, rmses):
    """
    Computes RMSEs for Solcast forecasts, calculates forecast skill compared to given RMSEs,
    and plots forecast skill vs forecast horizon.

    Parameters:
        merged_data_dict (dict): Dictionary with 'key_timestamp' and 'ghi_values'.
        test_keys (list): List of keys to use.
        delta_t (int): Time step in minutes (e.g., 5).
        Hdelta_t (int): Number of forecast steps (e.g., 2 for t+15 and t+30).
        rmses (list or array): RMSE values of user's forecast model for each horizon.
    """

    # Step 1: Build DataFrame dynamically for ghi_values
    df = pd.DataFrame([
        {
            'key': k,
            'key_timestamp': merged_data_dict[k]['key_timestamp'],
            **{
                f'ghi_value_{i+1}': merged_data_dict[k]['ghi_values'][i+1]  # skip t0
                for i in range(Hdelta_t)
                if len(merged_data_dict[k]['ghi_values']) > i+1
            }
        }
        for k in test_keys
    ])

    # Step 2: Get Solcast forecast data
    test_timestamps = df['key_timestamp'].tolist()
    solcast_data = solcast_forecast(test_timestamps,  Hdelta_t)

    # Step 3: Sort dataframes by timestamp
    df = df.sort_values('key_timestamp').reset_index(drop=True)
    solcast_data = solcast_data.sort_values('_time').reset_index(drop=True)

    # Step 4: Merge by nearest timestamp within 5 minutes
    merged = pd.merge_asof(
        df,
        solcast_data,
        left_on='key_timestamp',
        right_on='_time',
        direction='nearest',
        tolerance=pd.Timedelta(minutes=5)
    )

    # Step 5: Drop unmatched rows
    merged_clean = merged.dropna(subset=['_time'])

    solcast_rmses = []

    # Step 6: Compute Solcast RMSEs dynamically
    for i in range(1, Hdelta_t + 1):
        ghi_col = f'ghi_value_{i}'
        forecast_min = i * 15
        solcast_col = f'{forecast_min}.0'
        if ghi_col in merged_clean.columns and solcast_col in merged_clean.columns:
            error = merged_clean[ghi_col] - merged_clean[solcast_col]
            rmse = np.sqrt(np.mean(error**2))
            solcast_rmses.append(rmse)
            print(f"Solcast RMSE ({forecast_min}-min forecast): {rmse:.2f}")
        else:
            print(f"Skipping RMSE computation for {forecast_min} min: column missing.")

    # Step 7: Compute forecast skill: 1 - rmses / solcast_rmses
    if len(rmses) != len(solcast_rmses):
        print("Warning: Length of input rmses and solcast rmses differ. Plotting truncated to min length.")
    
    length = min(len(rmses), len(solcast_rmses))
    
    solcast_rmses = solcast_rmses[:length]

    skill = [1 - (r / s) if s != 0 else np.nan for r, s in zip(rmses, solcast_rmses)]

    # Step 8: Plot forecast skill vs forecast horizon
    horizons = [(i+1)*15 for i in range(length)]

    plt.figure(figsize=(8,5))
    plt.plot(horizons, skill, marker='o', linestyle='-', color='tab:blue')
    plt.title('Forecast Skill vs Forecast Horizon')
    plt.xlabel('Forecast Horizon (minutes)')
    plt.ylabel('Forecast Skill (1 - RMSE_forecast / RMSE_solcast)')
    plt.grid(True)
    plt.ylim(-0.5, 1.1)
    plt.axhline(0, color='red', linestyle='--', linewidth=1)
    plt.show()

    return 