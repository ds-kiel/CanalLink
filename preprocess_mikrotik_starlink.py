import pandas as pd

def preprocess_data_mikrotik_lte(df):
    # Infer objects, then convert dtypes
    df = df.infer_objects().convert_dtypes()

    for column in df.columns:
        try:
            df[column] = pd.to_numeric(df[column])
        except (ValueError, TypeError):
            pass  # Skip columns that cannot be converted

    # Rename timestamp from Elastic and keep it for future use
    # It is unreliable if a lot of messages come at the same time due to congestion
    df['timestamp_elastic'] = df.pop('@timestamp')
    
    # It is better to rely on timestamps from the router rather than ElasticSearch
    df['timestamp_router'] = pd.to_datetime(df['date'] + ' ' +  df['time'])
    df.drop(columns=['date', 'time'], inplace=True)
    df.sort_values(by=['timestamp_router'], inplace=True)
    df.set_index('timestamp_router', inplace=True, drop=False)
    #df = df.reset_index()

    # Convert Data Class into integer mapping
    dataclass_mapping = {'': 0, 'LTE': 1, '5G NSA': 2, '5G SA': 3}
    df['lte.lDataClassInt'] = df['lte.lDataClass'].map(dataclass_mapping)

    # Convert modulation into fixed bits per hz mapping
    modulation_mapping = {'': 0, 'qpsk': 2, '16qam': 4, '64qam': 6, '256qam': 8}
    df['lte.lDlModulationInt'] = df['lte.lDlModulation'].map(modulation_mapping)
    df['lte.lNrDlModulationInt'] = df['lte.lNrDlModulation'].map(modulation_mapping)

    # Fix wrong scaling on Rsrq and NrRsrq
    # If the value is -12dB, it is shown as -120 on the Elastic Search
    df['lte.lRsrq'] = df['lte.lRsrq'] / 10
    df['lte.lNrRsrq'] = df['lte.lNrRsrq'] / 10

    df.info(verbose=True, show_counts=True)

    return df

def preprocess_data_starlink_elastic(df):
    
    # Infer objects, then convert dtypes
    df = df.infer_objects().convert_dtypes()

    for column in df.columns:
        try:
            df[column] = pd.to_numeric(df[column])
        except (ValueError, TypeError):
            pass  # Skip columns that cannot be converted

    # Use timestamp from Elastic
    # Not the best but Starlink does not provide timestamp by itself ???
    df['timestamp_elastic'] = df.pop('@timestamp')
    df['timestamp_elastic'] = pd.to_datetime(df['timestamp_elastic'])
    df.sort_values(by=['timestamp_elastic'], inplace=True)
    df.set_index('timestamp_elastic', inplace=True, drop=False)
    #df = df.reset_index()

    # Convert state into integer mapping
    state_mapping = {'SEARCHING': 0, 'NO_PINGS': 1, 'NO_DOWNLINK': 2, 'CONNECTED': 3}
    df['dish_status.state'] = df['dish_status.state'].map(state_mapping)

    # Convert currently_obstructed into integer mapping
    obstructed_mapping = {False: 0, True: 1}
    df['dish_status.currently_obstructed'] = df['dish_status.currently_obstructed'].map(obstructed_mapping)

    # Convert is_snr_above_noise_floor into integer mapping
    snr_mapping = {False: 0, True: 1}
    df['dish_status.is_snr_above_noise_floor'] = df['dish_status.is_snr_above_noise_floor'].map(snr_mapping)

    df.info(verbose=True, show_counts=True)

    return df

def preprocess_data_starlink_mqtt(df):

    # Infer objects, then convert dtypes
    df = df.infer_objects().convert_dtypes()

    for column in df.columns:
            try:
                df[column] = pd.to_numeric(df[column])
            except (ValueError, TypeError):
                pass  # Skip columns that cannot be converted

    df['timestamp'] = pd.to_datetime(df.pop('@timestamp'))
    df["timestamp"] = (
        df["timestamp"]
        .dt.tz_localize(None)        # 06:34:55.448743  (drop +02:00, keep clock time)
        + pd.Timedelta(hours=2)      # 08:34:55.448743
        )
    
    df.sort_values(by=['timestamp'], inplace=True)
    df.set_index('timestamp', inplace=True, drop=False)
    
    # Convert state into integer mapping
    state_mapping = {'SEARCHING': 0, 'NO_PINGS': 1, 'NO_DOWNLINK': 2, 'CONNECTED': 3}
    df['dish_status.state'] = df['dish_status.state'].map(state_mapping)

    # Convert currently_obstructed into integer mapping
    obstructed_mapping = {False: 0, True: 1}
    df['dish_status.currently_obstructed'] = df['dish_status.currently_obstructed'].map(obstructed_mapping)

    # Convert is_snr_above_noise_floor into integer mapping
    snr_mapping = {False: 0, True: 1}
    df['dish_status.is_snr_above_noise_floor'] = df['dish_status.is_snr_above_noise_floor'].map(snr_mapping)

    df.info(verbose=True, show_counts=True)

    return df

def preprocess_data_starlink_mqtt_without_timestamp(df):

    # Infer objects, then convert dtypes
    df = df.infer_objects().convert_dtypes()

    for column in df.columns:
            try:
                df[column] = pd.to_numeric(df[column])
            except (ValueError, TypeError):
                pass  # Skip columns that cannot be converted

    # df['timestamp'] = pd.to_datetime(df.pop('@timestamp'))
    # df["timestamp"] = (
    #     df["timestamp"]
    #     .dt.tz_localize(None)        # 06:34:55.448743  (drop +02:00, keep clock time)
    #     + pd.Timedelta(hours=2)      # 08:34:55.448743
    #     )
    
    # df.sort_values(by=['timestamp'], inplace=True)
    # df.set_index('timestamp', inplace=True, drop=False)

    df.sort_values(by=['dish_status.uptime'], inplace=True)
    
    # Convert state into integer mapping
    state_mapping = {'SEARCHING': 0, 'NO_PINGS': 1, 'NO_DOWNLINK': 2, 'CONNECTED': 3}
    df['dish_status.state'] = df['dish_status.state'].map(state_mapping)

    # Convert currently_obstructed into integer mapping
    obstructed_mapping = {False: 0, True: 1}
    df['dish_status.currently_obstructed'] = df['dish_status.currently_obstructed'].map(obstructed_mapping)

    # Convert is_snr_above_noise_floor into integer mapping
    snr_mapping = {False: 0, True: 1}
    df['dish_status.is_snr_above_noise_floor'] = df['dish_status.is_snr_above_noise_floor'].map(snr_mapping)

    df.info(verbose=True, show_counts=True)

    return df

def preprocess_data_mikrotik_wlan(df):
    # Infer objects, then convert dtypes
    df = df.infer_objects().convert_dtypes()

    for column in df.columns:
        try:
            df[column] = pd.to_numeric(df[column])
        except (ValueError, TypeError):
            pass  # Skip columns that cannot be converted

    # Rename timestamp from Elastic and keep it for future use
    # It is unreliable if a lot of messages come at the same time due to congestion
    df['timestamp_elastic'] = df.pop('@timestamp')
    
    # It is better to rely on timestamps from the router rather than ElasticSearch
    df['timestamp_router'] = pd.to_datetime(df['date'] + ' ' +  df['time'])
    df.drop(columns=['date', 'time'], inplace=True)
    df.sort_values(by=['timestamp_router'], inplace=True)
    df.set_index('timestamp_router', inplace=True, drop=False)
    #df = df.reset_index()

    # ???

    df.info(verbose=True, show_counts=True)

    return df

def compress_data(df):
    print('df before')
    df.info(verbose=True, show_counts=True, memory_usage='deep')

    # Compress float64/Float64 to float32 and int64/Int64 to int32
    dtype_mapping = {col: 'float32' for col in df.select_dtypes(include=['float64', 'Float64']).columns}
    dtype_mapping.update({col: 'int32' for col in df.select_dtypes(include=['int64', 'Int64']).columns})

    df = df.astype(dtype_mapping)
    
    print('df after')
    df.info(verbose=True, show_counts=True, memory_usage='deep')
    
    return df

