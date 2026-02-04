from marketing_attribution_models import MAM
import pandas as pd
import numpy as np
from collections import defaultdict
import matplotlib.pyplot as plt
from markovclick.models import MarkovClickstream
from markovclick.viz import visualise_markov_chain
import os
import graphviz
import matplotlib as mpl
from pandas.io import gbq
import pandas_gbq
import glob
from pylab import *
import tempfile
import json
from datetime import timedelta
import seaborn as sns
import gc
from datetime import datetime
import re
from google.cloud import bigquery, bigquery_storage

################################################# Data Loading  #########################################

project = "ft-customer-analytics"
location = "EU"
client = bigquery.Client(project=project, location=location)
bqstorage = bigquery_storage.BigQueryReadClient()

################################################# Define variables #################################################

ids = "user_guid"
date = "attribution_visit_start_time"
touchpoint = "touchpoint"
transaction = "converting_visit"

################################################# Define the date range for processing #################################################

end_date = pd.Timestamp.today().date() - pd.DateOffset(days=1)
start_date = end_date - pd.DateOffset(days=14)

start_date = start_date.date()
end_date = end_date.date() 

table_id = "ft-customer-analytics.crg_nniu_attribution.stg_conversion_users_last_15_days_90_days_lookback_table"

################################################# Output DataFrames  #################################################

attribution_df_all_subs_90 = pd.DataFrame()
normalized_removal_effects_all_subs_90 = pd.DataFrame()
markov_transition_matrix_all_subs_90 = pd.DataFrame()
user_df_all_subs_90 = pd.DataFrame()
conversion_window_df_subs = pd.DataFrame()

################################################# Process Data for Each Day #########################################

for current_date in pd.date_range(start_date, end_date, freq="D"):
    # Create SQL query for the current date
    query = f"""
    SELECT * FROM {table_id}
    WHERE DATE(conversion_visit_timestamp) = "{current_date.strftime('%Y-%m-%d')}"
    """
    print(f"Fetching data for {current_date.strftime('%Y-%m-%d')}")


    # Execute the query
    query_job = client.query(query)
    df = query_job.to_dataframe(bqstorage_client=bqstorage)

    if df.empty:
        print(f"No data for {current_date.strftime('%Y-%m-%d')}")
        continue

    ################################################# Data Cleaning  #########################################
    
    df["original_transaction"] = df["converting_visit"]
    sub_df = df[df["conversion_type"] == "Subscription"].drop(columns=["conversion_type"])

    sub_df["user_max_date"] = sub_df.groupby(ids)["conversion_visit_timestamp"].transform("max")
    sub_df[transaction] = 0
    sub_df.loc[(sub_df[date] == sub_df["user_max_date"]) & (sub_df["original_transaction"] == 1), transaction] = 1

    #sub_df.drop(columns=["user_max_date"], inplace=True)
    sub_df = sub_df.sort_values([ids, date], ascending=[False, True])

    sub_df["run_date"] = current_date.date()

    ################################################# Median day calculation #########################################
    
    # Initialize a list to store each user's median time to subscribe
    user_median_days = []

    # Calculate the median days for each user
    for user_guid, user_data in sub_df.groupby(ids):
        # Find the earliest visit where transaction = 0 (initial visit)
        first_visit = user_data[user_data[transaction] == 0][date].min()

        # If no valid first visit is found, skip this user
        if pd.isnull(first_visit):
            continue

        # Find the conversion date (transaction = 1)
        conversion_date = user_data[user_data[transaction] == 1][date].min()

        # Calculate the time difference in days
        if pd.notnull(conversion_date):
            days_to_convert = (conversion_date - first_visit).days
            user_median_days.append(days_to_convert)

    # Calculate the median of the user's conversion times
    if user_median_days:
        median_days_to_subscribe = pd.Series(user_median_days).median()
    else:
        median_days_to_subscribe = None  # If no data, set median as None

    # Add the calculated median days and run date to the output DataFrame
    conversion_window_df_subs = pd.concat(
        [
            conversion_window_df_subs,
            pd.DataFrame(
                {
                    "stage": ["subscriber"],
                    "median_days": [median_days_to_subscribe],
                    "run_date": [current_date.date()],
                }
            ),
        ],
        ignore_index=True,
    )


    ################################################# MAM Initialization #########################################

    try:
        # Initialize the MAM class
        attributions = MAM(
            sub_df,
            group_channels=True,
            channels_colname=touchpoint,
            journey_with_conv_colname=transaction,
            group_channels_by_id_list=[ids],
            group_timestamp_colname=date,
            create_journey_id_based_on_conversion=True,
        )

        ################################################# Apply Attribution Models #########################################

        attributions.attribution_last_click()
        attributions.attribution_first_click()
        attributions.attribution_position_based(
            list_positions_first_middle_last=[0.3, 0.3, 0.4]
        )
        attributions.attribution_time_decay(
            decay_over_time=0.6, frequency=7
        )  # Frequency in hours
        attribution_markov = attributions.attribution_markov(
            transition_to_same_state=False
        )

        ################################################# Process Results #########################################

        # User-level attribution data
        user_df_temp = attributions.as_pd_dataframe()
        user_df_temp["num_touchpoints"] = (
            user_df_temp["channels_agg"].str.split(" > ").apply(len)
        )
        user_df_temp["run_date"] = current_date.date()

        # Extract user_guid from journey_id
        user_df_temp['user_guid'] = user_df_temp['journey_id'].str.extract(r'id:(.*)_J:0')[0]

        # Prepare df for merging
        df['conversion_visit_timestamp_date'] = df['conversion_visit_timestamp'].dt.date

        product_arrangement_df = df[df['conversion_type'] == 'Subscription'][[
        'user_guid',
        'conversion_visit_timestamp_date',
        'product_arrangement_id',
        'is_app_conversion',
        'product_type',
        'user_registration_source',
         ]].drop_duplicates(subset=['user_guid', 'conversion_visit_timestamp_date'], keep='first')

        # Merge user_df_temp with product_arrangement_df
        user_df_temp = user_df_temp.merge(
            product_arrangement_df,
            left_on=['user_guid', 'run_date'],
            right_on=['user_guid', 'conversion_visit_timestamp_date'],
            how='left'
        )

        # Drop 'conversion_visit_timestamp_date' column after merge
        user_df_temp.drop(columns=['conversion_visit_timestamp_date'], inplace=True)

        # Now concatenate user_df_temp into user_df_all_subs_90
        user_df_all_subs_90 = pd.concat(
            [user_df_all_subs_90, user_df_temp], ignore_index=True
        )

        # Proceed with processing markov_transition_matrix and other dataframes as before
        # Markov transition matrix
        markov_transition_matrix = attribution_markov[2].round(3)
        markov_transition_matrix = markov_transition_matrix.rename(
            index=lambda x: x.replace("(inicio)", "(start)"),
            columns=lambda x: x.replace("(inicio)", "(start)"),
        )
        markov_transition_matrix.reset_index(inplace=True)
        markov_transition_matrix = pd.melt(
            markov_transition_matrix,
            id_vars="index",
            var_name="destination",
            value_name="probability",
        )
        markov_transition_matrix.columns = ["source", "destination", "probability"]
        markov_transition_matrix["run_date"] = current_date.date()
        markov_transition_matrix_all_subs_90 = pd.concat(
            [markov_transition_matrix_all_subs_90, markov_transition_matrix],
            ignore_index=True,
        )

        # Removal effects
        removal_effect_matrix = attribution_markov[3].round(3)
        channels = removal_effect_matrix.index
        removal_effect_values = removal_effect_matrix[["removal_effect"]]
        normalized_values = (removal_effect_values / removal_effect_values.sum()) * 100
        normalized_removal_effects = pd.DataFrame(
            normalized_values, index=channels, columns=["removal_effect"]
        )
        normalized_removal_effects["run_date"] = current_date.date()
        normalized_removal_effects["removal_effect_raw"] = (
            removal_effect_values.values.flatten()
        )
        normalized_removal_effects.reset_index(inplace=True)
        normalized_removal_effects.rename(columns={"index": "channel"}, inplace=True)
        normalized_removal_effects_all_subs_90 = pd.concat(
            [normalized_removal_effects_all_subs_90, normalized_removal_effects],
            ignore_index=True,
        )

        # Attribution by channels and models
        attribution_df = attributions.group_by_channels_models
        attribution_df["run_date"] = current_date.date()
        attribution_df.columns = attribution_df.columns.str.replace(
            ".", "_", regex=False
        ).str.replace(" ", "_", regex=False)
        attribution_df_all_subs_90 = pd.concat(
            [attribution_df_all_subs_90, attribution_df], ignore_index=True
        )

        print(f"Processed data for {current_date.strftime('%Y-%m-%d')}")

    except Exception as e:
        print(
            f"An error occurred for the date {current_date.strftime('%Y-%m-%d')}: {e}"
        )
        continue

################################################# Finalize Results #########################################

attribution_df_all_subs_90["conversion_window"] = 90
normalized_removal_effects_all_subs_90["conversion_window"] = 90
markov_transition_matrix_all_subs_90["conversion_window"] = 90
user_df_all_subs_90["conversion_window"] = 90

attribution_df_all_subs_90["conversion_type"] = "Subscription"
normalized_removal_effects_all_subs_90["conversion_type"] = "Subscription"
markov_transition_matrix_all_subs_90["conversion_type"] = "Subscription"
user_df_all_subs_90["conversion_type"] = "Subscription"

################################################################################################## Sub 60 days ##########################################################################################

table_id = "ft-customer-analytics.crg_nniu_attribution.stg_conversion_users_last_15_days_60_days_lookback_table"

attribution_df_all_subs_60 = pd.DataFrame()
normalized_removal_effects_all_subs_60 = pd.DataFrame()
markov_transition_matrix_all_subs_60 = pd.DataFrame()
user_df_all_subs_60 = pd.DataFrame()

for current_date in pd.date_range(start_date, end_date, freq="D"):
    # Create SQL query for the current date
    query = f"""
    SELECT * FROM {table_id}
    WHERE DATE(conversion_visit_timestamp) = "{current_date.strftime('%Y-%m-%d')}"
    """
    print(f"Fetching data for {current_date.strftime('%Y-%m-%d')}")


    # Execute the query
    query_job = client.query(query)
    df = query_job.to_dataframe(bqstorage_client=bqstorage)

    if df.empty:
        print(f"No data for {current_date.strftime('%Y-%m-%d')}")
        continue
    
    ################################################# Data Cleaning  #########################################
    
    df["original_transaction"] = df["converting_visit"]
    sub_df = df[df["conversion_type"] == "Subscription"].drop(columns=["conversion_type"])
    
    sub_df["user_max_date"] = sub_df.groupby(ids)[date].transform("max")
    sub_df[transaction] = 0
    sub_df.loc[(sub_df[date] == sub_df["user_max_date"]) & (sub_df["original_transaction"] == 1), transaction] = 1
    sub_df.drop(columns=["user_max_date"], inplace=True)
    sub_df = sub_df.sort_values([ids, date], ascending=[False, True])
    
    sub_df["run_date"] = current_date.date()
    
    ################################################# MAM Initialization #########################################
    
    try:
        # Initialize the MAM class
        attributions = MAM(
            sub_df,
            group_channels=True,
            channels_colname=touchpoint,
            journey_with_conv_colname=transaction,
            group_channels_by_id_list=[ids],
            group_timestamp_colname=date,
            create_journey_id_based_on_conversion=True,
        )
    
        ################################################# Apply Attribution Models #########################################
    
        attributions.attribution_last_click()
        attributions.attribution_first_click()
        attributions.attribution_position_based(
            list_positions_first_middle_last=[0.3, 0.3, 0.4]
        )
        attributions.attribution_time_decay(
            decay_over_time=0.6, frequency=7
        )  # Frequency in hours
        attribution_markov = attributions.attribution_markov(
            transition_to_same_state=False
        )
    
        ################################################# Process Results #########################################
    
        # User-level attribution data
        user_df_temp = attributions.as_pd_dataframe()
        user_df_temp["num_touchpoints"] = (
            user_df_temp["channels_agg"].str.split(" > ").apply(len)
        )
        user_df_temp["run_date"] = current_date.date()
    
        # Extract user_guid from journey_id
        user_df_temp['user_guid'] = user_df_temp['journey_id'].str.extract(r'id:(.*)_J:0')[0]
    
        # Prepare df for merging
        df['conversion_visit_timestamp_date'] = df['conversion_visit_timestamp'].dt.date

        product_arrangement_df = df[df['conversion_type'] == 'Subscription'][[
        'user_guid',
        'conversion_visit_timestamp_date',
        'product_arrangement_id',
        'is_app_conversion',
        'product_type',
        'user_registration_source',
         ]].drop_duplicates(subset=['user_guid', 'conversion_visit_timestamp_date'], keep='first')
    
        # Merge user_df_temp with product_arrangement_df
        user_df_temp = user_df_temp.merge(
            product_arrangement_df,
            left_on=['user_guid', 'run_date'],
            right_on=['user_guid', 'conversion_visit_timestamp_date'],
            how='left'
        )
    
        # Drop 'conversion_visit_timestamp_date' column after merge
        user_df_temp.drop(columns=['conversion_visit_timestamp_date'], inplace=True)
    
        # Now concatenate user_df_temp into user_df_all_subs_60
        user_df_all_subs_60 = pd.concat(
            [user_df_all_subs_60, user_df_temp], ignore_index=True
        )
    
        # Markov transition matrix
        markov_transition_matrix = attribution_markov[2].round(3)
        markov_transition_matrix = markov_transition_matrix.rename(
            index=lambda x: x.replace("(inicio)", "(start)"),
            columns=lambda x: x.replace("(inicio)", "(start)"),
        )
        markov_transition_matrix.reset_index(inplace=True)
        markov_transition_matrix = pd.melt(
            markov_transition_matrix,
            id_vars="index",
            var_name="destination",
            value_name="probability",
        )
        markov_transition_matrix.columns = ["source", "destination", "probability"]
        markov_transition_matrix["run_date"] = current_date.date()
        markov_transition_matrix_all_subs_60 = pd.concat(
            [markov_transition_matrix_all_subs_60, markov_transition_matrix],
            ignore_index=True,
        )
    
        # Removal effects
        removal_effect_matrix = attribution_markov[3].round(3)
        channels = removal_effect_matrix.index
        removal_effect_values = removal_effect_matrix[["removal_effect"]]
        normalized_values = (removal_effect_values / removal_effect_values.sum()) * 100
        normalized_removal_effects = pd.DataFrame(
            normalized_values, index=channels, columns=["removal_effect"]
        )
        normalized_removal_effects["run_date"] = current_date.date()
        normalized_removal_effects["removal_effect_raw"] = (
            removal_effect_values.values.flatten()
        )
        normalized_removal_effects.reset_index(inplace=True)
        normalized_removal_effects.rename(columns={"index": "channel"}, inplace=True)
        normalized_removal_effects_all_subs_60 = pd.concat(
            [normalized_removal_effects_all_subs_60, normalized_removal_effects],
            ignore_index=True,
        )
    
        # Attribution by channels and models
        attribution_df = attributions.group_by_channels_models
        attribution_df["run_date"] = current_date.date()
        attribution_df.columns = attribution_df.columns.str.replace(
            ".", "_", regex=False
        ).str.replace(" ", "_", regex=False)
        attribution_df_all_subs_60 = pd.concat(
            [attribution_df_all_subs_60, attribution_df], ignore_index=True
        )
    
        print(f"Processed data for {current_date.strftime('%Y-%m-%d')}")
    
    except Exception as e:
        print(
            f"An error occurred for the date {current_date.strftime('%Y-%m-%d')}: {e}"
        )
        continue

################################################# Finalize Results #########################################

# Update the conversion window to 60
attribution_df_all_subs_60["conversion_window"] = 60
normalized_removal_effects_all_subs_60["conversion_window"] = 60
markov_transition_matrix_all_subs_60["conversion_window"] = 60
user_df_all_subs_60["conversion_window"] = 60

attribution_df_all_subs_60["conversion_type"] = "Subscription"
normalized_removal_effects_all_subs_60["conversion_type"] = "Subscription"
markov_transition_matrix_all_subs_60["conversion_type"] = "Subscription"
user_df_all_subs_60["conversion_type"] = "Subscription"

################################################################################################## Sub 30 days ##########################################################################################

table_id = "ft-customer-analytics.crg_nniu_attribution.stg_conversion_users_last_15_days_30_days_lookback_table"

attribution_df_all_subs_30 = pd.DataFrame()
normalized_removal_effects_all_subs_30 = pd.DataFrame()
markov_transition_matrix_all_subs_30 = pd.DataFrame()
user_df_all_subs_30 = pd.DataFrame()

for current_date in pd.date_range(start_date, end_date, freq="D"):
    # Create SQL query for the current date
    query = f"""
    SELECT * FROM {table_id}
    WHERE DATE(conversion_visit_timestamp) = "{current_date.strftime('%Y-%m-%d')}"
    """
    print(f"Fetching data for {current_date.strftime('%Y-%m-%d')}")


    # Execute the query
    query_job = client.query(query)
    df = query_job.to_dataframe(bqstorage_client=bqstorage)

    if df.empty:
        print(f"No data for {current_date.strftime('%Y-%m-%d')}")
        continue
    
    ################################################# Data Cleaning  #########################################
    
    df["original_transaction"] = df["converting_visit"]
    sub_df = df[df["conversion_type"] == "Subscription"].drop(columns=["conversion_type"])
    
    sub_df["user_max_date"] = sub_df.groupby(ids)[date].transform("max")
    sub_df[transaction] = 0
    sub_df.loc[(sub_df[date] == sub_df["user_max_date"]) & (sub_df["original_transaction"] == 1), transaction] = 1
    sub_df.drop(columns=["user_max_date"], inplace=True)
    sub_df = sub_df.sort_values([ids, date], ascending=[False, True])
    
    sub_df["run_date"] = current_date.date()
    
    ################################################# MAM Initialization #########################################
    
    try:
        # Initialize the MAM class
        attributions = MAM(
            sub_df,
            group_channels=True,
            channels_colname=touchpoint,
            journey_with_conv_colname=transaction,
            group_channels_by_id_list=[ids],
            group_timestamp_colname=date,
            create_journey_id_based_on_conversion=True,
        )
    
        ################################################# Apply Attribution Models #########################################
    
        attributions.attribution_last_click()
        attributions.attribution_first_click()
        attributions.attribution_position_based(
            list_positions_first_middle_last=[0.3, 0.3, 0.4]
        )
        attributions.attribution_time_decay(
            decay_over_time=0.6, frequency=7
        )  # Frequency in hours
        attribution_markov = attributions.attribution_markov(
            transition_to_same_state=False
        )
    
        ################################################# Process Results #########################################
    
       # User-level attribution data
        user_df_temp = attributions.as_pd_dataframe()
        user_df_temp["num_touchpoints"] = (
            user_df_temp["channels_agg"].str.split(" > ").apply(len)
        )
        user_df_temp["run_date"] = current_date.date()
    
        # Extract user_guid from journey_id
        user_df_temp['user_guid'] = user_df_temp['journey_id'].str.extract(r'id:(.*)_J:0')[0]
    
        # Prepare df for merging
        df['conversion_visit_timestamp_date'] = df['conversion_visit_timestamp'].dt.date
        
        product_arrangement_df = df[df['conversion_type'] == 'Subscription'][[
        'user_guid',
        'conversion_visit_timestamp_date',
        'product_arrangement_id',
        'is_app_conversion',
        'product_type',
        'user_registration_source',
         ]].drop_duplicates(subset=['user_guid', 'conversion_visit_timestamp_date'], keep='first')
    
        # Merge user_df_temp with product_arrangement_df
        user_df_temp = user_df_temp.merge(
            product_arrangement_df,
            left_on=['user_guid', 'run_date'],
            right_on=['user_guid', 'conversion_visit_timestamp_date'],
            how='left'
        )
    
        # Drop 'conversion_visit_timestamp_date' column after merge
        user_df_temp.drop(columns=['conversion_visit_timestamp_date'], inplace=True)
    
        # Now concatenate user_df_temp into user_df_all_subs_30
        user_df_all_subs_30 = pd.concat(
            [user_df_all_subs_30, user_df_temp], ignore_index=True
        )
    
        # Markov transition matrix
        markov_transition_matrix = attribution_markov[2].round(3)
        markov_transition_matrix = markov_transition_matrix.rename(
            index=lambda x: x.replace("(inicio)", "(start)"),
            columns=lambda x: x.replace("(inicio)", "(start)"),
        )
        markov_transition_matrix.reset_index(inplace=True)
        markov_transition_matrix = pd.melt(
            markov_transition_matrix,
            id_vars="index",
            var_name="destination",
            value_name="probability",
        )
        markov_transition_matrix.columns = ["source", "destination", "probability"]
        markov_transition_matrix["run_date"] = current_date.date()
        markov_transition_matrix_all_subs_30 = pd.concat(
            [markov_transition_matrix_all_subs_30, markov_transition_matrix],
            ignore_index=True,
        )
    
        # Removal effects
        removal_effect_matrix = attribution_markov[3].round(3)
        channels = removal_effect_matrix.index
        removal_effect_values = removal_effect_matrix[["removal_effect"]]
        normalized_values = (removal_effect_values / removal_effect_values.sum()) * 100
        normalized_removal_effects = pd.DataFrame(
            normalized_values, index=channels, columns=["removal_effect"]
        )
        normalized_removal_effects["run_date"] = current_date.date()
        normalized_removal_effects["removal_effect_raw"] = (
            removal_effect_values.values.flatten()
        )
        normalized_removal_effects.reset_index(inplace=True)
        normalized_removal_effects.rename(columns={"index": "channel"}, inplace=True)
        normalized_removal_effects_all_subs_30 = pd.concat(
            [normalized_removal_effects_all_subs_30, normalized_removal_effects],
            ignore_index=True,
        )
    
        # Attribution by channels and models
        attribution_df = attributions.group_by_channels_models
        attribution_df["run_date"] = current_date.date()
        attribution_df.columns = attribution_df.columns.str.replace(
            ".", "_", regex=False
        ).str.replace(" ", "_", regex=False)
        attribution_df_all_subs_30 = pd.concat(
            [attribution_df_all_subs_30, attribution_df], ignore_index=True
        )
    
        print(f"Processed data for {current_date.strftime('%Y-%m-%d')}")
    
    except Exception as e:
        print(
            f"An error occurred for the date {current_date.strftime('%Y-%m-%d')}: {e}"
        )
        continue

################################################# Finalize Results #########################################

# Update the conversion window to 60
attribution_df_all_subs_30["conversion_window"] = 30
normalized_removal_effects_all_subs_30["conversion_window"] = 30
markov_transition_matrix_all_subs_30["conversion_window"] = 30
user_df_all_subs_30["conversion_window"] = 30

attribution_df_all_subs_30["conversion_type"] = "Subscription"
normalized_removal_effects_all_subs_30["conversion_type"] = "Subscription"
markov_transition_matrix_all_subs_30["conversion_type"] = "Subscription"
user_df_all_subs_30["conversion_type"] = "Subscription"

############################################################################################## Merge subs ##########################################################################################
suffixes = ['30', '60', '90']

attribution_dfs = [globals()[f'attribution_df_all_subs_{suffix}'] for suffix in suffixes]
attribution_df_all_subs = pd.concat(attribution_dfs, ignore_index=True)

removal_effects_dfs = [globals()[f'normalized_removal_effects_all_subs_{suffix}'] for suffix in suffixes]
normalized_removal_effects_all_subs = pd.concat(removal_effects_dfs, ignore_index=True)

markov_transition_dfs = [globals()[f'markov_transition_matrix_all_subs_{suffix}'] for suffix in suffixes]
markov_transition_matrix_all_subs = pd.concat(markov_transition_dfs, ignore_index=True)

user_dfs = [globals()[f'user_df_all_subs_{suffix}'] for suffix in suffixes]
user_df_all_subs = pd.concat(user_dfs, ignore_index=True)

user_df_all_subs['num_touchpoints'] = user_df_all_subs['channels_agg'].str.split(' > ').apply(len)
user_df_all_subs["conversion_type"] = "Subscription"
markov_transition_matrix_all_subs["conversion_type"] = "Subscription"
normalized_removal_effects_all_subs["conversion_type"] = "Subscription"
attribution_df_all_subs["conversion_type"] = "Subscription"

# del attribution_df_all_subs_30
# del normalized_removal_effects_all_subs_30
# del markov_transition_matrix_all_subs_30
# del user_df_all_subs_30

# del attribution_df_all_subs_60
# del normalized_removal_effects_all_subs_60
# del markov_transition_matrix_all_subs_60
# del user_df_all_subs_60

# del attribution_df_all_subs_90
# del normalized_removal_effects_all_subs_90
# del markov_transition_matrix_all_subs_90
# del user_df_all_subs_90
# gc.collect()


############################################################################################## Trial 90 days ##########################################################################################
table_id = "ft-customer-analytics.crg_nniu_attribution.stg_conversion_users_last_15_days_90_days_lookback_table"

attribution_df_all_trial_90 = pd.DataFrame()
normalized_removal_effects_all_trial_90 = pd.DataFrame()
markov_transition_matrix_all_trial_90 = pd.DataFrame()
user_df_all_trial_90 = pd.DataFrame()
conversion_window_df_trial = pd.DataFrame()

for current_date in pd.date_range(start_date, end_date, freq="D"):
    # Create SQL query for the current date
    query = f"""
    SELECT * FROM {table_id}
    WHERE DATE(conversion_visit_timestamp) = "{current_date.strftime('%Y-%m-%d')}"
    """
    print(f"Fetching data for {current_date.strftime('%Y-%m-%d')}")


    # Execute the query
    query_job = client.query(query)
    df = query_job.to_dataframe(bqstorage_client=bqstorage)

    if df.empty:
        print(f"No data for {current_date.strftime('%Y-%m-%d')}")
        continue
    
    ################################################# Data Cleaning  #########################################
    
    df["original_transaction"] = df["converting_visit"]
    trial_df = df[df["conversion_type"] == "Trial"].drop(columns=["conversion_type"])

    trial_df["user_max_date"] = trial_df.groupby(ids)[date].transform("max")
    trial_df[transaction] = 0
    trial_df.loc[(trial_df[date] == trial_df["user_max_date"]) & (trial_df["original_transaction"] == 1), transaction] = 1
    trial_df.drop(columns=["user_max_date"], inplace=True)
    trial_df = trial_df.sort_values([ids, date], ascending=[False, True])

    trial_df["run_date"] = current_date.date()

    ################################################# Median day calculation #########################################
    
    # Initialize a list to store each user's median time to subscribe
    user_median_days = []

    # Calculate the median days for each user
    for user_guid, user_data in trial_df.groupby(ids):
        # Find the earliest visit where transaction = 0 (initial visit)
        first_visit = user_data[user_data[transaction] == 0][date].min()

        # If no valid first visit is found, skip this user
        if pd.isnull(first_visit):
            continue

        # Find the conversion date (transaction = 1)
        conversion_date = user_data[user_data[transaction] == 1][date].min()

        # Calculate the time difference in days
        if pd.notnull(conversion_date):
            days_to_convert = (conversion_date - first_visit).days
            user_median_days.append(days_to_convert)

    # Calculate the median of the user's conversion times
    if user_median_days:
        median_days_to_subscribe = pd.Series(user_median_days).median()
    else:
        median_days_to_subscribe = None  # If no data, set median as None

    # Add the calculated median days and run date to the output DataFrame
    conversion_window_df_trial = pd.concat(
        [
            conversion_window_df_trial,
            pd.DataFrame(
                {
                    "stage": ["trial"],
                    "median_days": [median_days_to_subscribe],
                    "run_date": [current_date.date()],
                }
            ),
        ],
        ignore_index=True,
    )
    
    ################################################# MAM Initialization #########################################
    
    try:
        # Initialize the MAM class
        attributions = MAM(
            trial_df,
            group_channels=True,
            channels_colname=touchpoint,
            journey_with_conv_colname=transaction,
            group_channels_by_id_list=[ids],
            group_timestamp_colname=date,
            create_journey_id_based_on_conversion=True,
        )

        ################################################# Apply Attribution Models #########################################

        attributions.attribution_last_click()
        attributions.attribution_first_click()
        attributions.attribution_position_based(
            list_positions_first_middle_last=[0.3, 0.3, 0.4]
        )
        attributions.attribution_time_decay(
            decay_over_time=0.6, frequency=7
        )  # Frequency in hours
        attribution_markov = attributions.attribution_markov(
            transition_to_same_state=False
        )

        ################################################# Process Results #########################################

        # User-level attribution data
        user_df_temp = attributions.as_pd_dataframe()
        user_df_temp["num_touchpoints"] = (
            user_df_temp["channels_agg"].str.split(" > ").apply(len)
        )
        user_df_temp["run_date"] = current_date.date()

        # Extract user_guid from journey_id
        user_df_temp['user_guid'] = user_df_temp['journey_id'].str.extract(r'id:(.*)_J:0')[0]

        # Prepare df for merging
        df['conversion_visit_timestamp_date'] = df['conversion_visit_timestamp'].dt.date
        product_arrangement_df = df[df['conversion_type'] == 'Trial'][[
        'user_guid',
        'conversion_visit_timestamp_date',
        'product_arrangement_id',
        'is_app_conversion',
        'product_type',
        'user_registration_source',
         ]].drop_duplicates(subset=['user_guid', 'conversion_visit_timestamp_date'], keep='first')

        # Merge user_df_temp with product_arrangement_df
        user_df_temp = user_df_temp.merge(
            product_arrangement_df,
            left_on=['user_guid', 'run_date'],
            right_on=['user_guid', 'conversion_visit_timestamp_date'],
            how='left'
        )

        # Drop 'conversion_visit_timestamp_date' column after merge
        user_df_temp.drop(columns=['conversion_visit_timestamp_date'], inplace=True)

        # Now concatenate user_df_temp into user_df_all_trial_90
        user_df_all_trial_90 = pd.concat(
            [user_df_all_trial_90, user_df_temp], ignore_index=True
        )

        # Markov transition matrix
        markov_transition_matrix = attribution_markov[2].round(3)
        markov_transition_matrix = markov_transition_matrix.rename(
            index=lambda x: x.replace("(inicio)", "(start)"),
            columns=lambda x: x.replace("(inicio)", "(start)"),
        )
        markov_transition_matrix.reset_index(inplace=True)
        markov_transition_matrix = pd.melt(
            markov_transition_matrix,
            id_vars="index",
            var_name="destination",
            value_name="probability",
        )
        markov_transition_matrix.columns = ["source", "destination", "probability"]
        markov_transition_matrix["run_date"] = current_date.date()
        markov_transition_matrix_all_trial_90 = pd.concat(
            [markov_transition_matrix_all_trial_90, markov_transition_matrix],
            ignore_index=True,
        )

        # Removal effects
        removal_effect_matrix = attribution_markov[3].round(3)
        channels = removal_effect_matrix.index
        removal_effect_values = removal_effect_matrix[["removal_effect"]]
        normalized_values = (removal_effect_values / removal_effect_values.sum()) * 100
        normalized_removal_effects = pd.DataFrame(
            normalized_values, index=channels, columns=["removal_effect"]
        )
        normalized_removal_effects["run_date"] = current_date.date()
        normalized_removal_effects["removal_effect_raw"] = (
            removal_effect_values.values.flatten()
        )
        normalized_removal_effects.reset_index(inplace=True)
        normalized_removal_effects.rename(columns={"index": "channel"}, inplace=True)
        normalized_removal_effects_all_trial_90 = pd.concat(
            [normalized_removal_effects_all_trial_90, normalized_removal_effects],
            ignore_index=True,
        )

        # Attribution by channels and models
        attribution_df = attributions.group_by_channels_models
        attribution_df["run_date"] = current_date.date()
        attribution_df.columns = attribution_df.columns.str.replace(
            ".", "_", regex=False
        ).str.replace(" ", "_", regex=False)
        attribution_df_all_trial_90 = pd.concat(
            [attribution_df_all_trial_90, attribution_df], ignore_index=True
        )

        print(f"Processed data for {current_date.strftime('%Y-%m-%d')}")

    except Exception as e:
        print(
            f"An error occurred for the date {current_date.strftime('%Y-%m-%d')}: {e}"
        )
        continue

################################################# Finalize Results #########################################

attribution_df_all_trial_90["conversion_window"] = 90
normalized_removal_effects_all_trial_90["conversion_window"] = 90
markov_transition_matrix_all_trial_90["conversion_window"] = 90
user_df_all_trial_90["conversion_window"] = 90

attribution_df_all_trial_90["conversion_type"] = "Trial"
normalized_removal_effects_all_trial_90["conversion_type"] = "Trial"
markov_transition_matrix_all_trial_90["conversion_type"] = "Trial"
user_df_all_trial_90["conversion_type"] = "Trial"

############################################################################################## Trial 60 days ##########################################################################################
table_id = "ft-customer-analytics.crg_nniu_attribution.stg_conversion_users_last_15_days_60_days_lookback_table"

attribution_df_all_trial_60 = pd.DataFrame()
normalized_removal_effects_all_trial_60 = pd.DataFrame()
markov_transition_matrix_all_trial_60 = pd.DataFrame()
user_df_all_trial_60 = pd.DataFrame()

for current_date in pd.date_range(start_date, end_date, freq="D"):
    # Create SQL query for the current date
    query = f"""
    SELECT * FROM {table_id}
    WHERE DATE(conversion_visit_timestamp) = "{current_date.strftime('%Y-%m-%d')}"
    """
    print(f"Fetching data for {current_date.strftime('%Y-%m-%d')}")


    # Execute the query
    query_job = client.query(query)
    df = query_job.to_dataframe(bqstorage_client=bqstorage)

    if df.empty:
        print(f"No data for {current_date.strftime('%Y-%m-%d')}")
        continue
    
    ################################################# Data Cleaning  #########################################
    
    df["original_transaction"] = df["converting_visit"]
    trial_df = df[df["conversion_type"] == "Trial"].drop(columns=["conversion_type"])

    trial_df["user_max_date"] = trial_df.groupby(ids)[date].transform("max")
    trial_df[transaction] = 0
    trial_df.loc[(trial_df[date] == trial_df["user_max_date"]) & (trial_df["original_transaction"] == 1), transaction] = 1
    trial_df.drop(columns=["user_max_date"], inplace=True)
    trial_df = trial_df.sort_values([ids, date], ascending=[False, True])

    trial_df["run_date"] = current_date.date()
    
    ################################################# MAM Initialization #########################################
    
    try:
        # Initialize the MAM class
        attributions = MAM(
            trial_df,
            group_channels=True,
            channels_colname=touchpoint,
            journey_with_conv_colname=transaction,
            group_channels_by_id_list=[ids],
            group_timestamp_colname=date,
            create_journey_id_based_on_conversion=True,
        )

        ################################################# Apply Attribution Models #########################################

        attributions.attribution_last_click()
        attributions.attribution_first_click()
        attributions.attribution_position_based(
            list_positions_first_middle_last=[0.3, 0.3, 0.4]
        )
        attributions.attribution_time_decay(
            decay_over_time=0.6, frequency=7
        )  # Frequency in hours
        attribution_markov = attributions.attribution_markov(
            transition_to_same_state=False
        )

        ################################################# Process Results #########################################

        # User-level attribution data
        user_df_temp = attributions.as_pd_dataframe()
        user_df_temp["num_touchpoints"] = (
            user_df_temp["channels_agg"].str.split(" > ").apply(len)
        )
        user_df_temp["run_date"] = current_date.date()

        # Extract user_guid from journey_id
        user_df_temp['user_guid'] = user_df_temp['journey_id'].str.extract(r'id:(.*)_J:0')[0]

        # Prepare df for merging
        df['conversion_visit_timestamp_date'] = df['conversion_visit_timestamp'].dt.date

        product_arrangement_df = df[df['conversion_type'] == 'Trial'][[
        'user_guid',
        'conversion_visit_timestamp_date',
        'product_arrangement_id',
        'is_app_conversion',
        'product_type',
        'user_registration_source',
         ]].drop_duplicates(subset=['user_guid', 'conversion_visit_timestamp_date'], keep='first')

        # Merge user_df_temp with product_arrangement_df
        user_df_temp = user_df_temp.merge(
            product_arrangement_df,
            left_on=['user_guid', 'run_date'],
            right_on=['user_guid', 'conversion_visit_timestamp_date'],
            how='left'
        )

        # Drop 'conversion_visit_timestamp_date' column after merge
        user_df_temp.drop(columns=['conversion_visit_timestamp_date'], inplace=True)

        # Now concatenate user_df_temp into user_df_all_trial_60
        user_df_all_trial_60 = pd.concat(
            [user_df_all_trial_60, user_df_temp], ignore_index=True
        )

        # Markov transition matrix
        markov_transition_matrix = attribution_markov[2].round(3)
        markov_transition_matrix = markov_transition_matrix.rename(
            index=lambda x: x.replace("(inicio)", "(start)"),
            columns=lambda x: x.replace("(inicio)", "(start)"),
        )
        markov_transition_matrix.reset_index(inplace=True)
        markov_transition_matrix = pd.melt(
            markov_transition_matrix,
            id_vars="index",
            var_name="destination",
            value_name="probability",
        )
        markov_transition_matrix.columns = ["source", "destination", "probability"]
        markov_transition_matrix["run_date"] = current_date.date()
        markov_transition_matrix_all_trial_60 = pd.concat(
            [markov_transition_matrix_all_trial_60, markov_transition_matrix],
            ignore_index=True,
        )

        # Removal effects
        removal_effect_matrix = attribution_markov[3].round(3)
        channels = removal_effect_matrix.index
        removal_effect_values = removal_effect_matrix[["removal_effect"]]
        normalized_values = (removal_effect_values / removal_effect_values.sum()) * 100
        normalized_removal_effects = pd.DataFrame(
            normalized_values, index=channels, columns=["removal_effect"]
        )
        normalized_removal_effects["run_date"] = current_date.date()
        normalized_removal_effects["removal_effect_raw"] = (
            removal_effect_values.values.flatten()
        )
        normalized_removal_effects.reset_index(inplace=True)
        normalized_removal_effects.rename(columns={"index": "channel"}, inplace=True)
        normalized_removal_effects_all_trial_60 = pd.concat(
            [normalized_removal_effects_all_trial_60, normalized_removal_effects],
            ignore_index=True,
        )

        # Attribution by channels and models
        attribution_df = attributions.group_by_channels_models
        attribution_df["run_date"] = current_date.date()
        attribution_df.columns = attribution_df.columns.str.replace(
            ".", "_", regex=False
        ).str.replace(" ", "_", regex=False)
        attribution_df_all_trial_60 = pd.concat(
            [attribution_df_all_trial_60, attribution_df], ignore_index=True
        )

        print(f"Processed data for {current_date.strftime('%Y-%m-%d')}")

    except Exception as e:
        print(
            f"An error occurred for the date {current_date.strftime('%Y-%m-%d')}: {e}"
        )
        continue

################################################# Finalize Results #########################################

attribution_df_all_trial_60["conversion_window"] = 60
normalized_removal_effects_all_trial_60["conversion_window"] = 60
markov_transition_matrix_all_trial_60["conversion_window"] = 60
user_df_all_trial_60["conversion_window"] = 60

attribution_df_all_trial_60["conversion_type"] = "Trial"
normalized_removal_effects_all_trial_60["conversion_type"] = "Trial"
markov_transition_matrix_all_trial_60["conversion_type"] = "Trial"
user_df_all_trial_60["conversion_type"] = "Trial"

############################################################################################## Trial 30 days ##########################################################################################

table_id = "ft-customer-analytics.crg_nniu_attribution.stg_conversion_users_last_15_days_30_days_lookback_table"

attribution_df_all_trial_30 = pd.DataFrame()
normalized_removal_effects_all_trial_30 = pd.DataFrame()
markov_transition_matrix_all_trial_30 = pd.DataFrame()
user_df_all_trial_30 = pd.DataFrame()

for current_date in pd.date_range(start_date, end_date, freq="D"):
    # Create SQL query for the current date
    query = f"""
    SELECT * FROM {table_id}
    WHERE DATE(conversion_visit_timestamp) = "{current_date.strftime('%Y-%m-%d')}"
    """
    print(f"Fetching data for {current_date.strftime('%Y-%m-%d')}")


    # Execute the query
    query_job = client.query(query)
    df = query_job.to_dataframe(bqstorage_client=bqstorage)

    if df.empty:
        print(f"No data for {current_date.strftime('%Y-%m-%d')}")
        continue
    
    ################################################# Data Cleaning  #########################################
    
    df["original_transaction"] = df["converting_visit"]
    trial_df = df[df["conversion_type"] == "Trial"].drop(columns=["conversion_type"])

    trial_df["user_max_date"] = trial_df.groupby(ids)[date].transform("max")
    trial_df[transaction] = 0
    trial_df.loc[(trial_df[date] == trial_df["user_max_date"]) & (trial_df["original_transaction"] == 1), transaction] = 1
    trial_df.drop(columns=["user_max_date"], inplace=True)
    trial_df = trial_df.sort_values([ids, date], ascending=[False, True])

    trial_df["run_date"] = current_date.date()

    ################################################# MAM Initialization #########################################
    
    try:
        # Initialize the MAM class
        attributions = MAM(
            trial_df,
            group_channels=True,
            channels_colname=touchpoint,
            journey_with_conv_colname=transaction,
            group_channels_by_id_list=[ids],
            group_timestamp_colname=date,
            create_journey_id_based_on_conversion=True,
        )

        ################################################# Apply Attribution Models #########################################

        attributions.attribution_last_click()
        attributions.attribution_first_click()
        attributions.attribution_position_based(
            list_positions_first_middle_last=[0.3, 0.3, 0.4]
        )
        attributions.attribution_time_decay(
            decay_over_time=0.6, frequency=7
        )  # Frequency in hours
        attribution_markov = attributions.attribution_markov(
            transition_to_same_state=False
        )

        ################################################# Process Results #########################################

        # User-level attribution data
        user_df_temp = attributions.as_pd_dataframe()
        user_df_temp["num_touchpoints"] = (
            user_df_temp["channels_agg"].str.split(" > ").apply(len)
        )
        user_df_temp["run_date"] = current_date.date()

        # Extract user_guid from journey_id
        user_df_temp['user_guid'] = user_df_temp['journey_id'].str.extract(r'id:(.*)_J:0')[0]

        # Prepare df for merging
        df['conversion_visit_timestamp_date'] = df['conversion_visit_timestamp'].dt.date

        product_arrangement_df = df[df['conversion_type'] == 'Trial'][[
        'user_guid',
        'conversion_visit_timestamp_date',
        'product_arrangement_id',
        'is_app_conversion',
        'product_type',
        'user_registration_source',
         ]].drop_duplicates(subset=['user_guid', 'conversion_visit_timestamp_date'], keep='first')
    
        # Merge user_df_temp with product_arrangement_df
        user_df_temp = user_df_temp.merge(
            product_arrangement_df,
            left_on=['user_guid', 'run_date'],
            right_on=['user_guid', 'conversion_visit_timestamp_date'],
            how='left'
        )
    
        # Drop 'conversion_visit_timestamp_date' column after merge
        user_df_temp.drop(columns=['conversion_visit_timestamp_date'], inplace=True)
    
        # Now concatenate user_df_temp into user_df_all_trial_30
        user_df_all_trial_30 = pd.concat(
            [user_df_all_trial_30, user_df_temp], ignore_index=True
        )

        # Markov transition matrix
        markov_transition_matrix = attribution_markov[2].round(3)
        markov_transition_matrix = markov_transition_matrix.rename(
            index=lambda x: x.replace("(inicio)", "(start)"),
            columns=lambda x: x.replace("(inicio)", "(start)"),
        )
        markov_transition_matrix.reset_index(inplace=True)
        markov_transition_matrix = pd.melt(
            markov_transition_matrix,
            id_vars="index",
            var_name="destination",
            value_name="probability",
        )
        markov_transition_matrix.columns = ["source", "destination", "probability"]
        markov_transition_matrix["run_date"] = current_date.date()
        markov_transition_matrix_all_trial_30 = pd.concat(
            [markov_transition_matrix_all_trial_30, markov_transition_matrix],
            ignore_index=True,
        )

        # Removal effects
        removal_effect_matrix = attribution_markov[3].round(3)
        channels = removal_effect_matrix.index
        removal_effect_values = removal_effect_matrix[["removal_effect"]]
        normalized_values = (removal_effect_values / removal_effect_values.sum()) * 100
        normalized_removal_effects = pd.DataFrame(
            normalized_values, index=channels, columns=["removal_effect"]
        )
        normalized_removal_effects["run_date"] = current_date.date()
        normalized_removal_effects["removal_effect_raw"] = (
            removal_effect_values.values.flatten()
        )
        normalized_removal_effects.reset_index(inplace=True)
        normalized_removal_effects.rename(columns={"index": "channel"}, inplace=True)
        normalized_removal_effects_all_trial_30 = pd.concat(
            [normalized_removal_effects_all_trial_30, normalized_removal_effects],
            ignore_index=True,
        )

        # Attribution by channels and models
        attribution_df = attributions.group_by_channels_models
        attribution_df["run_date"] = current_date.date()
        attribution_df.columns = attribution_df.columns.str.replace(
            ".", "_", regex=False
        ).str.replace(" ", "_", regex=False)
        attribution_df_all_trial_30 = pd.concat(
            [attribution_df_all_trial_30, attribution_df], ignore_index=True
        )

        print(f"Processed data for {current_date.strftime('%Y-%m-%d')}")

    except Exception as e:
        print(
            f"An error occurred for the date {current_date.strftime('%Y-%m-%d')}: {e}"
        )
        continue

################################################# Finalize Results #########################################

attribution_df_all_trial_30["conversion_window"] = 30
normalized_removal_effects_all_trial_30["conversion_window"] = 30
markov_transition_matrix_all_trial_30["conversion_window"] = 30
user_df_all_trial_30["conversion_window"] = 30

attribution_df_all_trial_30["conversion_type"] = "Trial"
normalized_removal_effects_all_trial_30["conversion_type"] = "Trial"
markov_transition_matrix_all_trial_30["conversion_type"] = "Trial"
user_df_all_trial_30["conversion_type"] = "Trial"




############################################################################################## Merge trials ##########################################################################################
suffixes = ["30", "60", "90"]

attribution_dfs_trial = [
    globals()[f"attribution_df_all_trial_{suffix}"] for suffix in suffixes
]
attribution_df_all_trial = pd.concat(attribution_dfs_trial, ignore_index=True)

removal_effects_dfs_trial = [
    globals()[f"normalized_removal_effects_all_trial_{suffix}"] for suffix in suffixes
]
normalized_removal_effects_all_trial = pd.concat(
    removal_effects_dfs_trial, ignore_index=True
)

markov_transition_dfs_trial = [
    globals()[f"markov_transition_matrix_all_trial_{suffix}"] for suffix in suffixes
]
markov_transition_matrix_all_trial = pd.concat(
    markov_transition_dfs_trial, ignore_index=True
)

user_dfs_trial = [globals()[f"user_df_all_trial_{suffix}"] for suffix in suffixes]
user_df_all_trial = pd.concat(user_dfs_trial, ignore_index=True)

user_df_all_trial["num_touchpoints"] = (
    user_df_all_trial["channels_agg"].str.split(" > ").apply(len)
)
user_df_all_trial["conversion_type"] = "Trial"
markov_transition_matrix_all_trial["conversion_type"] = "Trial"
normalized_removal_effects_all_trial["conversion_type"] = "Trial"
attribution_df_all_trial["conversion_type"] = "Trial"

############################################################################################## Regis 90 days ##########################################################################################

table_id = "ft-customer-analytics.crg_nniu_attribution.stg_conversion_users_last_15_days_90_days_lookback_table"

attribution_df_all_regis_90 = pd.DataFrame()
normalized_removal_effects_all_regis_90 = pd.DataFrame()
markov_transition_matrix_all_regis_90 = pd.DataFrame()
user_df_all_regis_90 = pd.DataFrame()
conversion_window_df_regis = pd.DataFrame()

################################################# Process Data for Each Day #########################################

for current_date in pd.date_range(start_date, end_date, freq="D"):
    # Create SQL query for the current date
    query = f"""
    SELECT * FROM {table_id}
    WHERE DATE(conversion_visit_timestamp) = "{current_date.strftime('%Y-%m-%d')}"
    """
    print(f"Fetching data for {current_date.strftime('%Y-%m-%d')}")


    # Execute the query
    query_job = client.query(query)
    df = query_job.to_dataframe(bqstorage_client=bqstorage)

    if df.empty:
        print(f"No data for {current_date.strftime('%Y-%m-%d')}")
        continue

    ################################################# Data Cleaning  #########################################
    
    df["original_transaction"] = df["converting_visit"]
    regis_df = df[df["conversion_type"] == "registration"].drop(columns=["conversion_type"])

    regis_df["user_max_date"] = regis_df.groupby(ids)["conversion_visit_timestamp"].transform("max")
    regis_df[transaction] = 0
    regis_df.loc[(regis_df[date] == regis_df["user_max_date"]) & (regis_df["original_transaction"] == 1), transaction] = 1

    regis_df = regis_df.sort_values([ids, date], ascending=[False, True])

    regis_df["run_date"] = current_date.date()

    ################################################# Median day calculation #########################################
    
    # Initialize a list to store each user's median time to register
    user_median_days = []

    # Calculate the median days for each user
    for user_guid, user_data in regis_df.groupby(ids):
        # Find the earliest visit where transaction = 0 (initial visit)
        first_visit = user_data[user_data[transaction] == 0][date].min()

        # If no valid first visit is found, skip this user
        if pd.isnull(first_visit):
            continue

        # Find the conversion date (transaction = 1)
        conversion_date = user_data[user_data[transaction] == 1][date].min()

        # Calculate the time difference in days
        if pd.notnull(conversion_date):
            days_to_convert = (conversion_date - first_visit).days
            user_median_days.append(days_to_convert)

    # Calculate the median of the user's conversion times
    if user_median_days:
        median_days_to_register = pd.Series(user_median_days).median()
    else:
        median_days_to_register = None  # If no data, set median as None

    # Add the calculated median days and run date to the output DataFrame
    conversion_window_df_regis = pd.concat(
        [
            conversion_window_df_regis,
            pd.DataFrame(
                {
                    "stage": ["registration"],
                    "median_days": [median_days_to_register],
                    "run_date": [current_date.date()],
                }
            ),
        ],
        ignore_index=True,
    )


    ################################################# MAM Initialization #########################################

    try:
        # Initialize the MAM class
        attributions = MAM(
            regis_df,
            group_channels=True,
            channels_colname=touchpoint,
            journey_with_conv_colname=transaction,
            group_channels_by_id_list=[ids],
            group_timestamp_colname=date,
            create_journey_id_based_on_conversion=True,
        )

        ################################################# Apply Attribution Models #########################################

        attributions.attribution_last_click()
        attributions.attribution_first_click()
        attributions.attribution_position_based(
            list_positions_first_middle_last=[0.3, 0.3, 0.4]
        )
        attributions.attribution_time_decay(
            decay_over_time=0.6, frequency=7
        )  # Frequency in hours
        attribution_markov = attributions.attribution_markov(
            transition_to_same_state=False
        )

        ################################################# Process Results #########################################

        # User-level attribution data
        user_df_temp = attributions.as_pd_dataframe()
        user_df_temp["num_touchpoints"] = (
            user_df_temp["channels_agg"].str.split(" > ").apply(len)
        )
        user_df_temp["run_date"] = current_date.date()

        # Extract user_guid from journey_id
        user_df_temp['user_guid'] = user_df_temp['journey_id'].str.extract(r'id:(.*)_J:0')[0]

        # Prepare df for merging
        df['conversion_visit_timestamp_date'] = df['conversion_visit_timestamp'].dt.date

        product_arrangement_df = df[df['conversion_type'] == 'registration'][[
        'user_guid',
        'conversion_visit_timestamp_date',
        'product_arrangement_id',
        'is_app_conversion',
        'product_type',
        'user_registration_source',
         ]].drop_duplicates(subset=['user_guid', 'conversion_visit_timestamp_date'], keep='first')

        # Merge user_df_temp with product_arrangement_df
        user_df_temp = user_df_temp.merge(
            product_arrangement_df,
            left_on=['user_guid', 'run_date'],
            right_on=['user_guid', 'conversion_visit_timestamp_date'],
            how='left'
        )

        # Drop 'conversion_visit_timestamp_date' column after merge
        user_df_temp.drop(columns=['conversion_visit_timestamp_date'], inplace=True)

        # Now concatenate user_df_temp into user_df_all_regis_90
        user_df_all_regis_90 = pd.concat(
            [user_df_all_regis_90, user_df_temp], ignore_index=True
        )

        # Proceed with processing markov_transition_matrix and other dataframes as before
        # Markov transition matrix
        markov_transition_matrix = attribution_markov[2].round(3)
        markov_transition_matrix = markov_transition_matrix.rename(
            index=lambda x: x.replace("(inicio)", "(start)"),
            columns=lambda x: x.replace("(inicio)", "(start)"),
        )
        markov_transition_matrix.reset_index(inplace=True)
        markov_transition_matrix = pd.melt(
            markov_transition_matrix,
            id_vars="index",
            var_name="destination",
            value_name="probability",
        )
        markov_transition_matrix.columns = ["source", "destination", "probability"]
        markov_transition_matrix["run_date"] = current_date.date()
        markov_transition_matrix_all_regis_90 = pd.concat(
            [markov_transition_matrix_all_regis_90, markov_transition_matrix],
            ignore_index=True,
        )

        # Removal effects
        removal_effect_matrix = attribution_markov[3].round(3)
        channels = removal_effect_matrix.index
        removal_effect_values = removal_effect_matrix[["removal_effect"]]
        normalized_values = (removal_effect_values / removal_effect_values.sum()) * 100
        normalized_removal_effects = pd.DataFrame(
            normalized_values, index=channels, columns=["removal_effect"]
        )
        normalized_removal_effects["run_date"] = current_date.date()
        normalized_removal_effects["removal_effect_raw"] = (
            removal_effect_values.values.flatten()
        )
        normalized_removal_effects.reset_index(inplace=True)
        normalized_removal_effects.rename(columns={"index": "channel"}, inplace=True)
        normalized_removal_effects_all_regis_90 = pd.concat(
            [normalized_removal_effects_all_regis_90, normalized_removal_effects],
            ignore_index=True,
        )

        # Attribution by channels and models
        attribution_df = attributions.group_by_channels_models
        attribution_df["run_date"] = current_date.date()
        attribution_df.columns = attribution_df.columns.str.replace(
            ".", "_", regex=False
        ).str.replace(" ", "_", regex=False)
        attribution_df_all_regis_90 = pd.concat(
            [attribution_df_all_regis_90, attribution_df], ignore_index=True
        )

        print(f"Processed data for {current_date.strftime('%Y-%m-%d')}")

    except Exception as e:
        print(
            f"An error occurred for the date {current_date.strftime('%Y-%m-%d')}: {e}"
        )
        continue

################################################# Finalize Results #########################################

attribution_df_all_regis_90["conversion_window"] = 90
normalized_removal_effects_all_regis_90["conversion_window"] = 90
markov_transition_matrix_all_regis_90["conversion_window"] = 90
user_df_all_regis_90["conversion_window"] = 90

attribution_df_all_regis_90["conversion_type"] = "registration"
normalized_removal_effects_all_regis_90["conversion_type"] = "registration"
markov_transition_matrix_all_regis_90["conversion_type"] = "registration"
user_df_all_regis_90["conversion_type"] = "registration"



############################################################################################## Regis 60 days ##########################################################################################
table_id = "ft-customer-analytics.crg_nniu_attribution.stg_conversion_users_last_15_days_60_days_lookback_table"

attribution_df_all_regis_60 = pd.DataFrame()
normalized_removal_effects_all_regis_60 = pd.DataFrame()
markov_transition_matrix_all_regis_60 = pd.DataFrame()
user_df_all_regis_60 = pd.DataFrame()
conversion_window_df_regis = pd.DataFrame()

################################################# Process Data for Each Day #########################################

for current_date in pd.date_range(start_date, end_date, freq="D"):
    # Create SQL query for the current date
    query = f"""
    SELECT * FROM {table_id}
    WHERE DATE(conversion_visit_timestamp) = "{current_date.strftime('%Y-%m-%d')}"
    """
    print(f"Fetching data for {current_date.strftime('%Y-%m-%d')}")


    # Execute the query
    query_job = client.query(query)
    df = query_job.to_dataframe(bqstorage_client=bqstorage)

    if df.empty:
        print(f"No data for {current_date.strftime('%Y-%m-%d')}")
        continue

    ################################################# Data Cleaning  #########################################
    
    df["original_transaction"] = df["converting_visit"]
    regis_df = df[df["conversion_type"] == "registration"].drop(columns=["conversion_type"])

    regis_df["user_max_date"] = regis_df.groupby(ids)["conversion_visit_timestamp"].transform("max")
    regis_df[transaction] = 0
    regis_df.loc[(regis_df[date] == regis_df["user_max_date"]) & (regis_df["original_transaction"] == 1), transaction] = 1

    regis_df = regis_df.sort_values([ids, date], ascending=[False, True])

    regis_df["run_date"] = current_date.date()

    ################################################# Median day calculation #########################################
    
    # Initialize a list to store each user's median time to register
    user_median_days = []

    # Calculate the median days for each user
    for user_guid, user_data in regis_df.groupby(ids):
        # Find the earliest visit where transaction = 0 (initial visit)
        first_visit = user_data[user_data[transaction] == 0][date].min()

        # If no valid first visit is found, skip this user
        if pd.isnull(first_visit):
            continue

        # Find the conversion date (transaction = 1)
        conversion_date = user_data[user_data[transaction] == 1][date].min()

        # Calculate the time difference in days
        if pd.notnull(conversion_date):
            days_to_convert = (conversion_date - first_visit).days
            user_median_days.append(days_to_convert)

    # Calculate the median of the user's conversion times
    if user_median_days:
        median_days_to_register = pd.Series(user_median_days).median()
    else:
        median_days_to_register = None  # If no data, set median as None

    # Add the calculated median days and run date to the output DataFrame
    conversion_window_df_regis = pd.concat(
        [
            conversion_window_df_regis,
            pd.DataFrame(
                {
                    "stage": ["registration"],
                    "median_days": [median_days_to_register],
                    "run_date": [current_date.date()],
                }
            ),
        ],
        ignore_index=True,
    )


    ################################################# MAM Initialization #########################################

    try:
        # Initialize the MAM class
        attributions = MAM(
            regis_df,
            group_channels=True,
            channels_colname=touchpoint,
            journey_with_conv_colname=transaction,
            group_channels_by_id_list=[ids],
            group_timestamp_colname=date,
            create_journey_id_based_on_conversion=True,
        )

        ################################################# Apply Attribution Models #########################################

        attributions.attribution_last_click()
        attributions.attribution_first_click()
        attributions.attribution_position_based(
            list_positions_first_middle_last=[0.3, 0.3, 0.4]
        )
        attributions.attribution_time_decay(
            decay_over_time=0.6, frequency=7
        )  # Frequency in hours
        attribution_markov = attributions.attribution_markov(
            transition_to_same_state=False
        )

        ################################################# Process Results #########################################

        # User-level attribution data
        user_df_temp = attributions.as_pd_dataframe()
        user_df_temp["num_touchpoints"] = (
            user_df_temp["channels_agg"].str.split(" > ").apply(len)
        )
        user_df_temp["run_date"] = current_date.date()

        # Extract user_guid from journey_id
        user_df_temp['user_guid'] = user_df_temp['journey_id'].str.extract(r'id:(.*)_J:0')[0]

        # Prepare df for merging
        df['conversion_visit_timestamp_date'] = df['conversion_visit_timestamp'].dt.date

        product_arrangement_df = df[df['conversion_type'] == 'registration'][[
        'user_guid',
        'conversion_visit_timestamp_date',
        'product_arrangement_id',
        'is_app_conversion',
        'product_type',
        'user_registration_source',
         ]].drop_duplicates(subset=['user_guid', 'conversion_visit_timestamp_date'], keep='first')

        # Merge user_df_temp with product_arrangement_df
        user_df_temp = user_df_temp.merge(
            product_arrangement_df,
            left_on=['user_guid', 'run_date'],
            right_on=['user_guid', 'conversion_visit_timestamp_date'],
            how='left'
        )

        # Drop 'conversion_visit_timestamp_date' column after merge
        user_df_temp.drop(columns=['conversion_visit_timestamp_date'], inplace=True)

        # Now concatenate user_df_temp into user_df_all_regis_60
        user_df_all_regis_60 = pd.concat(
            [user_df_all_regis_60, user_df_temp], ignore_index=True
        )

        # Proceed with processing markov_transition_matrix and other dataframes as before
        # Markov transition matrix
        markov_transition_matrix = attribution_markov[2].round(3)
        markov_transition_matrix = markov_transition_matrix.rename(
            index=lambda x: x.replace("(inicio)", "(start)"),
            columns=lambda x: x.replace("(inicio)", "(start)"),
        )
        markov_transition_matrix.reset_index(inplace=True)
        markov_transition_matrix = pd.melt(
            markov_transition_matrix,
            id_vars="index",
            var_name="destination",
            value_name="probability",
        )
        markov_transition_matrix.columns = ["source", "destination", "probability"]
        markov_transition_matrix["run_date"] = current_date.date()
        markov_transition_matrix_all_regis_60 = pd.concat(
            [markov_transition_matrix_all_regis_60, markov_transition_matrix],
            ignore_index=True,
        )

        # Removal effects
        removal_effect_matrix = attribution_markov[3].round(3)
        channels = removal_effect_matrix.index
        removal_effect_values = removal_effect_matrix[["removal_effect"]]
        normalized_values = (removal_effect_values / removal_effect_values.sum()) * 100
        normalized_removal_effects = pd.DataFrame(
            normalized_values, index=channels, columns=["removal_effect"]
        )
        normalized_removal_effects["run_date"] = current_date.date()
        normalized_removal_effects["removal_effect_raw"] = (
            removal_effect_values.values.flatten()
        )
        normalized_removal_effects.reset_index(inplace=True)
        normalized_removal_effects.rename(columns={"index": "channel"}, inplace=True)
        normalized_removal_effects_all_regis_60 = pd.concat(
            [normalized_removal_effects_all_regis_60, normalized_removal_effects],
            ignore_index=True,
        )

        # Attribution by channels and models
        attribution_df = attributions.group_by_channels_models
        attribution_df["run_date"] = current_date.date()
        attribution_df.columns = attribution_df.columns.str.replace(
            ".", "_", regex=False
        ).str.replace(" ", "_", regex=False)
        attribution_df_all_regis_60 = pd.concat(
            [attribution_df_all_regis_60, attribution_df], ignore_index=True
        )

        print(f"Processed data for {current_date.strftime('%Y-%m-%d')}")

    except Exception as e:
        print(
            f"An error occurred for the date {current_date.strftime('%Y-%m-%d')}: {e}"
        )
        continue

################################################# Finalize Results #########################################

attribution_df_all_regis_60["conversion_window"] = 60
normalized_removal_effects_all_regis_60["conversion_window"] = 60
markov_transition_matrix_all_regis_60["conversion_window"] = 60
user_df_all_regis_60["conversion_window"] = 60

attribution_df_all_regis_60["conversion_type"] = "registration"
normalized_removal_effects_all_regis_60["conversion_type"] = "registration"
markov_transition_matrix_all_regis_60["conversion_type"] = "registration"
user_df_all_regis_60["conversion_type"] = "registration"



############################################################################################## Regis 30 days ##########################################################################################
table_id = "ft-customer-analytics.crg_nniu_attribution.stg_conversion_users_last_15_days_30_days_lookback_table"

attribution_df_all_regis_30 = pd.DataFrame()
normalized_removal_effects_all_regis_30 = pd.DataFrame()
markov_transition_matrix_all_regis_30 = pd.DataFrame()
user_df_all_regis_30 = pd.DataFrame()
conversion_window_df_regis = pd.DataFrame()

################################################# Process Data for Each Day #########################################

for current_date in pd.date_range(start_date, end_date, freq="D"):
    # Create SQL query for the current date
    query = f"""
    SELECT * FROM {table_id}
    WHERE DATE(conversion_visit_timestamp) = "{current_date.strftime('%Y-%m-%d')}"
    """
    print(f"Fetching data for {current_date.strftime('%Y-%m-%d')}")


    # Execute the query
    query_job = client.query(query)
    df = query_job.to_dataframe(bqstorage_client=bqstorage)

    if df.empty:
        print(f"No data for {current_date.strftime('%Y-%m-%d')}")
        continue

    ################################################# Data Cleaning  #########################################
    
    df["original_transaction"] = df["converting_visit"]
    regis_df = df[df["conversion_type"] == "registration"].drop(columns=["conversion_type"])

    regis_df["user_max_date"] = regis_df.groupby(ids)["conversion_visit_timestamp"].transform("max")
    regis_df[transaction] = 0
    regis_df.loc[(regis_df[date] == regis_df["user_max_date"]) & (regis_df["original_transaction"] == 1), transaction] = 1

    regis_df = regis_df.sort_values([ids, date], ascending=[False, True])

    regis_df["run_date"] = current_date.date()

    ################################################# Median day calculation #########################################
    
    # Initialize a list to store each user's median time to register
    user_median_days = []

    # Calculate the median days for each user
    for user_guid, user_data in regis_df.groupby(ids):
        # Find the earliest visit where transaction = 0 (initial visit)
        first_visit = user_data[user_data[transaction] == 0][date].min()

        # If no valid first visit is found, skip this user
        if pd.isnull(first_visit):
            continue

        # Find the conversion date (transaction = 1)
        conversion_date = user_data[user_data[transaction] == 1][date].min()

        # Calculate the time difference in days
        if pd.notnull(conversion_date):
            days_to_convert = (conversion_date - first_visit).days
            user_median_days.append(days_to_convert)

    # Calculate the median of the user's conversion times
    if user_median_days:
        median_days_to_register = pd.Series(user_median_days).median()
    else:
        median_days_to_register = None  # If no data, set median as None

    # Add the calculated median days and run date to the output DataFrame
    conversion_window_df_regis = pd.concat(
        [
            conversion_window_df_regis,
            pd.DataFrame(
                {
                    "stage": ["registration"],
                    "median_days": [median_days_to_register],
                    "run_date": [current_date.date()],
                }
            ),
        ],
        ignore_index=True,
    )


    ################################################# MAM Initialization #########################################

    try:
        # Initialize the MAM class
        attributions = MAM(
            regis_df,
            group_channels=True,
            channels_colname=touchpoint,
            journey_with_conv_colname=transaction,
            group_channels_by_id_list=[ids],
            group_timestamp_colname=date,
            create_journey_id_based_on_conversion=True,
        )

        ################################################# Apply Attribution Models #########################################

        attributions.attribution_last_click()
        attributions.attribution_first_click()
        attributions.attribution_position_based(
            list_positions_first_middle_last=[0.3, 0.3, 0.4]
        )
        attributions.attribution_time_decay(
            decay_over_time=0.6, frequency=7
        )  # Frequency in hours
        attribution_markov = attributions.attribution_markov(
            transition_to_same_state=False
        )

        ################################################# Process Results #########################################

        # User-level attribution data
        user_df_temp = attributions.as_pd_dataframe()
        user_df_temp["num_touchpoints"] = (
            user_df_temp["channels_agg"].str.split(" > ").apply(len)
        )
        user_df_temp["run_date"] = current_date.date()

        # Extract user_guid from journey_id
        user_df_temp['user_guid'] = user_df_temp['journey_id'].str.extract(r'id:(.*)_J:0')[0]

        # Prepare df for merging
        df['conversion_visit_timestamp_date'] = df['conversion_visit_timestamp'].dt.date
        
        product_arrangement_df = df[df['conversion_type'] == 'registration'][[
        'user_guid',
        'conversion_visit_timestamp_date',
        'product_arrangement_id',
        'is_app_conversion',
        'product_type',
        'user_registration_source',
         ]].drop_duplicates(subset=['user_guid', 'conversion_visit_timestamp_date'], keep='first')
    
        # Merge user_df_temp with product_arrangement_df
        user_df_temp = user_df_temp.merge(
            product_arrangement_df,
            left_on=['user_guid', 'run_date'],
            right_on=['user_guid', 'conversion_visit_timestamp_date'],
            how='left'
        )
    
        # Drop 'conversion_visit_timestamp_date' column after merge
        user_df_temp.drop(columns=['conversion_visit_timestamp_date'], inplace=True)
    
        # Now concatenate user_df_temp into user_df_all_regis_30
        user_df_all_regis_30 = pd.concat(
            [user_df_all_regis_30, user_df_temp], ignore_index=True
        )

        # Proceed with processing markov_transition_matrix and other dataframes as before
        # Markov transition matrix
        markov_transition_matrix = attribution_markov[2].round(3)
        markov_transition_matrix = markov_transition_matrix.rename(
            index=lambda x: x.replace("(inicio)", "(start)"),
            columns=lambda x: x.replace("(inicio)", "(start)"),
        )
        markov_transition_matrix.reset_index(inplace=True)
        markov_transition_matrix = pd.melt(
            markov_transition_matrix,
            id_vars="index",
            var_name="destination",
            value_name="probability",
        )
        markov_transition_matrix.columns = ["source", "destination", "probability"]
        markov_transition_matrix["run_date"] = current_date.date()
        markov_transition_matrix_all_regis_30 = pd.concat(
            [markov_transition_matrix_all_regis_30, markov_transition_matrix],
            ignore_index=True,
        )

        # Removal effects
        removal_effect_matrix = attribution_markov[3].round(3)
        channels = removal_effect_matrix.index
        removal_effect_values = removal_effect_matrix[["removal_effect"]]
        normalized_values = (removal_effect_values / removal_effect_values.sum()) * 100
        normalized_removal_effects = pd.DataFrame(
            normalized_values, index=channels, columns=["removal_effect"]
        )
        normalized_removal_effects["run_date"] = current_date.date()
        normalized_removal_effects["removal_effect_raw"] = (
            removal_effect_values.values.flatten()
        )
        normalized_removal_effects.reset_index(inplace=True)
        normalized_removal_effects.rename(columns={"index": "channel"}, inplace=True)
        normalized_removal_effects_all_regis_30 = pd.concat(
            [normalized_removal_effects_all_regis_30, normalized_removal_effects],
            ignore_index=True,
        )

        # Attribution by channels and models
        attribution_df = attributions.group_by_channels_models
        attribution_df["run_date"] = current_date.date()
        attribution_df.columns = attribution_df.columns.str.replace(
            ".", "_", regex=False
        ).str.replace(" ", "_", regex=False)
        attribution_df_all_regis_30 = pd.concat(
            [attribution_df_all_regis_30, attribution_df], ignore_index=True
        )

        print(f"Processed data for {current_date.strftime('%Y-%m-%d')}")

    except Exception as e:
        print(
            f"An error occurred for the date {current_date.strftime('%Y-%m-%d')}: {e}"
        )
        continue

################################################# Finalize Results #########################################

attribution_df_all_regis_30["conversion_window"] = 30
normalized_removal_effects_all_regis_30["conversion_window"] = 30
markov_transition_matrix_all_regis_30["conversion_window"] = 30
user_df_all_regis_30["conversion_window"] = 30

attribution_df_all_regis_30["conversion_type"] = "registration"
normalized_removal_effects_all_regis_30["conversion_type"] = "registration"
markov_transition_matrix_all_regis_30["conversion_type"] = "registration"
user_df_all_regis_30["conversion_type"] = "registration"

############################################################################################## Merge Regis ##########################################################################################
suffixes = ['30', '60', '90']

attribution_dfs_regis = [globals()[f'attribution_df_all_regis_{suffix}'] for suffix in suffixes]
attribution_df_all_regis = pd.concat(attribution_dfs_regis, ignore_index=True)

removal_effects_dfs_regis = [globals()[f'normalized_removal_effects_all_regis_{suffix}'] for suffix in suffixes]
normalized_removal_effects_all_regis = pd.concat(removal_effects_dfs_regis, ignore_index=True)

markov_transition_dfs_regis = [globals()[f'markov_transition_matrix_all_regis_{suffix}'] for suffix in suffixes]
markov_transition_matrix_all_regis = pd.concat(markov_transition_dfs_regis, ignore_index=True)

user_dfs_regis = [globals()[f'user_df_all_regis_{suffix}'] for suffix in suffixes]
user_df_all_regis = pd.concat(user_dfs_regis, ignore_index=True)

user_df_all_regis['num_touchpoints'] = user_df_all_regis['channels_agg'].str.split(' > ').apply(len)
user_df_all_regis["conversion_type"] = "Registration"
markov_transition_matrix_all_regis["conversion_type"] = "Registration"
normalized_removal_effects_all_regis["conversion_type"] = "Registration"
attribution_df_all_regis["conversion_type"] = "Registration"



################################################# Merge Trial, Subscription, Registration subsets #########################################

user_df_all = pd.concat([user_df_all_trial, user_df_all_subs], ignore_index=True)
user_df_all= pd.concat([user_df_all, user_df_all_regis], ignore_index=True)

# Ensure user_registration_source column exists (values come from staging via product_arrangement_df merge; only add if missing)
if "user_registration_source" not in user_df_all.columns:
    user_df_all["user_registration_source"] = pd.NA

markov_transition_matrix_all = pd.concat(
    [markov_transition_matrix_all_trial, markov_transition_matrix_all_subs],
    ignore_index=True,
)
markov_transition_matrix_all = pd.concat([markov_transition_matrix_all, markov_transition_matrix_all_regis], ignore_index=True)

normalized_removal_effects_all = pd.concat(
    [normalized_removal_effects_all_trial, normalized_removal_effects_all_subs],
    ignore_index=True,
)

normalized_removal_effects_all = pd.concat([normalized_removal_effects_all, normalized_removal_effects_all_regis], ignore_index=True)

attribution_df_all = pd.concat(
    [attribution_df_all_subs, attribution_df_all_trial], ignore_index=True
)

attribution_df_all = pd.concat([attribution_df_all, attribution_df_all_regis], ignore_index=True)

conversion_window_df = pd.concat(
    [conversion_window_df_subs, conversion_window_df_trial], ignore_index=True
)

conversion_window_df = pd.concat(
    [conversion_window_df, conversion_window_df_regis], ignore_index=True
)

# Rename user_df_all columns in big query format
def sanitize_column_name(col_name):
    # Remove patterns like '_0.3', '0.6', etc.
    sanitized = re.sub(r"(_)?\d+\.\d+", "", col_name)
    # Replace multiple underscores with a single underscore
    sanitized = re.sub(r"_+", "_", sanitized)
    # Remove leading or trailing underscores
    sanitized = sanitized.strip("_")
    return sanitized


# Create a mapping from original to sanitized column names
renamed_columns = {col: sanitize_column_name(col) for col in user_df_all.columns}

# Rename the DataFrame columns
user_df_all = user_df_all.rename(columns=renamed_columns)

######################################################################################## Merge with LTV #####################################################################################

client = bigquery.Client(project="ft-customer-analytics")
ltv_table_id = "ft-customer-analytics.crg_nniu.ltv_last_15_days"
query = f"""
    SELECT * FROM
        {ltv_table_id}
"""


query_job = client.query(query)
ltv_df = query_job.to_dataframe(bqstorage_client=bqstorage)

ltv_df = ltv_df.dropna(subset=["ltv_acquisition_capped_12m"])

group_columns = [col for col in ltv_df.columns if col != "ltv_acquisition_capped_12m"]

# Group by all columns except 'ltv_acquisition_capped_12m' and calculate its mean
ltv_df = ltv_df.groupby(group_columns, as_index=False).agg(
    ltv_acquisition_capped_12m=("ltv_acquisition_capped_12m", "mean")
)

# # extract user guid from journey id
user_df_all["user_guid"] = user_df_all["journey_id"].str.extract(r"id:(.*)_J:0")[0]

# date column conversion for ltv df
ltv_df["product_order_timestamp"] = pd.to_datetime(
    ltv_df["product_order_timestamp"], utc=True
)
user_df_all["run_date"] = pd.to_datetime(user_df_all["run_date"], utc=True)

# Convert date columns
ltv_df["product_order_timestamp"] = ltv_df["product_order_timestamp"].dt.date
user_df_all["run_date"] = user_df_all["run_date"].dt.date

# convert ltv 12m as float
ltv_df["ltv_acquisition_capped_12m"] = ltv_df["ltv_acquisition_capped_12m"].astype(
    float
)

#user_df_all = user_df_all[user_df_all["conversion_value"] == 1]
user_df_all["product_arrangement_id"] = user_df_all["product_arrangement_id"].fillna(0)

user_df_all = pd.merge(
    user_df_all,
    ltv_df,
    left_on=["product_arrangement_id", "run_date"],
    right_on=["product_arrangement_id", "product_order_timestamp"],
    how="left",
)

# Trial conversion window DataFrames
user_df_trial_30 = user_df_all[
    (user_df_all["conversion_window"] == 30)
    & (user_df_all["conversion_type"] == "Trial")
]
user_df_trial_60 = user_df_all[
    (user_df_all["conversion_window"] == 60)
    & (user_df_all["conversion_type"] == "Trial")
]
user_df_trial_90 = user_df_all[
    (user_df_all["conversion_window"] == 90)
    & (user_df_all["conversion_type"] == "Trial")
]

# Subscription conversion window DataFrames
user_df_subscription_30 = user_df_all[
    (user_df_all["conversion_window"] == 30)
    & (user_df_all["conversion_type"] == "Subscription")
]
user_df_subscription_60 = user_df_all[
    (user_df_all["conversion_window"] == 60)
    & (user_df_all["conversion_type"] == "Subscription")
]
user_df_subscription_90 = user_df_all[
    (user_df_all["conversion_window"] == 90)
    & (user_df_all["conversion_type"] == "Subscription")
]

#Registration conversion window DataFrames
user_df_registration_30 = user_df_all[
    (user_df_all["conversion_window"] == 30)
    & (user_df_all["conversion_type"] == "Registration")
]
user_df_registration_60 = user_df_all[
    (user_df_all["conversion_window"] == 60)
    & (user_df_all["conversion_type"] == "Registration")
]
user_df_registration_90 = user_df_all[
    (user_df_all["conversion_window"] == 90)
    & (user_df_all["conversion_type"] == "Registration")
]


def calculate_removal_effect(row):
    attr = row["attribution_markov_algorithmic"]
    ltv = row["ltv_acquisition_capped_12m"]
    channels = row["channels_agg"]

    if pd.isna(attr) or pd.isna(channels):
        return np.nan

    attr_parts = attr.split(">")
    channel_parts = channels.split(">")

    if len(attr_parts) != len(channel_parts):
        return np.nan

    new_parts = []
    for channel, part in zip(channel_parts, attr_parts):
        channel = channel.strip()
        part = part.strip()
        try:
            val = float(part)
            multiplied_val = val * ltv
            formatted_val = f"{multiplied_val}"
            new_parts.append(f"{channel}: {formatted_val}")
        except ValueError:
            return np.nan

    return " > ".join(new_parts)


def process_user_df(user_df):
    # Apply the function to create the 'removal_effect_ltv' column
    user_df["removal_effect_ltv"] = user_df.apply(calculate_removal_effect, axis=1)
    user_df = user_df.dropna(subset=["removal_effect_ltv"]).copy()

    # Split 'removal_effect_ltv' into a list of 'channel: ltv' strings
    user_df["channel_ltv_list"] = user_df["removal_effect_ltv"].str.split(" > ")

    # Explode the list to have one 'channel: ltv' per row
    df_exploded = user_df.explode("channel_ltv_list")

    # Split each 'channel_ltv' into 'channel' and 'ltv'
    df_exploded[["channel", "ltv"]] = df_exploded["channel_ltv_list"].str.split(
        ": ", n=1, expand=True
    )

    # Convert 'ltv' to numeric, handling any non-numeric values gracefully
    df_exploded["ltv"] = pd.to_numeric(df_exploded["ltv"], errors="coerce")

    # Group by 'channel' and 'run_date', then calculate the mean LTV
    average_ltv_per_channel = (
        df_exploded.groupby(["channel", "run_date"])["ltv"].mean().reset_index()
    )

    # Rename columns for clarity
    average_ltv_per_channel.rename(columns={"ltv": "average_ltv"}, inplace=True)

    return average_ltv_per_channel


# Applying the process to each DataFrame and storing the results
user_df_trial_30_avg = process_user_df(user_df_trial_30)
user_df_trial_60_avg = process_user_df(user_df_trial_60)
user_df_trial_90_avg = process_user_df(user_df_trial_90)

user_df_subscription_30_avg = process_user_df(user_df_subscription_30)
user_df_subscription_60_avg = process_user_df(user_df_subscription_60)
user_df_subscription_90_avg = process_user_df(user_df_subscription_90)

user_df_registration_30_avg = process_user_df(user_df_registration_30)
user_df_registration_60_avg = process_user_df(user_df_registration_60)
user_df_registration_90_avg = process_user_df(user_df_registration_90)

# Adding the 'conversion_window' and 'conversion_type' columns to each DataFrame
user_df_trial_30_avg["conversion_window"] = 30
user_df_trial_30_avg["conversion_type"] = "Trial"

user_df_trial_60_avg["conversion_window"] = 60
user_df_trial_60_avg["conversion_type"] = "Trial"

user_df_trial_90_avg["conversion_window"] = 90
user_df_trial_90_avg["conversion_type"] = "Trial"

user_df_subscription_30_avg["conversion_window"] = 30
user_df_subscription_30_avg["conversion_type"] = "Subscription"

user_df_subscription_60_avg["conversion_window"] = 60
user_df_subscription_60_avg["conversion_type"] = "Subscription"

user_df_subscription_90_avg["conversion_window"] = 90
user_df_subscription_90_avg["conversion_type"] = "Subscription"

user_df_registration_30_avg['conversion_window'] = 30
user_df_registration_30_avg['conversion_type'] = 'Registration'

user_df_registration_60_avg['conversion_window'] = 60
user_df_registration_60_avg['conversion_type'] = 'Registration'

user_df_registration_90_avg['conversion_window'] = 90
user_df_registration_90_avg['conversion_type'] = 'Registration'

# Merging all DataFrames together
average_ltv_per_channel = pd.concat(
    [
        user_df_trial_30_avg,
        user_df_trial_60_avg,
        user_df_trial_90_avg,
        user_df_subscription_30_avg,
        user_df_subscription_60_avg,
        user_df_subscription_90_avg,
        user_df_registration_30_avg,
        user_df_registration_60_avg,
        user_df_registration_90_avg
    ],
    ignore_index=True,
)

normalized_removal_effects_all = normalized_removal_effects_all[
    normalized_removal_effects_all["removal_effect"] != 0
]
normalized_removal_effects_all = pd.merge(
    normalized_removal_effects_all,
    average_ltv_per_channel,
    left_on=("channel", "conversion_window", "conversion_type", "run_date"),
    right_on=("channel", "conversion_window", "conversion_type", "run_date"),
    how="left",
)

user_df_all.drop(columns=["user_guid_x", "user_guid_y"], inplace=True)

######################################################################################## Data upload #####################################################################################

#Configure the load job

# job_config = bigquery.LoadJobConfig(
#     write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # WRITE_TRUNCATE. # WRITE_APPEND
#     source_format=bigquery.SourceFormat.PARQUET,
#     autodetect=True,
#     time_partitioning=bigquery.TimePartitioning(
#         type_=bigquery.TimePartitioningType.DAY, field="run_date"
#     ),
# )

# dataframes = {
#     "ft-customer-analytics.crg_nniu.attribution_markov_transition_matrix_all": markov_transition_matrix_all,
#     "ft-customer-analytics.crg_nniu.attribution_normalized_removal_effects_all": normalized_removal_effects_all,
#     "ft-customer-analytics.crg_nniu.attribution_user_df_all": user_df_all,
#     "ft-customer-analytics.crg_nniu.attribution_df_all": attribution_df_all,
#     "ft-customer-analytics.crg_nniu.attribution_conversion_window_df": conversion_window_df
    
# }

# for destination_table, dataframe in dataframes.items():
#     try:
#         load_job = client.load_table_from_dataframe(
#             dataframe.reset_index(drop=True), destination_table, job_config=job_config
#         )
#         load_job.result()
#         print(f"Load job for {destination_table} completed successfully.")
#     except Exception as e:
#         print(f"Error loading data to {destination_table}: {e}")

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append after deleting old partitions
    source_format=bigquery.SourceFormat.PARQUET,
    autodetect=True,
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field="run_date"
    ),
)

dataframes = {
    "ft-customer-analytics.crg_nniu_attribution.attribution_markov_transition_matrix_all_test": markov_transition_matrix_all,
    "ft-customer-analytics.crg_nniu_attribution.attribution_normalized_removal_effects_all_test": normalized_removal_effects_all,
    "ft-customer-analytics.crg_nniu_attribution.attribution_user_df_all_test": user_df_all,
    "ft-customer-analytics.crg_nniu_attribution.attribution_df_all_test": attribution_df_all,
    "ft-customer-analytics.crg_nniu_attribution.attribution_conversion_window_df_test": conversion_window_df
}

for destination_table, dataframe in dataframes.items():
    # Extract unique run_dates from the DataFrame
    run_dates = dataframe['run_date'].unique()  # Already in "YYYY-MM-DD" format
    
    # Delete existing data for these run_dates
    for run_date in run_dates:
        query = f"""
            DELETE FROM `{destination_table}`
            WHERE run_date = DATE('{run_date}')
        """
        try:
            delete_job = client.query(query)
            delete_job.result()  # Wait for completion
            print(f"Deleted partition {run_date} from {destination_table}")
        except Exception as e:
            print(f"Error deleting partition {run_date}: {e}")
    
    # Load new data into the cleared partitions
    try:
        load_job = client.load_table_from_dataframe(
            dataframe.reset_index(drop=True), destination_table, job_config=job_config
        )
        load_job.result()
        print(f"Loaded new data into {destination_table}")
    except Exception as e:
        print(f"Error loading data to {destination_table}: {e}")