# %% [markdown]
# # Attribution Model — main3

# %% Imports and helpers
from marketing_attribution_models import MAM
import pandas as pd
import numpy as np
import os
import re
import traceback
import time
import psycopg2
from sqlalchemy import create_engine, Text

import sys as _sys

def _ts(msg):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}\n"
    (_sys.__stdout__ or _sys.stdout).write(line)
    (_sys.__stdout__ or _sys.stdout).flush()

_ts("Imports loaded.")

# %% Redshift connection

_rs_host = os.environ.get("REDSHIFT_HOST", "redshift.dw.in.ft.com")
_rs_port = int(os.environ.get("REDSHIFT_PORT", 5439))
_rs_db   = os.environ.get("REDSHIFT_DB", "prod")
_rs_user = os.environ.get("REDSHIFT_USER")
_rs_pass = os.environ.get("REDSHIFT_PASSWORD")

conn   = psycopg2.connect(host=_rs_host, port=_rs_port, dbname=_rs_db, user=_rs_user, password=_rs_pass)
engine = create_engine(f"postgresql+psycopg2://{_rs_user}:{_rs_pass}@{_rs_host}:{_rs_port}/{_rs_db}")
_ts("Connected to Redshift.")

# %% Config

IDS         = "user_guid"
DATE_COL    = "attribution_visit_start_time"
TOUCHPOINT  = "touchpoint"
TRANSACTION = "converting_visit"

end_date   = (pd.Timestamp.today() - pd.DateOffset(days=1)).date()
start_date = (pd.Timestamp.today() - pd.DateOffset(days=15)).date()
_ts(f"Date range: {start_date} → {end_date}")

# Each tuple: (conversion_type filter in DB, output label, stage name, lookback days)
# Grouped by lookback so each stg table is loaded once and reused across conv types.
SEGMENTS = [
    ("Subscription", "Subscription", "subscriber",   30),
    ("Trial",        "Trial",        "trial",         30),
    ("registration", "Registration", "registration",  30),
    ("Subscription", "Subscription", "subscriber",   60),
    ("Trial",        "Trial",        "trial",         60),
    ("registration", "Registration", "registration",  60),
    ("Subscription", "Subscription", "subscriber",   90),
    ("Trial",        "Trial",        "trial",         90),
    ("registration", "Registration", "registration",  90),
]

# %% Helper functions

def load_table(table_id):
    """Fetch all rows for the run date window and build the channels_agg journey string."""
    _ts(f"  Loading {table_id}...")
    t0 = time.time()
    df = pd.read_sql(
        f"SELECT * FROM {table_id} "
        f"WHERE conversion_visit_timestamp::DATE >= '{start_date}' "
        f"AND   conversion_visit_timestamp::DATE <= '{end_date}'",
        engine,
    )
    _ts(f"  → {len(df):,} rows loaded in {time.time()-t0:.1f}s")
    df = df.sort_values([IDS, "conversion_visit_timestamp", DATE_COL])
    journey = (
        df.groupby([IDS, "conversion_visit_timestamp"])[TOUCHPOINT]
        .apply(" > ".join)
        .reset_index(name="channels_agg")
    )
    return df.merge(journey, on=[IDS, "conversion_visit_timestamp"])


def compute_median_days(filtered_df):
    """Return the median number of days from first visit to conversion."""
    days = []
    for _, user_data in filtered_df.groupby(IDS):
        first_visit = user_data[user_data[TRANSACTION] == 0][DATE_COL].min()
        if pd.isnull(first_visit):
            continue
        conv = user_data[user_data[TRANSACTION] == 1][DATE_COL].min()
        if pd.notnull(conv):
            days.append((conv - first_visit).days)
    return pd.Series(days).median() if days else None


def run_mam(df, current_date, conv_type):
    """
    Run the full MAM attribution pipeline for one date slice and one conversion type.

    Returns (user_df, markov_df, removal_df, attr_df, median_days).
    Result DataFrames are None if MAM fails or there are no matching rows.
    """
    df = df.copy()
    df["original_transaction"] = df[TRANSACTION]

    filtered = df[df["conversion_type"] == conv_type].drop(columns=["conversion_type"]).copy()
    if filtered.empty:
        return None, None, None, None, None

    # Mark a single converting visit per user (most recent conversion)
    filtered["user_max_date"] = filtered.groupby(IDS)["conversion_visit_timestamp"].transform("max")
    filtered[TRANSACTION] = 0
    filtered.loc[
        (filtered[DATE_COL] == filtered["user_max_date"]) & (filtered["original_transaction"] == 1),
        TRANSACTION,
    ] = 1
    filtered.drop(columns=["user_max_date"], inplace=True)
    filtered = filtered.sort_values([IDS, DATE_COL], ascending=[False, True])
    filtered["run_date"] = current_date.date()

    median_days = compute_median_days(filtered)

    try:
        _ts(f"    MAM init — {current_date.date()} | {conv_type} | {len(filtered):,} rows")
        attributions = MAM(
            filtered,
            group_channels=True,
            channels_colname=TOUCHPOINT,
            journey_with_conv_colname=TRANSACTION,
            group_channels_by_id_list=[IDS],
            group_timestamp_colname=DATE_COL,
            create_journey_id_based_on_conversion=True,
        )
        _ts(f"    last_click...")
        attributions.attribution_last_click()
        _ts(f"    first_click...")
        attributions.attribution_first_click()
        _ts(f"    position_based...")
        attributions.attribution_position_based(list_positions_first_middle_last=[0.3, 0.3, 0.4])
        _ts(f"    time_decay...")
        attributions.attribution_time_decay(decay_over_time=0.6, frequency=7)
        _ts(f"    markov (slowest step)...")
        markov_result = attributions.attribution_markov(transition_to_same_state=False)

        # User-level results
        user_df = attributions.as_pd_dataframe()
        user_df["num_touchpoints"] = user_df["channels_agg"].fillna("").str.split(" > ").apply(len)
        user_df["run_date"] = current_date.date()
        user_df[IDS] = user_df["journey_id"].str.extract(r"id:(.*)_J:0")[0]

        # Join product metadata back from the original (unfiltered) date slice
        df["conversion_visit_timestamp_date"] = df["conversion_visit_timestamp"].dt.date
        product_df = (
            df[df["conversion_type"] == conv_type]
            [[IDS, "conversion_visit_timestamp_date",
              "product_arrangement_id", "is_app_conversion",
              "product_type", "user_registration_source"]]
            .drop_duplicates(subset=[IDS, "conversion_visit_timestamp_date"], keep="first")
        )
        user_df = user_df.merge(
            product_df,
            left_on=[IDS, "run_date"],
            right_on=[IDS, "conversion_visit_timestamp_date"],
            how="left",
        ).drop(columns=["conversion_visit_timestamp_date"])

        # Markov transition matrix → long format
        markov_df = markov_result[2].round(3).rename(
            index=lambda x: x.replace("(inicio)", "(start)"),
            columns=lambda x: x.replace("(inicio)", "(start)"),
        )
        markov_df.reset_index(inplace=True)
        markov_df = pd.melt(markov_df, id_vars="index", var_name="destination", value_name="probability")
        markov_df.columns = ["source", "destination", "probability"]
        markov_df["run_date"] = current_date.date()

        # Normalized removal effects
        raw = markov_result[3].round(3)
        norm = (raw[["removal_effect"]] / raw[["removal_effect"]].sum()) * 100
        removal_df = pd.DataFrame(norm.values, index=raw.index, columns=["removal_effect"])
        removal_df["removal_effect_raw"] = raw["removal_effect"].values
        removal_df["run_date"] = current_date.date()
        removal_df.reset_index(inplace=True)
        removal_df.rename(columns={"index": "channel"}, inplace=True)

        # Channel × model attribution summary
        attr_df = attributions.group_by_channels_models.copy()
        attr_df["run_date"] = current_date.date()
        attr_df.columns = (
            attr_df.columns
            .str.replace(".", "_", regex=False)
            .str.replace(" ", "_", regex=False)
        )

        _ts(f"    ✓ {current_date.date()} done — {len(user_df)} users")
        return user_df, markov_df, removal_df, attr_df, median_days

    except Exception as e:
        _ts(f"    ✗ {current_date.date()} failed: {e}")
        print(traceback.format_exc(), flush=True)
        return None, None, None, None, median_days


def sanitize_col(col):
    """Remove decimal notation added by MAM (e.g. 'attr_0.3') and tidy underscores."""
    col = re.sub(r"(_)?\d+\.\d+", "", col)
    col = re.sub(r"_+", "_", col)
    return col.strip("_")


def calculate_removal_effect(row):
    attr, ltv, channels = (
        row["attribution_markov_algorithmic"],
        row["ltv_acquisition_capped_12m"],
        row["channels_agg"],
    )
    if any(pd.isna(v) or v is None for v in [attr, ltv, channels]):
        return np.nan
    try:
        attr_parts = str(attr).strip().split(">")
        ch_parts   = str(channels).strip().split(">")
    except Exception:
        return np.nan
    if len(attr_parts) != len(ch_parts):
        return np.nan
    parts = []
    for ch, pt in zip(ch_parts, attr_parts):
        try:
            parts.append(f"{ch.strip()}: {float(pt.strip()) * float(ltv)}")
        except (ValueError, TypeError):
            return np.nan
    return " > ".join(parts) if parts else np.nan


def process_user_df(user_df):
    """Calculate average LTV per channel and run_date from a user-level DataFrame."""
    user_df = user_df.copy()
    user_df["removal_effect_ltv"] = user_df.apply(calculate_removal_effect, axis=1)
    user_df = user_df.dropna(subset=["removal_effect_ltv"])
    user_df["channel_ltv_list"] = user_df["removal_effect_ltv"].str.split(" > ")
    df_exp = user_df.explode("channel_ltv_list")
    df_exp[["channel", "ltv"]] = df_exp["channel_ltv_list"].str.split(": ", n=1, expand=True)
    df_exp["ltv"] = pd.to_numeric(df_exp["ltv"], errors="coerce")
    return (
        df_exp.groupby(["channel", "run_date"])["ltv"]
        .mean()
        .reset_index()
        .rename(columns={"ltv": "average_ltv"})
    )

_ts("Helper functions defined.")

# %% Main attribution loop

all_user, all_markov, all_removal, all_attr = [], [], [], []
conv_window_records = []
table_cache = {}  # avoids re-querying the same stg table for different conv types

for conv_type, output_label, stage_name, lookback in SEGMENTS:
    table_id = f"bilayer.stg_conversion_users_last_15_days_{lookback}_days_lookback_table"

    _ts(f"{'─' * 60}")
    _ts(f"Segment: {output_label} | {lookback}-day lookback")

    if table_id not in table_cache:
        table_cache[table_id] = load_table(table_id)
    df_all = table_cache[table_id]

    seg_user, seg_markov, seg_removal, seg_attr = [], [], [], []
    all_dates = list(pd.date_range(start_date, end_date, freq="D"))

    for date_i, current_date in enumerate(all_dates, 1):
        df = df_all[df_all["conversion_visit_timestamp"].dt.date == current_date.date()].copy()
        _ts(f"  Date {date_i}/{len(all_dates)}: {current_date.date()} — {len(df):,} rows")
        if df.empty:
            _ts(f"  Skipping {current_date.date()} (no data)")
            continue

        user_df, markov_df, removal_df, attr_df, median_days = run_mam(df, current_date, conv_type)

        # Record conversion window stats once per stage per date (from 90-day run)
        if lookback == 90:
            conv_window_records.append({
                "stage": stage_name,
                "median_days": median_days,
                "run_date": current_date.date(),
            })

        if user_df is not None:
            seg_user.append(user_df)
            seg_markov.append(markov_df)
            seg_removal.append(removal_df)
            seg_attr.append(attr_df)

    def _concat(dfs):
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    user_out    = _concat(seg_user)
    markov_out  = _concat(seg_markov)
    removal_out = _concat(seg_removal)
    attr_out    = _concat(seg_attr)

    for df_ in [user_out, markov_out, removal_out, attr_out]:
        if not df_.empty:
            df_["conversion_window"] = lookback
            df_["conversion_type"]   = output_label

    all_user.append(user_out)
    all_markov.append(markov_out)
    all_removal.append(removal_out)
    all_attr.append(attr_out)

    _ts(f"  Segment done — {len(user_out):,} user rows")

# %% Combine all segments

_ts("Combining all segments...")

user_df_all  = pd.concat(all_user,    ignore_index=True)
markov_all   = pd.concat(all_markov,  ignore_index=True)
removal_all  = pd.concat(all_removal, ignore_index=True)
attr_all     = pd.concat(all_attr,    ignore_index=True)
conv_window  = pd.DataFrame(conv_window_records)

user_df_all = user_df_all.rename(columns={c: sanitize_col(c) for c in user_df_all.columns})

_ts(f"Total rows — users: {len(user_df_all):,} | markov: {len(markov_all):,} | removal: {len(removal_all):,} | attr: {len(attr_all):,}")

# %% LTV merge

_ts("Loading LTV table...")
ltv_df = pd.read_sql("SELECT * FROM bilayer.ltv_last_15_days", engine)
ltv_df = ltv_df.dropna(subset=["ltv_acquisition_capped_12m"])
group_cols = [c for c in ltv_df.columns if c != "ltv_acquisition_capped_12m"]
ltv_df = ltv_df.groupby(group_cols, as_index=False).agg(
    ltv_acquisition_capped_12m=("ltv_acquisition_capped_12m", "mean")
)
ltv_df["ltv_acquisition_capped_12m"] = ltv_df["ltv_acquisition_capped_12m"].astype(float)
ltv_df["product_order_timestamp"] = pd.to_datetime(ltv_df["product_order_timestamp"], utc=True).dt.date

user_df_all[IDS] = user_df_all["journey_id"].str.extract(r"id:(.*)_J:0")[0]
user_df_all["run_date"] = pd.to_datetime(user_df_all["run_date"], utc=True).dt.date
user_df_all["product_arrangement_id"] = user_df_all["product_arrangement_id"].fillna(0)

user_df_all = pd.merge(
    user_df_all, ltv_df,
    left_on=["product_arrangement_id", "run_date"],
    right_on=["product_arrangement_id", "product_order_timestamp"],
    how="left",
)
for col in ["user_guid_x", "user_guid_y"]:
    if col in user_df_all.columns:
        user_df_all.drop(columns=[col], inplace=True)

_ts("Calculating average LTV per channel...")
ltv_by_segment = []
for conv_label, win in [
    ("Subscription", 30), ("Subscription", 60), ("Subscription", 90),
    ("Trial",        30), ("Trial",        60), ("Trial",        90),
    ("Registration", 30), ("Registration", 60), ("Registration", 90),
]:
    subset = user_df_all[
        (user_df_all["conversion_window"] == win) &
        (user_df_all["conversion_type"]   == conv_label)
    ]
    avg = process_user_df(subset)
    avg["conversion_window"] = win
    avg["conversion_type"]   = conv_label
    ltv_by_segment.append(avg)

average_ltv_per_channel = pd.concat(ltv_by_segment, ignore_index=True)

removal_all = removal_all[removal_all["removal_effect"] != 0]
removal_all = pd.merge(
    removal_all, average_ltv_per_channel,
    on=["channel", "conversion_window", "conversion_type", "run_date"],
    how="left",
)

_ts("LTV merge complete.")

# %% Write to Redshift

# Drop zero-probability transitions — they dominate row count and are useless
markov_all = markov_all[markov_all["probability"] > 0]
_ts(f"Markov rows after zero-filter: {len(markov_all):,}")

_ts("Writing results to Redshift...")

output_tables = {
    "attribution_markov_transition_matrix_all_test": markov_all,
    "attribution_normalized_removal_effects_all_test": removal_all,
    "attribution_user_df_all_test": user_df_all,
    "attribution_df_all_test": attr_all,
    "attribution_conversion_window_df_test": conv_window,
}

cursor = conn.cursor()
for table_name, df_ in output_tables.items():
    if df_.empty:
        _ts(f"  Skipping bilayer.{table_name} (empty)")
        continue
    _ts(f"  Writing bilayer.{table_name} — {len(df_):,} rows, columns: {list(df_.columns)}")
    for run_date in df_["run_date"].unique():
        try:
            cursor.execute(f"DELETE FROM bilayer.{table_name} WHERE run_date = '{run_date}'")
        except Exception:
            conn.rollback()  # reset aborted transaction before next statement
    conn.commit()
    try:
        dtype_override = {col: Text() for col in df_.select_dtypes(include="object").columns}
        df_.reset_index(drop=True).to_sql(table_name, engine, schema="bilayer", if_exists="append", index=False, chunksize=5000, method="multi", dtype=dtype_override)
        _ts(f"  ✓ bilayer.{table_name} — {len(df_):,} rows written")
    except Exception as e:
        _ts(f"  ✗ bilayer.{table_name} failed: {e}")
        print(traceback.format_exc(), flush=True)

cursor.close()
conn.close()
_ts("All done.")
