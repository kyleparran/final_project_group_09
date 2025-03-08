import pandas as pd
import numpy as np
import wrds
from settings import config
from pathlib import Path
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")
db = wrds.Connection(wrds_username=WRDS_USERNAME)

PAPER_START_DATE = '1970-01-01'
PAPER_END_DATE   = '2008-12-31'
CURRENT_START_DATE = '2009-01-01'
CURRENT_END_DATE   = '2024-02-28'

def fetch_wrds_contract_info(product_contract_code, time_period='paper'):
    """
    Fetch rows from wrds_contract_info.
    """
    if time_period == 'paper':
        start_date = PAPER_START_DATE
        end_date = PAPER_END_DATE
    else:
        start_date = CURRENT_START_DATE
        end_date = CURRENT_END_DATE
    query = f"""
    SELECT futcode, contrcode, contrname, contrdate, startdate, lasttrddate
    FROM tr_ds_fut.wrds_contract_info
    WHERE contrcode = {product_contract_code}
      AND startdate >= '{start_date}'
      AND lasttrddate <= '{end_date}'
    """
    return db.raw_sql(query)

def fetch_wrds_fut_contract(futcodes_contrdates, time_period='paper'):
    """
    Fetch daily settlement prices from wrds_fut_contract.
    """
    if time_period == 'paper':
        start_date = PAPER_START_DATE
        end_date = PAPER_END_DATE
    else:
        start_date = CURRENT_START_DATE
        end_date = CURRENT_END_DATE
    query = f"""
    SELECT futcode, date_, settlement
    FROM tr_ds_fut.wrds_fut_contract
    WHERE futcode IN {tuple(futcodes_contrdates.keys())}
      AND date_ >= '{start_date}'
      AND date_ <= '{end_date}'
    """
    df = db.raw_sql(query)
    df["date_"] = pd.to_datetime(df["date_"])
    df["contrdate"] = df["futcode"].map(futcodes_contrdates)
    return df

def pull_all_futures_data(time_period="paper"):
    """
    Pull raw data from WRDS for all product codes in product_list,
    then concatenate into one DataFrame.
    """
    product_list = [
        3160, 289, 3161, 1980, 2038, 3247, 1992, 361, 385, 2036,
        379, 3256, 396, 430, 1986, 2091, 2029, 2060, 3847, 2032,
        3250, 2676, 2675, 3126, 2087, 2026, 2020, 2065, 2074, 2108
    ]
    all_frames = []
    for code in product_list:
        info_df = fetch_wrds_contract_info(code, time_period)
        if info_df.empty:
            continue
        futcodes_contrdates = info_df.set_index("futcode")["contrdate"].to_dict()
        data_contracts = fetch_wrds_fut_contract(futcodes_contrdates, time_period)
        if not data_contracts.empty:
            # add product_code (optional, for reference)
            data_contracts["product_code"] = code
            all_frames.append(data_contracts)
    if len(all_frames) > 0:
        final_df = pd.concat(all_frames, ignore_index=True)
    else:
        final_df = pd.DataFrame()  # empty if nothing found
    return final_df



