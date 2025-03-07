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


