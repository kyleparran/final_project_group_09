import pandas as pd
import pytest
from settings import config
import pull_futures_data
DATA_DIR = config("DATA_DIR")


def test_fetch_wrds_contract_info():
    df = pull_futures_data.fetch_wrds_contract_info(2036, 'paper')
    # Test if the function returns a pandas DataFrame
    assert isinstance(df, pd.DataFrame)

    # Test if the DataFrame has the expected columns
    expected_columns = ['futcode', 'contrcode', 'contrname', 'contrdate', 'startdate', 'lasttrddate']
    assert all(col in df.columns for col in expected_columns)


def test_fetch_wrds_fut_contract_validity():
    info_df = pull_futures_data.fetch_wrds_contract_info(2036, 'paper')

    futcodes_contrdates = info_df.set_index("futcode")["contrdate"].to_dict()

    data_contracts = pull_futures_data.fetch_wrds_fut_contract(futcodes_contrdates, 'paper')

    # Test if the default date range has the expected start date and end date
    assert data_contracts['date_'].min() >= pd.Timestamp('1970-01-01')
    assert data_contracts['date_'].max() <= pd.Timestamp('2008-12-31')

    # Test if the average settlement price for orange juice futures contracts is within a reasonable range
    OJ_avg_price = data_contracts.dropna()['settlement'].mean()
    assert OJ_avg_price > 80 and OJ_avg_price < 200
