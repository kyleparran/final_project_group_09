# test_calc_format_futures_data.py

import pandas as pd
import numpy as np
from calc_format_futures_data import (
    extract_first_through_12th_contracts,
    compute_futures_stats
)

def test_compute_basis_and_excess_returns_expanded():
    """
    Expanded test data for a single futcode "F1" over 6 obs_months (Jan..Jun),
    each with 1-month and 2-month-ahead contracts.  This ensures we have more
    rows and more varied basis/returns to test the pivot and calculation logic.
    """

    # -------------------------------------------------------------------------
    # 1) Build a monthly_df for obs_period = Jan..Jun 2023.
    #    Each obs_month has two contr_periods: (obs_month+1), (obs_month+2).
    # -------------------------------------------------------------------------
    data = [
        # obs_period=Jan => T1=Feb=100, T2=Mar=105
        {"futcode": "F1", "obs_period": pd.Period("2023-01", freq="M"),
         "contr_period": pd.Period("2023-02", freq="M"), "settlement": 100},
        {"futcode": "F1", "obs_period": pd.Period("2023-01", freq="M"),
         "contr_period": pd.Period("2023-03", freq="M"), "settlement": 105},

        # obs_period=Feb => T1=Mar=110, T2=Apr=120
        {"futcode": "F1", "obs_period": pd.Period("2023-02", freq="M"),
         "contr_period": pd.Period("2023-03", freq="M"), "settlement": 110},
        {"futcode": "F1", "obs_period": pd.Period("2023-02", freq="M"),
         "contr_period": pd.Period("2023-04", freq="M"), "settlement": 120},

        # obs_period=Mar => T1=Apr=115, T2=May=130
        {"futcode": "F1", "obs_period": pd.Period("2023-03", freq="M"),
         "contr_period": pd.Period("2023-04", freq="M"), "settlement": 115},
        {"futcode": "F1", "obs_period": pd.Period("2023-03", freq="M"),
         "contr_period": pd.Period("2023-05", freq="M"), "settlement": 130},

        # obs_period=Apr => T1=May=125, T2=Jun=140
        {"futcode": "F1", "obs_period": pd.Period("2023-04", freq="M"),
         "contr_period": pd.Period("2023-05", freq="M"), "settlement": 125},
        {"futcode": "F1", "obs_period": pd.Period("2023-04", freq="M"),
         "contr_period": pd.Period("2023-06", freq="M"), "settlement": 140},

        # obs_period=May => T1=Jun=135, T2=Jul=145
        {"futcode": "F1", "obs_period": pd.Period("2023-05", freq="M"),
         "contr_period": pd.Period("2023-06", freq="M"), "settlement": 135},
        {"futcode": "F1", "obs_period": pd.Period("2023-05", freq="M"),
         "contr_period": pd.Period("2023-07", freq="M"), "settlement": 145},

        # obs_period=Jun => T1=Jul=140, T2=Aug=150
        {"futcode": "F1", "obs_period": pd.Period("2023-06", freq="M"),
         "contr_period": pd.Period("2023-07", freq="M"), "settlement": 140},
        {"futcode": "F1", "obs_period": pd.Period("2023-06", freq="M"),
         "contr_period": pd.Period("2023-08", freq="M"), "settlement": 150},
    ]
    monthly_df = pd.DataFrame(data)

    # -------------------------------------------------------------------------
    # 2) Pivot out the 1..12-month settlements
    # -------------------------------------------------------------------------
    pivoted_df = extract_first_through_12th_contracts(monthly_df)
    
    # Quick sanity check: the pivoted_df should have an index of six obs_periods (Jan..Jun)
    # and columns "1mth_settlement", "2mth_settlement", ... "12mth_settlement"
    # We'll just confirm 1mth and 2mth are indeed in the columns:
    assert "1mth_settlement" in pivoted_df.columns, "1mth_settlement missing after pivot!"
    assert "2mth_settlement" in pivoted_df.columns, "2mth_settlement missing after pivot!"
    
    # Also verify the pivoted values for Jan (2023-01) are 100 and 105, etc.
    jan_row = pivoted_df.loc[pd.Period("2023-01", freq="M")]
    assert jan_row["1mth_settlement"] == 100
    assert jan_row["2mth_settlement"] == 105

    # -------------------------------------------------------------------------
    # 3) Compute basis & annual excess-return stats
    # -------------------------------------------------------------------------
    stats_dict = compute_futures_stats(pivoted_df, monthly_df)

    # Confirm the result has the keys we expect:
    for k in ["N", "mean_basis", "freq_bw", "excess_return_mean", "excess_return_std", "sharpe_ratio"]:
        assert k in stats_dict, f"Missing key {k} in compute_futures_stats output!"

    # check that the stats are within a reasonable range of what we expect
    assert stats_dict["N"] == 6, "Expected 6 obs_periods in the stats output!"
    assert abs(stats_dict["mean_basis"] - (-8.5339)) < 0.01, "Expected mean_basis to be 8.5339!"
    assert abs(stats_dict["freq_bw"]) < 1e-4, "Expected freq_bw to be 0.0000!"
    assert abs(stats_dict["excess_return_mean"] - 50) < 1e-4, "Expected excess_return_mean to be 50!"
    assert np.isnan(stats_dict["excess_return_std"]), "Expected excess_return_std to be nan!"
    assert np.isnan(stats_dict["sharpe_ratio"]), "Expected sharpe_ratio to be nan!"


    print("test_compute_basis_and_excess_returns_expanded() passed!")
