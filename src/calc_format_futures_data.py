from pull_futures_data import *

"""All of the functions below are associated with the assembly of data towards replciating
the commodity table by Fan Yang"""

sector_map = {
    3160: "Agriculture",
    289:  "Agriculture",
    3161: "Agriculture",
    1980: "Agriculture",
    2038: "Agriculture",
    3247: "Agriculture",
    1992: "Agriculture",
    361:  "Agriculture",
    385:  "Agriculture",
    2036: "Agriculture",
    379:  "Agriculture",
    3256: "Agriculture",
    396:  "Agriculture",
    430:  "Agriculture",
    1986: "Energy",
    2091: "Energy",
    2029: "Energy",
    2060: "Energy",
    3847: "Energy",   
    2032: "Energy",   
    3250: "Livestock",
    2676: "Livestock",
    2675: "Livestock",
    3126: "Metals",
    2087: "Metals",   
    2026: "Metals",   
    2020: "Metals",   
    2065: "Metals",   
    2074: "Metals",   
    2108: "Metals"    
}


def futures_series_to_monthly(df):
    """
    Convert a daily futures DataFrame into a monthly frequency by taking
    the last available daily row for each (futcode, month). Also parses
    contract periods and drops date columns to produce a final monthly dataset.

    Parameters
    ----------
    df : pandas.DataFrame
        Must contain columns ['futcode', 'date_', 'contrdate', 'settlement'].

    Returns
    -------
    pandas.DataFrame
        Monthly data with columns ['futcode', 'contr_period', 'obs_period', 'settlement'].
        Each row corresponds to the last daily entry in that month for the given futcode.
    """

    
    df = df.sort_values(["futcode", "date_"])
    monthly_df = df.groupby(["futcode", df["date_"].dt.to_period("M")]).tail(1).copy()

    monthly_df["contr_period"] = monthly_df["contrdate"].apply(parse_contrdate)
    monthly_df["obs_period"]   = monthly_df["date_"].dt.to_period("M")

    monthly_df = monthly_df.drop(columns=["date_", "contrdate"])
    monthly_df = monthly_df.sort_values(by=["obs_period","contr_period"])
    return monthly_df

# Parse the contrdate strings into a monthly Period 
def parse_contrdate(c):
    """
    Parse a contract date string (MMYY or MM/YY) into a monthly pandas.Period.

    Parameters
    ----------
    c : str
        Contract date in the format 'MMYY' or 'MM/YY'.

    Returns
    -------
    pandas.Period
        A monthly Period object corresponding to the parsed year and month.
    """
    c = c.replace('/', '') 
    mm = int(c[:2])
    yy = int(c[2:])
    year = (2000 + yy) if yy < 50 else (1900 + yy)
    return pd.Period(freq='M', year=year, month=mm)

def extract_first_through_12th_contracts(monthly_df):
    """
    Constructs a wide DataFrame of monthly settlement prices for the 1st through 12th contracts.

    For each obs_period in monthly_df, creates columns for 1mth_settlement up to 12mth_settlement,
    indexing by the monthly observation period.

    Parameters
    ----------
    monthly_df : pandas.DataFrame
        Must contain columns ['futcode', 'contr_period', 'obs_period', 'settlement'].

    Returns
    -------
    pandas.DataFrame
        Index = unique obs_period. Columns = ["1mth_settlement", "2mth_settlement", ..., "12mth_settlement"].
        Each cell contains that month's settlement price if available, else NaN.
    """

    temp = monthly_df.set_index(["obs_period", "contr_period"])["settlement"]

    first_through_12th_contracts_df = pd.DataFrame(index=monthly_df['obs_period'].unique())

    for i in range(1, 13):
        first_through_12th_contracts_df[f"{i}mth_settlement"] = first_through_12th_contracts_df.index.to_series().apply(
            lambda op: temp.get((op, op + i), float("nan"))  # or np.nan
        )

    return first_through_12th_contracts_df



def compute_futures_stats(first_through_12th_contracts_df, monthly_df):
    """
    Compute basis, frequency of backwardation, and basic returns stats.

    Parameters
    ----------
    first_through_12th_contracts_df : pandas.DataFrame
        Wide DataFrame of monthly settlement prices with columns like "1mth_settlement" ... "12mth_settlement".
    monthly_df : pandas.DataFrame
        Monthly-level data used to compute excess returns.

    Returns
    -------
    dict
        Keys:
            'N' : int (count of valid basis observations)
            'mean_basis' : float
            'freq_bw' : float (freq. of backwardation, in %)
            'excess_return_mean' : float (annual excess return)
            'excess_return_std' : float (std dev of annual excess return)
            'sharpe_ratio' : float (risk-adjusted return measure)
    """

    basis_df = pd.DataFrame(index=first_through_12th_contracts_df.index)
    first_through_12th_contracts_df['T1'] = first_through_12th_contracts_df.apply(
        lambda row: next((i for i in range(1, 13) if not pd.isna(row[f"{i}mth_settlement"])), np.nan),
        axis=1
    )
    first_through_12th_contracts_df['T2'] = first_through_12th_contracts_df.apply(
        lambda row: next((i for i in range(12, 0, -1) if not pd.isna(row[f"{i}mth_settlement"])), np.nan),
        axis=1
    )
    basis_df['T1'] = first_through_12th_contracts_df['T1']
    basis_df['T2'] = first_through_12th_contracts_df['T2']
    basis_df['month_diff'] = basis_df['T2'] - basis_df['T1']

    basis_df['settlement_T1'] = first_through_12th_contracts_df.apply(
        lambda row: row[f"{int(row['T1'])}mth_settlement"] if not pd.isna(row['T1']) else np.nan,
        axis=1
    )
    basis_df['settlement_T2'] = first_through_12th_contracts_df.apply(
        lambda row: row[f"{int(row['T2'])}mth_settlement"] if not pd.isna(row['T1']) else np.nan,
        axis=1
    )

    basis_df['basis'] = (np.log(basis_df['settlement_T1']) - np.log(basis_df['settlement_T2'])) / basis_df['month_diff'] * 100
    basis_df = basis_df.dropna()

    freq_bw = (basis_df["basis"] > 0).mean() * 100
    n_valid = len(basis_df)
    
    excess_return_df = monthly_df.groupby("futcode").apply(
        lambda x: (x.sort_values(by="obs_period").iloc[-1]["settlement"] / x.sort_values(by="obs_period").iloc[0]["settlement"] - 1) * 100
    ).reset_index(name="excess_return")

    er_mean = excess_return_df["excess_return"].mean()
    er_std = excess_return_df["excess_return"].std()
    sharpe = 100 * er_mean / er_std if er_std != 0 else np.nan
    return {
        "N": n_valid,
        "mean_basis": basis_df["basis"].mean(),
        "freq_bw": freq_bw,
        "excess_return_mean": er_mean,
        "excess_return_std": er_std,
        "sharpe_ratio": sharpe
    }

def process_single_product(product_contract_code, time_period='paper'):
    """
    Compute stats for a single product code.

    Pulls WRDS info for the given product, converts daily data to monthly,
    extracts contract settlements, and calculates basis/backwardation statistics.

    Parameters
    ----------
    product_contract_code : int
        Commodity's contract code.
    time_period : str, optional
        'paper' (default) or 'current' date range.

    Returns
    -------
    pandas.DataFrame or None
        Single-row DataFrame of calculated stats (e.g., N, Basis, freq. BW, Sharpe).
        Returns None if no valid data is found.
    """

    info_df = fetch_wrds_contract_info(product_contract_code, time_period)
    if info_df.empty:
        return None
    futcodes_contrdates = info_df.set_index("futcode")["contrdate"].to_dict()
    data_contracts = fetch_wrds_fut_contract(futcodes_contrdates, time_period)
    if data_contracts.empty:
        return None
    
    monthly_df = futures_series_to_monthly(data_contracts)
    first_through_12th_contracts_df = extract_first_through_12th_contracts(monthly_df)

    stats = compute_futures_stats(first_through_12th_contracts_df, monthly_df)
    
    commodity_name = info_df["contrname"].unique()[0]
    contract_code = info_df["contrcode"].unique()[0]
    return pd.DataFrame({
        "Commodity": [commodity_name],
        "Contract Code": [contract_code],
        "N": [stats["N"]],
        "Basis": [stats["mean_basis"]],
        "Freq. of Backwardation (%)": [stats["freq_bw"]],
        "E(Re) (Mean Annual Excess Return)": [stats["excess_return_mean"]],
        "σ(Re) (Std Dev of Excess Return)": [stats["excess_return_std"]],
        "Sharpe Ratio": [stats["sharpe_ratio"]]
    })

DISPLAY_NAME_MAP = {
    "WESTERN BARLEY": ("Barley", "WA"),
    "BUTTER (CASH)": ("Butter", "02"),
    "CANOLA": ("Canola", "WC"),
    "COCOA": ("Cocoa", "CC"),
    "COFFEE 'C'": ("Coffee", "KC"),
    "CORN": ("Corn", "C-"),
    "COTTON #2": ("Cotton", "CT"),
    "LUMBER": ("Lumber", "LB"),
    "OATS": ("Oats", "O-"),
    "ORANGE JUICE (FCOJ-A)": ("Orange juice", "JO"),
    "RICE (ROUGH)": ("Rough rice", "RR"),
    "SOYBEAN MEAL": ("Soybean meal", "SM"),
    "SOYBEANS": ("Soybeans", "S-"),
    "WHEAT": ("Wheat", "W-"),
    "CRUDE OIL (LIGHT SWEET)": ("Crude oil", "CL"),
    "GASOLINE RBOB": ("Gasoline", "RB"),
    "HEATING OIL (NEW YORK)": ("Heating oil", "HO"),
    "Mont Belvieu LDH Propane (OPIS) Swap Pit":("Propane", "PN"),
    "NATURAL GAS": ("Natural gas", "NG"),
    "FEEDER CATTLE COMP.": ("Feeder cattle", "FC"),
    "LEAN HOGS COMP.": ("Lean hogs", "LH"),
    "LIVE CATTLE COMP.": ("Live cattle", "LC"),
    "GASOLINE UNLEADED (NEW YORK)": ("Unleaded gas", "HU"), 
    "ALUMINIUM": ("Aluminum", "AL"),
    "COAL": ("Coal", "CO"),  
    "COPPER (HIGH GRADE)": ("Copper", "HG"),
    "GOLD (100 OZ)": ("Gold", "GC"),
    "PALLADIUM": ("Palladium", "PA"),
    "PLATINUM": ("Platinum", "PL"),
    "SILVER (5000 OZ)": ("Silver", "SI")
}

format_dict = {
    "Basis": "{:.2f}",
    "Freq. of bw.": "{:.2f}",
    "E[Re]": "{:.2f}",
    "σ[Re]": "{:.2f}",
    "Sharpe ratio": "{:.2f}"
}


def main_summary(time_period="paper"):
    """
    A function for mapping and formatting the desired table results, as close as possible
    to the paper.

    Parameters
    ----------
    time_period : str, optional
        'paper' (default) or 'current' for the date coverage.

    Returns
    -------
    pandas.DataFrame
        Summary table for multiple commodities, containing stats like Basis, Freq. of bw., E[Re], σ[Re], Sharpe ratio.
    """
    product_list = [
        3160, 289, 3161, 1980, 2038, 3247, 1992, 361, 385, 2036,
        379, 3256, 396, 430, 1986, 2091, 2029, 2060, 3847, 2032,
        3250, 2676, 2675, 3126, 2087, 2026, 2020, 2065, 2074, 2108
    ]
    summary_table = pd.DataFrame(columns=[
        "Commodity",
        "Contract Code",
        "N",
        "Basis",
        "Freq. of Backwardation (%)",
        "E(Re) (Mean Annual Excess Return)",
        "σ(Re) (Std Dev of Excess Return)",
        "Sharpe Ratio"
    ])
    for code in product_list:
        row = process_single_product(code, time_period)
        if row is not None:
            row["Sector"] = sector_map.get(code, "")
            summary_table = pd.concat([summary_table, row], ignore_index=True)
    if time_period == "current":
        summary_table = summary_table[~(
            summary_table["Commodity"].str.lower().str.contains("mont belvieu", na=False) 
            & (summary_table["Contract Code"] != 3847)
        )]
    summary_table.rename(columns={
        "Freq. of Backwardation (%)": "Freq. of bw.",
        "E(Re) (Mean Annual Excess Return)": "E[Re]",
        "σ(Re) (Std Dev of Excess Return)": "σ[Re]",
        "Sharpe Ratio": "Sharpe ratio"
    }, inplace=True)
    return summary_table

def rename_for_display(df):
    """
    Formatting the Index by replacing the commodity name with a user-friendly version
    and adding a Symbol column.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing a 'Commodity' column.

    Returns
    -------
    pandas.DataFrame
        Same data with 'Commodity' replaced where applicable and a new 'Symbol' column added.
    """
    df = df.copy()
    df["Symbol"] = ""
    for i in df.index:
        orig_name = df.at[i, "Commodity"]
        if orig_name in DISPLAY_NAME_MAP:
            new_name, new_sym = DISPLAY_NAME_MAP[orig_name]
            df.at[i, "Commodity"] = new_name
            df.at[i, "Symbol"] = new_sym
    return df

def final_table(df):
    """
    This is the final format to enable the closest replication in format to the Yang Paper.

    Renames columns, applies style formatting, and sorts by Sector and Commodity.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame with columns like 'Sector', 'Commodity', 'Basis', 'E[Re]', 'Sharpe ratio', etc.

    Returns
    -------
    pandas.io.formats.style.Styler
        A styled DataFrame ready for display or export to HTML.
    """
    
    df = df.rename(columns={
        "Freq. of bw.": "Freq. of<br>bw.",
        "Sharpe ratio": "Sharpe<br>ratio"
    })

    df = rename_for_display(df)
    df = df[["Sector","Commodity","Symbol","N","Basis","Freq. of<br>bw.","E[Re]","σ[Re]","Sharpe<br>ratio"]]
    df.sort_values(["Sector","Commodity"], inplace=True)
    df.reset_index(drop=True, inplace=True)

    df["Sector"] = df["Sector"].mask(df["Sector"].eq(df["Sector"].shift()))
    df["Sector"].fillna("", inplace=True)

    # decimal formatting
    format_dict_local = {
        "N": "{:.0f}",
        "Basis": "{:.2f}",
        "Freq. of<br>bw.": "{:.2f}",
        "E[Re]": "{:.2f}",
        "σ[Re]": "{:.2f}",
        "Sharpe<br>ratio": "{:.2f}"
    }
    df_styled = df.style.format(format_dict_local)

    e_col = df.columns.get_loc("E[Re]")
    s_col = df.columns.get_loc("σ[Re]")
    
    table_styles = [
        # Changing the font and type of header
        {
            "selector": "th",
            "props": [
                ("font-family", "Cambria, serif"),
                ("font-size", "11pt"),
                ("font-weight", "bold"),
                ("vertical-align", "top"),
                ("background-color", "#ffffff"),
                ("border-bottom", "1px solid black"),
                ("line-height", "1.2"),
                ("padding", "4px")
            ]
        },
        # Make E[Re] and σ[Re] headers not bold
        {
            "selector": f"th.col{e_col}, th.col{s_col}",
            "props": [("font-weight", "normal")]
        },
        {
            "selector": "td",
            "props": [
                ("font-family", "'Times New Roman', serif"),
                ("font-size", "11pt"),
                ("background-color", "#ffffff"),
                ("border", "0px"),
                ("line-height", "1.2"),
                ("padding", "4px")
            ]
        },
        {
            "selector": ".row_heading, .blank",
            "props": [("display", "none")]
        }
    ]
    df_styled = df_styled.set_table_styles(table_styles, overwrite=False)
    return df_styled


if __name__ == "__main__":
    table_paper = main_summary(time_period="paper")
    table_current = main_summary(time_period="current")

    final_paper = final_table(table_paper)
    final_current = final_table(table_current)

    print("=== PAPER PERIOD ===")
    print(final_paper.to_string())
    print()
    print("=== CURRENT PERIOD ===")
    print(final_current.to_string())
    print()
