from pull_futures_data import *
"""
def extract_prompt_and_12m_settlements(df_month):
    
    #Pick earliest row as prompt and last row as 12-month.
    
    df_month = df_month.sort_values(by="date_")
    prompt = df_month.iloc[0]
    last_12m = df_month.iloc[-1] if len(df_month) > 1 else None
    return pd.Series({
        "prompt_settlement": prompt["settlement"],
        "contract_12m_settlement": last_12m["settlement"] if last_12m is not None else np.nan
    })
    """
# Parse the contrdate strings into a monthly Period (e.g. '03/03' -> 2003-03)
def parse_contrdate(c):
    c = c.replace('/', '')  # handle both 'MMYY' & 'MM/YY'
    mm = int(c[:2])
    yy = int(c[2:])
    # crude century switch: assume YY<50 => 20xx, else 19xx
    year = (2000 + yy) if yy < 50 else (1900 + yy)
    return pd.Period(freq='M', year=year, month=mm)

def extract_prompt_and_12m_settlements(monthly_df):
    """
    NEED TO ADD DOCSTRING
    """

    # Make a copy so we don’t overwrite the original
    df = monthly_df.copy()
    
    df['contr_period'] = df['contrdate'].apply(parse_contrdate)
    df['obs_period']   = df['date_'].dt.to_period('M')

    # Filter the DataFrame to only include rows where contr_period is 1 month or 12 months ahead of obs_period
    #df = df[(df['contr_period'] == df['obs_period'] + 1) | (df['contr_period'] == df['obs_period'] + 11)]

    #df['prompt_settlement'] = df[df['contr_period'] == df['obs_period'] + 1]['settlement']
    df = df.sort_values(by=['obs_period', 'contr_period'])
    temp = df.set_index(["obs_period", "contr_period"])["settlement"]

    basis_df = pd.DataFrame(index=df['obs_period'].unique())

    basis_df["prompt_settlement"] = basis_df.index.to_series().apply(
        lambda op: temp.get((op, op + 1), float("nan"))  # or np.nan
    )

    basis_df["11mth_settlement"] = basis_df.index.to_series().apply(
        lambda op: temp.get((op, op + 11), float("nan"))  # or np.nan
    )

    #basis_df['basis'] = (np.log(basis_df['prompt_settlement']) - np.log(basis_df['11mth_settlement'])) / 11
    return basis_df



def compute_futures_stats(monthly_df):
    """
    Compute basis, frequency of backwardation, and basic returns stats.
    """
    monthly_df["basis"] = (np.log(monthly_df["prompt_settlement"]) - np.log(monthly_df["contract_12m_settlement"])) / 11
    freq_bw = (monthly_df["basis"] > 0).mean() * 100
    monthly_df["excess_return"] = monthly_df["prompt_settlement"].pct_change() * 100
    er_mean = monthly_df["excess_return"].mean()
    er_std = monthly_df["excess_return"].std()
    sharpe = er_mean / er_std if er_std != 0 else np.nan
    n_valid = len(monthly_df.dropna())
    return {
        "N": n_valid,
        "mean_basis": monthly_df["basis"].mean(),
        "freq_bw": freq_bw,
        "excess_return_mean": er_mean,
        "excess_return_std": er_std,
        "sharpe_ratio": sharpe
    }

def process_single_product(product_contract_code, time_period='paper'):
    """
    Compute stats for a single product code.
    """
    info_df = fetch_wrds_contract_info(product_contract_code, time_period)
    if info_df.empty:
        return None
    futcodes = tuple(info_df["futcode"].unique())
    data_contracts = fetch_wrds_fut_contract(futcodes, time_period)
    if data_contracts.empty:
        return None
    #data_contracts["month"] = data_contracts["date_"].dt.to_period("M")
    
    monthly_df = data_contracts.groupby("month").apply(extract_prompt_and_12m_settlements, include_groups=False).reset_index()
    stats = compute_futures_stats(monthly_df)
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
    "BUTTER (CASH)": ("Butter", "O2"),
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

def main_summary(time_period='paper'):
    """ Formatting Function that uses multi-level indexing to display the summary table as close as possible to the paper """
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
    sector_map = {
        3160: "Agriculture",
        289: "Agriculture",
        3161: "Agriculture",
        1980: "Agriculture",
        2038: "Agriculture",
        3247: "Agriculture",
        1992: "Agriculture",
        361: "Agriculture",
        385: "Agriculture",
        2036: "Agriculture",
        379: "Agriculture",
        3256: "Agriculture",
        396: "Agriculture",
        430: "Agriculture",
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
    for code in product_list:
        row = process_single_product(code, time_period)
        if row is not None:
            row["Sector"] = sector_map.get(code, "")
            summary_table = pd.concat([summary_table, row], ignore_index=True)
    summary_table.rename(columns={
        "Freq. of Backwardation (%)": "Freq. of bw.",
        "E(Re) (Mean Annual Excess Return)": "E[Re]",
        "σ(Re) (Std Dev of Excess Return)": "σ[Re]",
        "Sharpe Ratio": "Sharpe ratio"
    }, inplace=True)
    return summary_table

def rename_for_display(df):
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
    df = rename_for_display(df)
    df = df[["Sector","Commodity","Symbol","N","Basis","Freq. of bw.","E[Re]","σ[Re]","Sharpe ratio"]]
    df = df.sort_values(["Sector","Commodity"])
    df = df.set_index(["Sector","Commodity"])
    df = df[["Symbol","N","Basis","Freq. of bw.","E[Re]","σ[Re]","Sharpe ratio"]]
    df.index.set_names(["", ""], inplace=True)
    df = df.style.format(format_dict)
    return df

if __name__ == "__main__":
    table_paper = main_summary(time_period="paper")
    table_current = main_summary(time_period="current")

    final_paper = final_table(table_paper)
    final_current = final_table(table_current)

    print("=== PAPER PERIOD ===")
    #print(final_paper.to_string(sparsify=True))
    print(final_paper.to_string())
    print()
    print("=== CURRENT PERIOD ===")
    #print(final_current.to_string(sparsify=True))
    print(final_current.to_string())
    print()