""" Functions designed for alternative analyses outside of replicating the table
within the paper"""

import os
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm
from pull_futures_data import *
from calc_format_futures_data import *
from IPython.display import HTML
import seaborn as sns
import warnings
import matplotlib
from matplotlib.colors import ListedColormap

warnings.filterwarnings(
    "ignore",
    message=".*The get_cmap function was deprecated.*",
    category=matplotlib.MatplotlibDeprecationWarning
)
warnings.filterwarnings(
    "ignore",
    message=".*Glyph.*missing from font.*|.*The get_cmap function was deprecated.*",
    category=UserWarning
    )
warnings.filterwarnings(
    "ignore",
    message=".*Glyph.*missing from font.*|.*The get_cmap function was deprecated.*",
    category=matplotlib.MatplotlibDeprecationWarning
    )

DATA_DIR = Path("../_data")
DATA_DIR.mkdir(exist_ok=True)
DATA_FILE = DATA_DIR / "df_all.parquet"


CORRELATION_MAP = {
    3160: "Barley (WA)",
    289:  "Butter (02)",
    3161: "Canola (WC)",
    1980: "Cocoa (CC)",
    2038: "Coffee (KC)",
    3247: "Corn (C-)",
    1992: "Cotton (CT)",
    361:  "Lumber (LB)",
    385:  "Oats (O-)",
    2036: "Orange juice (JO)",
    379:  "Rough rice (RR)",
    3256: "Soybean meal (SM)",
    396:  "Soybeans (S-)",
    430:  "Wheat (W-)",
    1986: "Crude oil (CL)",
    2091: "Gasoline (RB)",
    2029: "Heating oil (HO)",
    2060: "Natural gas (NG)",
    3847: "Propane (PN)",
    2032: "Unleaded gas (HU)",
    3250: "Feeder cattle (FC)",
    2676: "Lean hogs (LH)",
    2675: "Live cattle (LC)",
    3126: "Aluminum (AL)",
    2087: "Coal (CO)",
    2026: "Copper (HG)",
    2020: "Gold (GC)",
    2065: "Palladium (PA)",
    2074: "Platinum (PL)",
    2108: "Silver (SI)"
}

PRODUCT_NAME_MAP = {
    3160: "Barley",               
    289:  "Butter",               
    3161: "Canola",               
    1980: "Cocoa",                
    2038: "Coffee",               
    3247: "Corn",                 
    1992: "Cotton",               
    361:  "Lumber",               
    385:  "Oats",                 
    2036: "Orange Juice",         
    379:  "Rough Rice",           
    3256: "Soybean Meal",         
    396:  "Soybeans",             
    430:  "Wheat",                
    1986: "Crude Oil",            
    2091: "Gasoline",             
    2029: "Heating Oil",          
    2060: "Natural Gas",          
    3847: "Propane",              
    2032: "Unleaded Gas",         
    3250: "Feeder Cattle",        
    2676: "Lean Hogs",            
    2675: "Live Cattle",          
    3126: "Aluminum",             
    2087: "Coal",                 
    2026: "Copper",               
    2020: "Gold",                 
    2065: "Palladium",            
    2074: "Platinum",             
    2108: "Silver"                
}


def plot_all_commodities_settlement_time_series(
    main_title="Monthly Settlement Prices by Commodity",
    caption_text=(
        "Illustrating each commodity's monthly settlement trend (1970-2025). Some commodities have\n"
        "missing data in WRDS (e.g., aluminum). Cocoa and aluminum appear to be drivers of higher\n"
        "deviation among settlement prices."
    ),
    figure_size=(16, 9),
    legend_columns=1
):
    """
    Plots a multi-line time series of monthly settlement prices for all commodities
    (labeled by user-friendly names). Each commodity gets a unique color+dash style.
    The title, axes, and caption are placed using a bounding-box approach so they
    align neatly with the main axis. 
    """

    df_all = load_combined_futures_data()
    if df_all.empty:
        print("No data found from WRDS or local file.")
        return

    monthly_df = futures_series_to_monthly(df_all)
    if monthly_df.empty:
        print("No monthly data available after processing.")
        return

    grouped = (
        monthly_df.groupby(["obs_period", "product_code"])["settlement"]
        .mean()
        .reset_index()
    )
    if grouped.empty:
        print("No monthly settlement data to plot.")
        return

    pivot_df = grouped.pivot(
        index="obs_period",
        columns="product_code",
        values="settlement"
    ).sort_index()

    numeric_cols = pivot_df.select_dtypes(include=[np.number]).columns
    if pivot_df.empty or len(numeric_cols) == 0:
        print("No numeric settlement data to plot.")
        return

    rename_dict = {
        code: PRODUCT_NAME_MAP.get(code, f"Code {code}")
        for code in pivot_df.columns
    }
    pivot_df.rename(columns=rename_dict, inplace=True)

    plt.rcParams["font.family"] = "Times New Roman"
    plt.rcParams["font.size"]   = 11
    fig, ax = plt.subplots(figsize=figure_size)

    commodity_names = pivot_df.columns.tolist()
    num_commodities = len(commodity_names)

    # Color list setup
    if num_commodities <= 20:
        cmap = cm.get_cmap("tab20", num_commodities)
    else:
        cmap = cm.get_cmap("nipy_spectral", num_commodities)
    color_list = [cmap(i) for i in range(num_commodities)]

    # Setting up distinct line styles
    style_list = ["-", "--", "-.", ":", (0, (3, 5, 1, 5)), (0, (3, 1, 1, 1))]

    for i, commodity in enumerate(commodity_names):
        color = color_list[i]
        linestyle = style_list[i % len(style_list)]
        ax.plot(
            pivot_df.index.to_timestamp(),  # Convert Period -> Timestamp
            pivot_df[commodity],
            label=commodity,
            color=color,
            linestyle=linestyle,
            linewidth=2,
            alpha=0.8
        )

    ax.set_title("")
    ax.set_xlabel("")
    ax.set_ylabel("")

    # Grid
    ax.grid(True, linestyle="--", alpha=0.5)

    # Construct legend with a box
    leg = ax.legend(
        title="Commodity",
        bbox_to_anchor=(1.02, 1),
        loc="upper left",
        ncol=legend_columns,
        frameon=True,            
        edgecolor="black",       
        facecolor="white",       
        prop={"size": 11}        
    )
    leg.set_title(leg.get_title().get_text(), prop={"size": 12, "weight": "bold"})

    fig.tight_layout()
    fig.canvas.draw()

    # Compute bounding box of the main axis (in figure coords)
    ax_bbox = ax.get_position()
    axis_center_x = 0.5 * (ax_bbox.x0 + ax_bbox.x1)

    title_y = ax_bbox.y1 + 0.04
    fig.text(
        axis_center_x,
        title_y,
        main_title,
        ha="center",
        va="bottom",
        fontname="Georgia",
        fontsize=18,      
        fontweight="bold"
    )

    x_label_y = ax_bbox.y0 - 0.06
    fig.text(
        axis_center_x,
        x_label_y,
        "Monthly Observation Period",
        ha="center",
        va="top",
        fontname="Georgia",
        fontsize=14       
    )

    # Y-axis label
    label_x = ax_bbox.x0 - 0.07
    label_y = 0.5 * (ax_bbox.y0 + ax_bbox.y1)
    fig.text(
        label_x,
        label_y,
        "Settlement Price",
        ha="center",
        va="center",
        rotation=90,
        fontname="Georgia",
        fontsize=14,      
        fontweight="normal"
    )

    caption_y = ax_bbox.y0 - 0.12
    fig.text(
        axis_center_x,
        caption_y,
        caption_text,
        ha="center",
        va="top",
        fontname="Georgia",
        fontsize=11
    )

    plt.show()


def sector_settlement_summary_all_periods(
    top_n=5,
    title="Combined Period Settlement Summary",
    caption=(
        "Above are some summary statistics relating to each sector throughout the whole period (1970-2025). "
        "Metals have the widest range of settlement prices, likely due to the difference in prices of commodities "
        "like coal and gold. Agricultural is the next widest range of settlement prices, likely due to the size/quantity "
        "of the contract with commodities such as lumber and corn. This table is meant to give the user a grasp "
        "of the prices and the deviation of those prices."
    )
):
    """
    Builds a styled HTML table of aggregated settlement stats by Sector, for the top_n
    sorted by mean settlement. We prepend a truly centered title by placing both the
    title and table within an inline-block container that auto-sizes to the table width.

    If no data is found, returns an HTML message.
    """

    df_all = load_combined_futures_data()
    if df_all.empty:
        return HTML("<p>No data found from WRDS or local file.</p>")

    monthly_df = futures_series_to_monthly(df_all)
    monthly_df["Sector"] = monthly_df["product_code"].map(sector_map)
    monthly_df = monthly_df.dropna(subset=["Sector"])
    if monthly_df.empty:
        return HTML("<p>No valid monthly data with Sectors available.</p>")

    agg_stats = (
        monthly_df.groupby("Sector")["settlement"]
        .agg(["mean", "std", "min", "max", "count"])
        .reset_index()
        .rename(
            columns={
                "mean": "Mean Settlement",
                "std": "Std Settlement",
                "min": "Min Settlement",
                "max": "Max Settlement",
                "count": "Observations"
            }
        )
    )
    agg_stats = agg_stats.sort_values(by="Mean Settlement", ascending=False).head(top_n)

    if agg_stats.empty:
        return HTML("<p>No aggregated data to display.</p>")

    format_dict = {
        "Mean Settlement": "{:.2f}",
        "Std Settlement": "{:.2f}",
        "Min Settlement": "{:.2f}",
        "Max Settlement": "{:.2f}",
        "Observations": "{:.0f}"
    }

    df_styled = agg_stats.style.format(format_dict, na_rep="").set_caption(caption)

    # Define custom CSS for the table
    table_styles = [
        {
            "selector": "table",
            "props": [
                ("border-collapse", "collapse"),
                ("border", "none"),
                ("display", "inline-block"),  
                ("vertical-align", "top"),
            ]
        },
        {
            "selector": "caption",
            "props": [
                ("caption-side", "bottom"),
                ("font-family", "'Times New Roman', serif"),
                ("font-size", "10pt"),
                ("font-weight", "normal"),
                ("text-align", "center"),
                ("margin-top", "8px"),
                ("margin-bottom", "4px"),
            ]
        },
        {
            "selector": "th",
            "props": [
                ("background-color", "#f2f2f2"),
                ("font-family", "Georgia, serif"),
                ("font-size", "12pt"),
                ("font-weight", "bold"),
                ("border-top", "2px solid black"),
                ("border-bottom", "2px solid black"),
                ("text-align", "center"),
                ("padding", "6px 8px"),
            ]
        },
        {
            "selector": "td",
            "props": [
                ("font-family", "'Times New Roman', serif"),
                ("font-size", "11pt"),
                ("text-align", "center"),
                ("padding", "6px 8px"),
                ("border", "0px"),
            ]
        },
        {
            "selector": "tbody tr:nth-child(even)",
            "props": [("background-color", "#fafafa")]
        },
        {
            "selector": ".row_heading, .blank",
            "props": [("display", "none")]
        }
    ]

    df_styled = df_styled.set_table_styles(table_styles)
    table_html = df_styled.to_html()

    full_html = f"""
    <div style="text-align:center;">
      <div style="display:inline-block;">
        <div style="font-family:Georgia, serif; font-size:16pt; font-weight:bold; margin-bottom:8px;">
          {title}
        </div>
        {table_html}
      </div>
    </div>
    """

    return HTML(full_html)

def plot_commodity_correlation_heatmap_pairwise(
    main_title="Commodity Correlation Heatmap (Settlement Prices)",
    caption_text=(
        "Pairwise correlation of monthly settlement prices for selected commodities.\n"
        "We drop short-coverage or explicitly excluded commodities so the rest can show meaningful correlations.\n"
        "As we can see, the majority of correlations are positive or minimal, but there are a few\n"
        "commodities with noticeably negative correlations (e.g., silver & natural gas vs. butter)."
    ),
    figure_size=(14, 12),
    annot=True,
    min_coverage=200,
    exclude_codes=None
):
    """
    Generates a correlation heatmap of settlement prices for the specified commodities,
    excluding those with insufficient coverage or explicitly removed..
    """

    if exclude_codes is None:
        exclude_codes = set()

    df_all = load_combined_futures_data()
    if df_all.empty:
        print("No data found from WRDS or local file.")
        return

    monthly_df = futures_series_to_monthly(df_all)
    if monthly_df.empty:
        print("No monthly data after converting from daily.")
        return

    coverage_df = (
        monthly_df.groupby("product_code")["obs_period"]
        .nunique()
        .reset_index(name="monthly_count")
    )
    drop_codes = coverage_df.loc[coverage_df["monthly_count"] < min_coverage, "product_code"]
    drop_codes = set(drop_codes).union(exclude_codes)
    monthly_df = monthly_df[~monthly_df["product_code"].isin(drop_codes)]
    if monthly_df.empty:
        print(
            f"All commodities were dropped (coverage < {min_coverage} or exclude_codes used)."
        )
        return

    monthly_df["date_dt"] = monthly_df["obs_period"].apply(lambda p: p.asfreq("M").to_timestamp())
    pivot_df = monthly_df.pivot_table(
        index="date_dt",
        columns="product_code",
        values="settlement",
        aggfunc="mean"
    )
    if pivot_df.empty:
        print("After pivot, the DataFrame is empty.")
        return

    pivot_df = pivot_df.resample("M").mean().sort_index()

    rename_dict = {
        code: CORRELATION_MAP.get(code, f"Code {code}")
        for code in pivot_df.columns
    }
    pivot_df.rename(columns=rename_dict, inplace=True)

    corr_matrix = pivot_df.corr(method="pearson")
    if corr_matrix.isna().all().all():
        print("All correlations are NaN after filtering.")
        return

    corr_matrix.index.name = None
    corr_matrix.columns.name = None

    plt.rcParams["font.family"] = "Times New Roman"
    plt.rcParams["font.size"]   = 11

    fig, ax = plt.subplots(figsize=figure_size)
    sns.set_style("whitegrid", {"axes.edgecolor": ".8", "grid.color": ".8"})
    sns.set_context("notebook", font_scale=0.9)

    ax.set_title("")
    ax.set_xlabel("")
    ax.set_ylabel("")

    heat = sns.heatmap(
        corr_matrix,
        cmap="RdBu",
        center=0,
        annot=annot,
        annot_kws={"size": 8},
        fmt=".2f",
        square=True,
        linewidths=0.5,
        cbar_kws={"shrink": 0.8},
        ax=ax
    )

    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")
    ax.set_yticklabels(ax.get_yticklabels(), rotation=0,  ha="right")

    fig.tight_layout()
    fig.canvas.draw()

    ax_bbox = ax.get_position()
    axis_center_x = 0.5 * (ax_bbox.x0 + ax_bbox.x1)

    title_y = ax_bbox.y1 + 0.04
    fig.text(
        axis_center_x,
        title_y,
        main_title,
        ha="center",
        va="bottom",
        fontname="Georgia",
        fontsize=16,
        fontweight="bold"
    )

    x_label_y = ax_bbox.y0 - 0.085
    fig.text(
        axis_center_x,
        x_label_y,
        "Commodity (Symbol)",
        ha="center",
        va="top",
        fontname="Georgia",
        fontsize=13
    )

    label_x = ax_bbox.x0 - 0.1
    label_y = 0.5 * (ax_bbox.y0 + ax_bbox.y1)
    fig.text(
        label_x,
        label_y,
        "Commodity (Symbol)",
        ha="center",
        va="center",
        rotation=90,
        fontname="Georgia",
        fontsize=13
    )

    caption_y = x_label_y - 0.04
    fig.text(
        axis_center_x,
        caption_y,
        caption_text,
        ha="center",
        va="top",
        fontsize=10,
        fontname="Georgia"
    )

    plt.show()



def plot_commodity_coverage_heatmap(
    main_title="Coverage Heatmap: Monthly Data",
    caption_text=(
        "Each row is a commodity (with its symbol). Each column corresponds to one monthly period.\n"
        "Dark squares represent coverage; light squares represent no data.\n"
        "As we can see there is no data for  Broilers and for some reason there is a split in \n"
        "the data in 2009. Overall, we can see the misalignment of data and dates."
    ),
    figure_size=(26, 10),
    xtick_subsample=12,
    show_only_year=True,
    presence_color="#003c80",
    absence_color="#fafafa"
):
    """
    Creates a "block-style" coverage heatmap for all commodities in CORRELATION_MAP,
    ensuring each appears even if it has zero coverage. We manually place the title/axes
    so they're centered over the plotted heatmap.
    """

    # Adds broilers to show that it is missing
    CORRELATION_MAP[19] = "Broilers (BR)"

    df_all = load_combined_futures_data()
    if df_all.empty:
        print("No data found from WRDS or local file.")
        return

    monthly_df = futures_series_to_monthly(df_all)
    if monthly_df.empty:
        print("After converting daily → monthly, no data remains.")
        return

    coverage = monthly_df[["product_code", "obs_period"]].drop_duplicates()
    coverage["has_data"] = 1

    coverage_pivot = coverage.pivot(
        index="product_code",
        columns="obs_period",
        values="has_data"
    ).fillna(0)

    coverage_pivot = coverage_pivot.reindex(
        sorted(coverage_pivot.columns),
        axis=1
    )

    all_defined_codes = set(CORRELATION_MAP.keys())
    pivot_codes = set(coverage_pivot.index)
    missing_codes = all_defined_codes - pivot_codes
    for code in missing_codes:
        coverage_pivot.loc[code] = 0

    new_index = [
        CORRELATION_MAP.get(code, f"Code {code}")
        for code in coverage_pivot.index
    ]
    coverage_pivot.index = new_index
    coverage_pivot.sort_index(inplace=True)

    cmap = ListedColormap([absence_color, presence_color])

    plt.rcParams["font.family"] = "Times New Roman"
    plt.rcParams["font.size"] = 11

    fig, ax = plt.subplots(figsize=figure_size)

    sns.set_style("white", {"axes.edgecolor": ".8", "grid.color": ".8"})
    sns.set_context("notebook", font_scale=0.95)

    heatmap = sns.heatmap(
        coverage_pivot,
        cmap=cmap,
        vmin=0, vmax=1,
        cbar=True,
        square=False,
        linewidths=0,
        linecolor=None,
        ax=ax
    )

    cbar = heatmap.collections[0].colorbar
    cbar.set_ticks([0.25, 0.75])
    cbar.set_ticklabels(["No Data", "Has Data"])
    cbar.outline.set_linewidth(0.5)

    all_months = coverage_pivot.columns
    n_cols = len(all_months)
    step = max(1, xtick_subsample)
    x_positions = np.arange(0, n_cols, step)

    x_labels = []
    for i in x_positions:
        period_val = all_months[i] 
        if show_only_year:
            x_labels.append(str(period_val.year))
        else:
            x_labels.append(str(period_val))

    ax.set_xticks(x_positions)
    ax.set_xticklabels(x_labels, rotation=45, ha="right")

    plt.setp(ax.get_yticklabels(), rotation=0, ha="right")

    ax.set_title("")
    ax.set_xlabel("")
    ax.set_ylabel("")

    fig.tight_layout()
    fig.canvas.draw()

    ax_bbox = ax.get_position()
    axis_center_x = 0.5 * (ax_bbox.x0 + ax_bbox.x1)

    title_y = ax_bbox.y1 + 0.035
    fig.text(
        axis_center_x,
        title_y,
        main_title,
        ha="center",
        va="bottom",
        fontname="Georgia",
        fontsize=18,
        fontweight="bold"
    )

    x_label_y = ax_bbox.y0 - 0.06
    fig.text(
        axis_center_x,
        x_label_y,
        "Monthly Period" if show_only_year else "Monthly Period (YYYY-MM)",
        ha="center",
        va="top",
        fontname="Georgia",
        fontsize=16
    )

    label_x = ax_bbox.x0 - 0.06
    label_y = 0.5 * (ax_bbox.y0 + ax_bbox.y1)
    fig.text(
        label_x,
        label_y,
        "Commodity (Symbol)",
        ha="center",
        va="center",
        rotation=90,
        fontname="Georgia",
        fontsize=16
    )

    caption_y = ax_bbox.y0 - 0.12
    fig.text(
        axis_center_x,
        caption_y,
        caption_text,
        ha="center",
        va="top",
        fontname="Georgia",
        fontsize=14
    )

    plt.show()




