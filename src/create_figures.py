"""
This script is used to create figures (both png and tex files) for the final report.
"""


import pandas as pd
from settings import config
from pathlib import Path
import warnings
import logging
from pull_futures_data import *
from calc_format_futures_data import *
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib import cm
from matplotlib.colors import ListedColormap
import matplotlib


warnings.filterwarnings("ignore", category=FutureWarning)
logging.basicConfig(level=logging.INFO, format='%(message)s')

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

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))



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


def sector_settlement_summary_all_periods_latex(
    top_n=5, output_table_name="sector_settlement_summary"):

    """
    Converts aggregated sector settlement statistics into a LaTeX table
    and saves it as a .tex file.

    Parameters
    ----------
    top_n : int, optional
        Number of rows to display, sorted by mean settlement (default is 5).
    output_table_name : str, optional
        The base filename for the output LaTeX file (default is "sector_settlement_summary").

    Returns
    -------
    None
        Writes the LaTeX table to the specified .tex file.
    """

    try:
        df_all = load_combined_futures_data()
        if df_all.empty:
            logging.warning("No data found from WRDS or local file.")
            return

        monthly_df = futures_series_to_monthly(df_all)
        monthly_df["Sector"] = monthly_df["product_code"].map(sector_map)
        monthly_df = monthly_df.dropna(subset=["Sector"])
        if monthly_df.empty:
            logging.warning("No valid monthly data with Sectors available.")
            return

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
                    "count": "Observations",
                }
            )
        )
        agg_stats = agg_stats.sort_values(by="Mean Settlement", ascending=False).head(top_n)

        if agg_stats.empty:
            logging.warning("No aggregated data to display.")
            return

        # Apply formatting
        format_dict = {
            "Mean Settlement": "{:.2f}",
            "Std Settlement": "{:.2f}",
            "Min Settlement": "{:.2f}",
            "Max Settlement": "{:.2f}",
            "Observations": "{:.0f}",
        }
        for col, fmt in format_dict.items():
            agg_stats[col] = agg_stats[col].apply(lambda x: fmt.format(x))

        # Convert DataFrame to LaTeX string
        latex_table_string = agg_stats.to_latex(index=False, escape=True)

        # Define output file path
        output_file_path = OUTPUT_DIR / f"{output_table_name}.tex"

        # Write to file
        with open(output_file_path, "w") as text_file:
            text_file.write(latex_table_string)

        logging.info(f"LaTeX table for {output_table_name} successfully saved!")

    except Exception as e:
        logging.error(f"An error occurred: {e}")

def paper_table1_replication_latex():
    """
    Generates LaTeX tables for the paper and current time period data and saves them
    as .tex files.

    Parameters
    ----------
    None

    Returns
    -------
    None
        Writes two .tex files, one for the paper period table and one for the current period table.
    """
    try:
        # Generate tables
        table_paper = main_summary(time_period="paper")
        table_current = main_summary(time_period="current")

        # Define output file paths
        output_files = {
            "paper_table1_replication_paper.tex": table_paper,
            "paper_table1_replication_current.tex": table_current
        }

        column_replacements = {
            "σ[Re]": r"sigma[Re]",
            "E[Re]": r"E[Re]"
        }

        for filename, df in output_files.items():
            output_path = OUTPUT_DIR / filename

            # Format numbers before saving
            format_dict = {
                "contrcode": "{:.0f}",
                "N": "{:.0f}",
                "Basis": "{:.2f}",
                "Freq. of bw.": "{:.2f}",
                "E[Re]": "{:.2f}",
                "σ[Re]": "{:.2f}",
                "Sharpe ratio": "{:.2f}"
            }

            for col, fmt in format_dict.items():
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: fmt.format(x))

            df.rename(columns=column_replacements, inplace=True)
            df["Commodity"] = df["Commodity"].str.replace("#", r"\#", regex=False)

            # Convert to LaTeX and save
            latex_table_string = df.to_latex(index=False, escape=False, column_format="lcccccccc")#df.to_latex(index=False, escape=True, column_format="lccccccc")
            
            with open(output_path, "w") as text_file:
                text_file.write(latex_table_string)

            logging.info(f"LaTeX table saved: {output_path}")

    except Exception as e:
        logging.error(f"An error occurred while generating LaTeX tables: {e}")


def plot_all_commodities_settlement_time_series_png(
    main_title="Monthly Settlement Prices by Commodity",
    caption_text=(""
        
    ),
    figure_size=(16, 9),
    legend_columns=1,
    output_file_name="all_commodities_settlement.png"
):
    """
    Plots a multi-line time series of monthly settlement prices for all commodities
    and saves the plot as a PNG file.

    Parameters
    ----------
    main_title : str, optional
        Chart title.
    caption_text : str, optional
        Descriptive text placed below the chart.
    figure_size : tuple of (int, int), optional
        Size of the figure in inches (default is (16, 9)).
    legend_columns : int, optional
        Number of columns used in the legend (default is 1).
    output_file_name : str, optional
        Filename for the saved plot.

    Returns
    -------
    None
        Saves the generated chart as a PNG image.
    """

    try:
        df_all = load_combined_futures_data()
        if df_all.empty:
            logging.warning("No data found from WRDS or local file.")
            return

        monthly_df = futures_series_to_monthly(df_all)
        if monthly_df.empty:
            logging.warning("No monthly data available after processing.")
            return

        grouped = (
            monthly_df.groupby(["obs_period", "product_code"])["settlement"]
            .mean()
            .reset_index()
        )
        if grouped.empty:
            logging.warning("No monthly settlement data to plot.")
            return

        pivot_df = grouped.pivot(
            index="obs_period",
            columns="product_code",
            values="settlement"
        ).sort_index()

        numeric_cols = pivot_df.select_dtypes(include=[np.number]).columns
        if pivot_df.empty or len(numeric_cols) == 0:
            logging.warning("No numeric settlement data to plot.")
            return

        rename_dict = {
            code: PRODUCT_NAME_MAP.get(code, f"Code {code}")
            for code in pivot_df.columns
        }
        pivot_df.rename(columns=rename_dict, inplace=True)

        plt.rcParams["font.family"] = "Times New Roman"
        plt.rcParams["font.size"] = 11
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
            fontsize=22,
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
            fontsize=18
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
            fontsize=18,
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
            fontsize=18
        )

        # Save plot as PNG
        output_file_path = OUTPUT_DIR / output_file_name
        fig.savefig(output_file_path, dpi=300, bbox_inches="tight")

        logging.info(f"Plot saved successfully as {output_file_path}")

    except Exception as e:
        logging.error(f"An error occurred while generating the plot: {e}")



def plot_commodity_correlation_heatmap_pairwise_png(
    main_title="Commodity Correlation Heatmap (Settlement Prices)",
    caption_text=(""
        
    ),
    figure_size=(14, 12),
    annot=True,
    min_coverage=200,
    exclude_codes=None,
    output_file_name="commodity_correlation_heatmap.png"
):
    """
    Generates a correlation heatmap of settlement prices for the specified commodities
    and saves it as a PNG file.

    Parameters
    ----------
    main_title : str, optional
        Title for the heatmap.
    caption_text : str, optional
        Additional explanatory text.
    figure_size : tuple of (int, int), optional
        Size of the figure in inches (default is (14, 12)).
    annot : bool, optional
        Whether to display correlation values in each cell (default is True).
    min_coverage : int, optional
        Minimum monthly observations required (default is 200).
    exclude_codes : set of int, optional
        Commodity codes to exclude from correlation.
    output_file_name : str, optional
        Filename for the saved plot.

    Returns
    -------
    None
        Saves the generated heatmap as a PNG image.
    """

    try:
        if exclude_codes is None:
            exclude_codes = set()

        df_all = load_combined_futures_data()
        if df_all.empty:
            logging.warning("No data found from WRDS or local file.")
            return

        monthly_df = futures_series_to_monthly(df_all)
        if monthly_df.empty:
            logging.warning("No monthly data after converting from daily.")
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
            logging.warning(f"All commodities were dropped (coverage < {min_coverage} or exclude_codes used).")
            return

        monthly_df["date_dt"] = monthly_df["obs_period"].apply(lambda p: p.asfreq("M").to_timestamp())
        pivot_df = monthly_df.pivot_table(
            index="date_dt",
            columns="product_code",
            values="settlement",
            aggfunc="mean"
        )
        if pivot_df.empty:
            logging.warning("After pivot, the DataFrame is empty.")
            return

        pivot_df = pivot_df.resample("M").mean().sort_index()

        rename_dict = {
            code: CORRELATION_MAP.get(code, f"Code {code}")
            for code in pivot_df.columns
        }
        pivot_df.rename(columns=rename_dict, inplace=True)

        corr_matrix = pivot_df.corr(method="pearson")
        if corr_matrix.isna().all().all():
            logging.warning("All correlations are NaN after filtering.")
            return

        corr_matrix.index.name = None
        corr_matrix.columns.name = None

        plt.rcParams["font.family"] = "Times New Roman"
        plt.rcParams["font.size"] = 11

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
        ax.set_yticklabels(ax.get_yticklabels(), rotation=0, ha="right")

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
            fontsize=20,
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
            fontsize=16
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
            fontsize=16
        )

        caption_y = x_label_y - 0.04
        fig.text(
            axis_center_x,
            caption_y,
            caption_text,
            ha="center",
            va="top",
            fontsize=14,
            fontname="Georgia"
        )

        # Save plot as PNG
        output_file_path = OUTPUT_DIR / output_file_name
        fig.savefig(output_file_path, dpi=300, bbox_inches="tight")

        logging.info(f"Correlation heatmap saved successfully as {output_file_path}")

    except Exception as e:
        logging.error(f"An error occurred while generating the heatmap: {e}")



def plot_commodity_coverage_heatmap_png(
    main_title="Coverage Heatmap: Monthly Data",
    caption_text=(""),
    figure_size=(26, 10),
    xtick_subsample=12,
    show_only_year=True,
    presence_color="#003c80",
    absence_color="#fafafa",
    output_file_name="commodity_coverage_heatmap.png"
):
    """
    Creates a coverage heatmap of monthly data for all commodities and saves it as a PNG file.

    Parameters
    ----------
    main_title : str, optional
        Title displayed above the heatmap.
    caption_text : str, optional
        Descriptive text placed below the chart.
    figure_size : tuple of (int, int), optional
        Size of the figure in inches (default is (26, 10)).
    xtick_subsample : int, optional
        Spacing for x-axis tick labels (default is 12).
    show_only_year : bool, optional
        If True, display only the year on x-axis ticks.
    presence_color : str, optional
        Color used for "has data" cells (default is "#003c80").
    absence_color : str, optional
        Color used for "no data" cells (default is "#fafafa").
    output_file_name : str, optional
        Filename for the saved plot.

    Returns
    -------
    None
        Saves the generated heatmap as a PNG image.
    """

    try:
        # Adds broilers to show that it is missing
        CORRELATION_MAP[19] = "Broilers (BR)"

        df_all = load_combined_futures_data()
        if df_all.empty:
            logging.warning("No data found from WRDS or local file.")
            return

        monthly_df = futures_series_to_monthly(df_all)
        if monthly_df.empty:
            logging.warning("After converting daily → monthly, no data remains.")
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
            fontsize=22,
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
            fontsize=18
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
            fontsize=18
        )

        caption_y = ax_bbox.y0 - 0.12
        fig.text(
            axis_center_x,
            caption_y,
            caption_text,
            ha="center",
            va="top",
            fontname="Georgia",
            fontsize=18
        )

        # Save plot as PNG
        output_file_path = OUTPUT_DIR / output_file_name
        fig.savefig(output_file_path, dpi=300, bbox_inches="tight")

        logging.info(f"Coverage heatmap saved successfully as {output_file_path}")

    except Exception as e:
        logging.error(f"An error occurred while generating the heatmap: {e}")


if __name__ == "__main__":
    # Define expected output files
    output_files = {
        "sector_settlement_summary.tex": sector_settlement_summary_all_periods_latex,
        "paper_table1_replication_paper.tex": paper_table1_replication_latex,
        "paper_table1_replication_current.tex": paper_table1_replication_latex,
        "all_commodities_settlement.png": plot_all_commodities_settlement_time_series_png,
        "commodity_correlation_heatmap.png": plot_commodity_correlation_heatmap_pairwise_png,
        "commodity_coverage_heatmap.png": plot_commodity_coverage_heatmap_png
    }

    for filename, func in output_files.items():
        output_path = OUTPUT_DIR / filename
        if not output_path.exists():
            logging.info(f"Generating {filename}...")
            func()  
        else:
            logging.info(f"Skipping {filename}, already exists.")