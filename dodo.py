"""
Run or update the project. This file uses the `doit` Python package. It works
like a Makefile, but is Python-based.
"""

import sys
sys.path.insert(1, "./src/")

import shutil
from pathlib import Path
from colorama import Fore, Style, init
from doit.reporter import ConsoleReporter
from os import environ
import pandas as pd
from pull_futures_data import pull_all_futures_data
from calc_format_futures_data import main_summary, final_table
from settings import config

try:
    in_slurm = environ["SLURM_JOB_ID"] is not None
except:
    in_slurm = False


class GreenReporter(ConsoleReporter):
    def write(self, stuff, **kwargs):
        doit_mark = stuff.split(" ")[0].ljust(2)
        task = " ".join(stuff.split(" ")[1:]).strip() + "\n"
        output = Fore.GREEN + doit_mark + f" dodo.py: " + task + Style.RESET_ALL
        self.outstream.write(output)

if not in_slurm:
    DOIT_CONFIG = {
        "reporter": GreenReporter,
        "backend": "sqlite3",
        "dep_file": "./.doit-db.sqlite",
    }
else:
    DOIT_CONFIG = {"backend": "sqlite3", "dep_file": "./.doit-db.sqlite"}

init(autoreset=True)

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = Path(config("OUTPUT_DIR"))

def task_config():
    """
    Ensure _data/ and _output/ directories exist
    """
    def ensure_dirs():
        DATA_DIR.mkdir(exist_ok=True, parents=True)
        OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    return {
        "actions": [ensure_dirs],
        "targets": [str(DATA_DIR), str(OUTPUT_DIR)],
    }

def task_pull_futures_data():
    """
    Pull raw futures data for 'paper' and 'current' periods, save CSVs in _data.
    """
    paper_raw = DATA_DIR / "raw_futures_paper.csv"
    current_raw = DATA_DIR / "raw_futures_current.csv"

    def pull():
        df_paper = pull_all_futures_data("paper")
        df_paper.to_csv(paper_raw, index=False)
        print(f"Saved {paper_raw} with {len(df_paper)} rows.")

        df_current = pull_all_futures_data("current")
        df_current.to_csv(current_raw, index=False)
        print(f"Saved {current_raw} with {len(df_current)} rows.")

    return {
        "actions": [pull],
        "file_dep": [],
        "targets": [paper_raw, current_raw],
        "uptodate": [True],
        "clean": True,
    }

def task_clean_futures_data():
    """
    Clean the raw CSVs , save to _data/clean_...
    """
    paper_raw = DATA_DIR / "raw_futures_paper.csv"
    current_raw = DATA_DIR / "raw_futures_current.csv"
    paper_clean = DATA_DIR / "clean_futures_paper.csv"
    current_clean = DATA_DIR / "clean_futures_current.csv"

    def clean():
        dfp = pd.read_csv(paper_raw)
        dfp.drop_duplicates(subset=["futcode","date_","settlement"], inplace=True)
        dfp.to_csv(paper_clean, index=False)
        print(f"Cleaned -> {paper_clean} with {len(dfp)} rows.")

        dfc = pd.read_csv(current_raw)
        dfc.drop_duplicates(subset=["futcode","date_","settlement"], inplace=True)
        dfc.to_csv(current_clean, index=False)
        print(f"Cleaned -> {current_clean} with {len(dfc)} rows.")

    return {
        "actions": [clean],
        "file_dep": [paper_raw, current_raw],
        "targets": [paper_clean, current_clean],
        "clean": True,
    }

def task_calc_futures_data():
    """
    Calculate final stats (paper/current) using main_summary & final_table functions.
    """
    paper_clean = DATA_DIR / "clean_futures_paper.csv"
    current_clean = DATA_DIR / "clean_futures_current.csv"
    paper_csv = DATA_DIR / "final_paper.csv"
    current_csv = DATA_DIR / "final_current.csv"
    paper_html = OUTPUT_DIR / "final_paper.html"
    current_html = OUTPUT_DIR / "final_current.html"

    def calc():
        # PAPER
        df_paper = main_summary("paper")  
        df_paper.to_csv(paper_csv, index=False)
        styled_paper = final_table(df_paper)  
        paper_html.write_text(styled_paper.to_html(), encoding="utf-8")
        print(f"Saved final paper CSV -> {paper_csv} and HTML -> {paper_html}")

        df_current = main_summary("current")
        df_current.to_csv(current_csv, index=False)
        styled_current = final_table(df_current)
        current_html.write_text(styled_current.to_html(), encoding="utf-8")
        print(f"Saved final current CSV -> {current_csv} and HTML -> {current_html}")

    return {
        "actions": [calc],
        "file_dep": [paper_clean, current_clean],
        "targets": [paper_csv, current_csv, paper_html, current_html],
        "clean": True,
    }

NOTEBOOKS = ["project_walkthrough"] 

def task_run_notebooks():
    """
    Execute/convert Jupyter notebooks from ./src/ to HTML in _output
    """
    for nb in NOTEBOOKS:
        nb_path = Path("./src") / f"{nb}.ipynb"
        out_html = OUTPUT_DIR / f"{nb}.html"
        yield {
            "name": nb,
            "actions": [
                f"jupyter nbconvert --execute --to html --output-dir={OUTPUT_DIR} {nb_path}"
            ],
            "file_dep": [nb_path],
            "targets": [out_html],
            "clean": True,
        }
