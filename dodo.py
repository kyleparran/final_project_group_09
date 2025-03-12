"""
Run or update the project. This file uses the `doit` Python package.
Automates everything, including creating a minimal Sphinx project
that references your Jupyter notebooks via MyST-NB, producing LaTeX + PDF.
"""

import sys
sys.path.insert(1, "./src/")
from pathlib import Path
from colorama import Fore, Style, init
from doit.reporter import ConsoleReporter
from os import environ
import pandas as pd
import os
import shutil

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

NOTEBOOKS = ["Project_Walkthrough", "Project_Analysis"]

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

def task_pull_clean_futures_data():
    """
    Pull clean futures data for 'paper' and 'current' periods from WRDS, save CSVs in _data.
    """
    paper_raw = DATA_DIR / "clean_futures_paper.csv"
    current_raw = DATA_DIR / "clean_futures_current.csv"

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


def task_calc_futures_data():
    """
    Calculate final stats (paper/current) using main_summary & final_table.
    CSV files in _data, HTML outputs in _output.
    """
    paper_clean = DATA_DIR / "clean_futures_paper.csv"
    current_clean = DATA_DIR / "clean_futures_current.csv"
    paper_csv = DATA_DIR / "final_paper.csv"
    current_csv = DATA_DIR / "final_current.csv"
    paper_html = OUTPUT_DIR / "final_paper.html"
    current_html = OUTPUT_DIR / "final_current.html"

    def calc():
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

def task_run_notebooks():
    """
    Convert notebooks to HTML & PDF (webpdf). 
    No latex from nbconvert, so no pandoc needed here.
    """
    for nb in NOTEBOOKS:
        nb_path = Path("./notebooks") / f"{nb}.ipynb"

        # 1) HTML
        out_html = OUTPUT_DIR / f"{nb}.html"
        yield {
            "name": f"{nb}_html",
            "actions": [
                f"jupyter nbconvert --execute --to html "
                f"--output-dir={OUTPUT_DIR} {nb_path}"
            ],
            "file_dep": [nb_path],
            "targets": [out_html],
            "clean": True,
        }

        # 2) PDF via webpdf
        out_pdf = OUTPUT_DIR / f"{nb}.pdf"
        yield {
            "name": f"{nb}_pdf",
            "actions": [
                f"jupyter nbconvert --execute --to webpdf --allow-chromium-download "
                f"--output-dir={OUTPUT_DIR} {nb_path}"
            ],
            "file_dep": [nb_path],
            "targets": [out_pdf],
            "clean": True,
        }


def task_create_figures():
    """
    Create and save LaTeX tables and PNG figures for the final report (if they don't exist).
    """

    # Define expected output files (both .tex and .png)
    expected_outputs = [
        OUTPUT_DIR / "sector_settlement_summary.tex",
        OUTPUT_DIR / "paper_table1_replication_paper.tex",
        OUTPUT_DIR / "paper_table1_replication_current.tex",
        OUTPUT_DIR / "all_commodities_settlement.png",
        OUTPUT_DIR / "commodity_correlation_heatmap.png",
        OUTPUT_DIR / "commodity_coverage_heatmap.png"
    ]

    # Run the script to generate missing figures
    action = ["python src/create_figures.py"]

    return {
        "actions": action,
        "targets": expected_outputs,
        "uptodate": [True],  # Mark task as complete if outputs exist
        "clean": True,
    }


def task_init_sphinx():
    """
    Creates a minimal Sphinx + MyST-NB project in docs/ automatically,
    referencing the notebooks in ./src. No user interaction needed.
    """
    docs_dir = Path("docs")
    conf_py = docs_dir / "conf.py"
    index_rst = docs_dir / "index.rst"

    def init_sphinx():
        # 1) create docs/ if missing
        docs_dir.mkdir(parents=True, exist_ok=True)

        # 2) minimal conf.py with myst-nb
        conf_content = r'''
import os
import sys
sys.path.insert(0, os.path.abspath(".."))

extensions = [
    "myst_nb",
]

myst_enable_extensions = [
    "colon_fence",
]

# The master toctree document.
master_doc = "index"

project = "ProjectDocs"
author = "Your Name"
release = "0.1"

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "alabaster"
'''
        if not conf_py.exists():
            conf_py.write_text(conf_content.strip(), encoding="utf-8")

        # 3) minimal index.rst referencing the notebooks
        # We assume each notebook is in ../src relative to docs/
        # MyST-NB can parse .ipynb directly if we list them in the toctree.
        toctree_lines = []
        for nb in NOTEBOOKS:
            toctree_lines.append(f"   ../notebooks/{nb}.ipynb")
        
        index_content = f'''
Welcome to ProjectDocs
======================

.. toctree::
   :maxdepth: 2

''' + "\n".join(toctree_lines)
        
        if not index_rst.exists():
            index_rst.write_text(index_content.strip() + "\n", encoding="utf-8")

    return {
        "actions": [init_sphinx],
        "targets": [conf_py, index_rst],
        "uptodate": [True],  # Mark task as complete if targets exist
        "clean": True,
    }

def task_sphinx_latexpdf():
    """
    Build LaTeX & PDF from notebooks using Sphinx + MyST-NB,
    fully automated, no pandoc needed. 
    Creates .tex and .pdf in _output/sphinx_latex/.
    """
    docs_dir = Path("docs")
    build_dir = OUTPUT_DIR / "sphinx_latex"
    latex_pdf = build_dir / "ProjectDocs.pdf"  # final PDF name

    def build_latex():
        # 1) Sphinx build LaTeX
        cmd_build = f"sphinx-build -b latex {docs_dir} {build_dir}"
        print(cmd_build)
        ret = os.system(cmd_build)
        if ret != 0:
            raise RuntimeError("Sphinx LaTeX build failed.")

        # 2) find the main .tex file
        # By default, Sphinx might name it ProjectDocs.tex or docs.tex
        main_tex = None
        for f in build_dir.glob("*.tex"):
            main_tex = f
            break
        if not main_tex:
            raise FileNotFoundError("No .tex file found in build directory.")

        # 3) compile .tex -> .pdf using system pdflatex (TeX Live)
        # do it twice for references
        for _ in range(2):
            cmd_pdf = f"pdflatex -interaction=nonstopmode -output-directory={build_dir} {main_tex.name}"
            print(cmd_pdf)
            ret2 = os.system(cmd_pdf)
            if ret2 != 0:
                raise RuntimeError("pdflatex failed. Check logs.")

    return {
        "actions": [build_latex],
        "file_dep": ["docs/conf.py", "docs/index.rst"],  # from task_init_sphinx
        "targets": [latex_pdf],
        "clean": True,
    }


def task_final_report_latex_to_pdf():
    """
    Compile the final report LaTeX file to PDF using latexmk.
    """
    report_tex = Path("./reports/Final_Report.tex")
    report_pdf = OUTPUT_DIR / "Final_Report.pdf"

    return {
        "actions": [
            f"latexmk -xelatex -halt-on-error -cd -output-directory={OUTPUT_DIR} {report_tex}"
        ],
        "file_dep": [str(report_tex)],
        "targets": [str(report_pdf)],
        "clean": True,
    }