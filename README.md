Project Overview
=============================================

## About this project
=============================================
### Overview
The primary goal of this project is to:
- Pull futures contract data for multiple commodities from the WRDS database
- Convert daily settlement prices into monthly observations
- Identify front-month (nearest) and twelfth-month (latest) contracts to compute the basis
- Calculate frequency of backwardation, excess returns, and Sharpe ratios
- Compare results to Table 1 in Fan Yang’s paper, try to replicate
as close as possible

### Pulling Data
Our data is pulled directly from WRDS API and cleaned then stored in the _data directory. There are two sets of two types of datasets. "cleaned" and "final_table" data for both the paper, which is the range listed within Yang's Paper(1970/01/01 - 2008/12/31), and a "current" range, which is the date range after until 2025/02/28.


## Quick Start

To quickest way to run code in this repo is to use the following steps. First, you must have the `conda`  
package manager installed (e.g., via Anaconda). However, I recommend using `mamba`, via [miniforge]
(https://github.com/conda-forge/miniforge) as it is faster and more lightweight than `conda`. Second, you 
must have TexLive (or another LaTeX distribution) installed on your computer and available in your path.
You can do this by downloading and 
installing it from here ([windows](https://tug.org/texlive/windows.html#install) 
and [mac](https://tug.org/mactex/mactex-download.html) installers).
Having done these things, open a terminal and navigate to the root directory of the project and create a 
conda environment using the following command:
```
conda create -n blank python=3.12
conda activate blank
```
and then install the dependencies with pip
```
pip install -r requirements.txt
```
Finally, you can then run 
```
doit
```
And that's it!



#### Unit Tests and Doc Tests

You can run the unit test, including doctests, with the following command:
```
pytest --doctest-modules
```
You can build the documentation with:
```
rm ./src/.pytest_cache/README.md 
jupyter-book build -W ./
```
Use `del` instead of rm on Windows

#### Setting Environment Variables

You can 
[export your environment variables](https://stackoverflow.com/questions/43267413/how-to-set-environment-variables-from-env-file) 
from your `.env` files like so, if you wish. This can be done easily in a Linux or Mac terminal with the following command:
```
set -a ## automatically export all variables
source .env
set +a
```
In Windows, this can be done with the included `set_env.bat` file,
```
set_env.bat
```

### General Directory Structure

- **_data/**: Stores cleaned data and the final table data, that has been pulled from WRDS (local CSV/Parquet files).
- **assets/**: Contains a tex file of the replication of table 1
- **notebooks/**: Contains two Jupyter notebooks(Project_Analysis & Project_Walkthrough) that both walk through
the data cleaning process, as well as a deeper analysis on the table data and respective commodities.
- **output_/**: Contains png, html, pdf, and tex files that are different formats of illustrative examples createed wtihin this notebook
- **reports/**: Stores the Final_Report.tex file with the final analysis as well as the sources of the papers we reference.
- **src/**: Houses all Python modules to fetch data, transform it, and run analyses.
- **README.md**: Project documentation.
- **dodo.py**: Conventionally recognized task-definition file used by the Python-based automation tool doit. When you run "doit" in a director. Each task within automates, pulls, cleans and outputs the data in the required format.
Called by running "doit" within the terminal.
- **env.example** gives the format of the env file the user will need to add with their credentials to correctly pull the data and run doit.

### Data and Output Storage

Data is pulled and stored in _data/, which is excluded from version control. Rerunning doit recreates it. Any manually created data is stored in data_manual/ and committed to Git to preserve changes. Generated outputs (e.g., dataframes, charts) live in _output/ and may be versioned if small enough. Paths to these folders and credentials are defined via environment variables (usually in .env) and loaded through settings.py, which all scripts access by importing config.


### Computational Definitions
- Within the replication of the table you will see several different computations:
  - Sector - The grouping of the commodities, a certain type of classification for where the comodity is generally grouped.
  - Commodity - The product we are aiming to analyze. Could be any physical asset traded on the exchange. Examples are Gold, Oil, Corn etc..
  - Symbol - The ticker for which the commodity is referenced.
  - N - Number of valid monthly observations or data points used in the analysis
  - Basis - The log price difference (front-month minus twelfth-month contract), scaled by months to maturity, expressed in percentage terms 
  - Freq. of bw. - Frequency (as a percentage) of monthly observations where the front-month contract price exceeds the deferred (twelfth-month) contract price (i.e., backwardation)
  - E[Re] - The mean annualized excess return of the commodity’s futures price over the sample period
  - σ[Re] - Standard deviation of the commodity’s annualized excess returns 
  - Sharpe ratio - Ratio of the commodity’s mean excess return to its standard deviation of returns, indicating risk-adjusted performance.

### Dependencies and Virtual Environments
- Users need to have TeXlive or another similar alternative to create the .tex documents. The rest of the requirements should be covered by the quickstart by creating the environment and installing the requirements via
```
pip install -r requirements.txt
```


#### Working with `pip` requirements

`conda` allows for a lot of flexibility, but can often be slow. `pip`, however, is fast for what it does.  You can install the requirements for this project using the `requirements.txt` file specified here. Do this with the following command:
```
pip install -r requirements.txt
```

The requirements file can be created like this:
```
pip list --format=freeze
```

#### Working with `conda` environments

The dependencies used in this environment (along with many other environments commonly used in data science) are stored in the conda environment called `blank` which is saved in the file called `environment.yml`. To create the environment from the file (as a prerequisite to loading the environment), use the following command:

```
conda env create -f environment.yml
```

Now, to load the environment, use

```
conda activate blank
```

Note that an environment file can be created with the following command:

```
conda env export > environment.yml
```

However, it's often preferable to create an environment file manually, as was done with the file in this project.

Also, these dependencies are also saved in `requirements.txt` for those that would rather use pip. Also, GitHub actions work better with pip, so it's nice to also have the dependencies listed here. This file is created with the following command:

```
pip freeze > requirements.txt
```

**Other helpful `conda` commands**

- Create conda environment from file: `conda env create -f environment.yml`
- Activate environment for this project: `conda activate blank`
- Remove conda environment: `conda remove --name blank --all`
- Create blank conda environment: `conda create --name myenv --no-default-packages`
- Create blank conda environment with different version of Python: `conda create --name myenv --no-default-packages python` Note that the addition of "python" will install the most up-to-date version of Python. Without this, it may use the system version of Python, which will likely have some packages installed already.

#### `mamba` and `conda` performance issues

Since `conda` has so many performance issues, it's recommended to use `mamba` instead. I recommend installing the `miniforge` distribution. See here: https://github.com/conda-forge/miniforge

