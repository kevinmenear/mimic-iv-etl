# mimic-iv-etl
A streamlined, flexible ETL pipeline tool designed for efficient processing and analysis of MIMIC-IV healthcare data.

## Setup
Navigate to the `mimic-iv-etl` directory.

### Environment
Set up and activate the Conda environment for this project:
   ```
   conda env create -f environment.yml
   conda activate mimic-iv-etl
   ```

### Package Installation

Install the `mimicfouretl` Python package:
   ```
   pip install .
   ```

### Set Up Google Authorization to Access MIMIC-IV Dataset

   ```
   pip install google-auth
   pip install google-auth-oauthlib
   pip install google-auth-httplib2
   pip install google-cloud-bigquery
   ```
