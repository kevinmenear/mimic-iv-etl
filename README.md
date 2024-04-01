# MIMIC-IV ETL and ML Utilities Package

This package offers a comprehensive toolkit for data processing and machine learning with a focus on healthcare datasets like MIMIC-IV.

## Modules

### bigquery_utils
Handles authentication and interaction with Google BigQuery, enabling querying and data retrieval. Includes Spark session creation for data processing.

### query_builder
Facilitates dynamic SQL query construction, supporting complex queries with features like selection, filtering, and joining datasets.

### data_insights
Provides tools for understanding dataset structures and content, leveraging YAML files for dataset descriptions and offering interactive exploration capabilities.

### analysis_utils
Offers analytical functions to explore item frequencies, outcomes by item, abnormal item analysis, provider activity, and event interval calculations.

### feature_engineering
Enables advanced data manipulation, including event counting, flagging, encoding, and the creation of composite scores and conditional features.

### phenotyping_engine
Supports rule-based patient phenotyping, allowing the definition and application of phenotyping rules to classify patients into groups.

### ml_utils
Comprehensive machine learning utilities, including data splitting, normalization, model training (regression, classification), and evaluation with visualization tools like confusion matrices.

## Future Directions
Focus on expanding ML utilities, enhancing feature engineering, and integrating deep learning support. Emphasize user-friendly documentation, interactive data exploration, and robust data handling.


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


