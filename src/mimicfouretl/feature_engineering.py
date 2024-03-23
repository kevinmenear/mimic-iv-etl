from mimicfouretl.bigquery_utils import run_query as bq_run_query


def get_item_frequency(item_id, column_name, dataset, limit=None):
    """
    Analyzes the frequency of a specific item within a dataset.

    Parameters:
    - item_id (int or str): The identifier for the item whose frequency is to be calculated.
    - column_name (str): The name of the column in the dataset to match with item_id.
    - dataset (str): The name of the dataset to be queried.

    Returns:
    - DataFrame: A DataFrame containing the frequency count of the specified item.

    Example Output:
    +-------+-----+
    | itemid|count|
    +-------+-----+
    |  51248| 1234|
    +-------+-----+
    """
    query = f"""
    SELECT {column_name}, COUNT(*) as count 
    FROM `{dataset}` 
    WHERE {column_name} = {item_id} 
    GROUP BY {column_name}"""
    if limit is not None:
        query += f" LIMIT {limit}"
    return bq_run_query(self.spark, query)


def get_temporal_trends(item_id, column_name, date_column, dataset, limit):
    """
    Examines trends over time for a specific item or event within a dataset.

    Parameters:
    - item_id (int or str): Identifier for the item or event to analyze.
    - column_name (str): Name of the column to match with item_id.
    - date_column (str): Name of the date/time column to use for the trend analysis.
    - dataset (str): Name of the dataset to be queried.

    Returns:
    - DataFrame: A DataFrame showing the trend of the item over time.

    Example Output:
    +-------+----------+-----+
    | itemid| charttime|count|
    +-------+----------+-----+
    |  51248|2020-01-01|  10 |
    |  51248|2020-01-02|  12 |
    +-------+----------+-----+
    """
    query = f"""
    SELECT {column_name}, {date_column}, COUNT(*) as count
    FROM `{dataset}`
    WHERE {column_name} = {item_id}
    GROUP BY {column_name}, {date_column}
    ORDER BY {date_column}
    """
    if limit is not None:
        query += f" LIMIT {limit}"
    return bq.run_query(query)


def get_outcomes_by_item(self, item_id, item_column, outcome_column, item_dataset, outcome_dataset):
    """
    Fetches patient outcomes related to specific items.

    Parameters:
    - item_id (int or str): Identifier for the item of interest.
    - item_column (str): Column name in the dataset corresponding to the item.
    - outcome_column (str): Column in the outcome dataset indicating the patient outcome.
    - item_dataset (str): Dataset name containing the item.
    - outcome_dataset (str): Dataset name containing patient outcome information.

    Returns:
    DataFrame: A DataFrame showing each occurrence of the specified item along with the associated patient outcomes.

    Example Output:
    +-----------+--------+---------+-------------+
    | subject_id| hadm_id| item_id | outcome     |
    +-----------+--------+---------+-------------+
    |     12345 | 54321  |  51248  | Recovered   |
    |     12345 | 54321  |  51248  | Readmitted  |
    +-----------+--------+---------+-------------+
    """
    query = f"""
    SELECT A.subject_id, A.hadm_id, A.{item_column} AS item_value, B.{outcome_column} AS outcome_value
    FROM `{item_dataset}` A
    JOIN `{outcome_dataset}` B ON A.subject_id = B.subject_id AND A.hadm_id = B.hadm_id
    WHERE A.{item_column} = '{item_id}'
    """
    return bq.run_query(self.spark, query)
