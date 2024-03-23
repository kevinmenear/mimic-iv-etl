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

    Example Usage:
    >>> item_frequency = get_item_frequency(51248, 'itemid', 'labevents')
    >>> item_frequency.show()
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

    Example Usage:
    >>> trend_data = get_temporal_trends(51248, 'itemid', 'charttime', 'labevents')
    >>> trend_data.show()
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


def count_patient_outcomes(item_id, item_column, outcome_column, dataset, outcome_dataset, limit):
    """
    Correlates specific items (like tests or medications) with patient outcomes (such as length of stay or readmission).

    Parameters:
    - item_id (int or str): Identifier for the item to be correlated.
    - item_column (str): Name of the column in the dataset that corresponds to the item.
    - outcome_column (str): Name of the column in the outcome dataset that indicates the patient outcome.
    - dataset (str): Name of the dataset containing the item.
    - outcome_dataset (str): Name of the dataset containing patient outcome information.

    Returns:
    - DataFrame: A DataFrame showing the correlation between the specified item and patient outcomes.

    Example Usage:
    >>> correlation_data = get_patient_outcomes_correlation(51248, 'itemid', 'length_of_stay', 'labevents', 'admissions')
    >>> correlation_data.show()
    +-------+---------------+----------------+
    | itemid| length_of_stay|      count     |
    +-------+---------------+----------------+
    |  51248|              5|             75 |
    |  51248|             10|             60 |
    +-------+---------------+----------------+
    """
    query = f"""
    SELECT A.{item_column}, B.{outcome_column}, COUNT(*) as count
    FROM `{dataset}` A
    JOIN `{outcome_dataset}` B ON A.subject_id = B.subject_id
    WHERE A.{item_column} = {item_id}
    GROUP BY A.{item_column}, B.{outcome_column}
    """
    if limit is not None:
        query += f" LIMIT {limit}"
    return bq.run_query(query)

