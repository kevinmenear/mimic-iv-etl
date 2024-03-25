from mimicfouretl.bigquery_utils as bq

def get_item_frequency(self, column_name, dataset, item_id=None, limit=None):
    """
    Analyzes the frequency of items within a dataset. Can target a specific item or all items.

    Parameters:
    - column_name (str): The name of the column in the dataset to analyze.
    - dataset (str): The name of the dataset to be queried.
    - item_id (int or str, optional): The identifier for a specific item (default is None, which analyzes all items).
    - limit (int, optional): The maximum number of results to return (default is None, no limit).

    Returns:
    - DataFrame: A DataFrame containing the frequency count of the specified item(s).

    Example Output:
    For a specific item:
    +-------+-----+
    | itemid|count|
    +-------+-----+
    |  51248| 1234|
    +-------+-----+

    For all items:
    +-------+-----+
    | itemid|count|
    +-------+-----+
    |  51248| 1234|
    |  51249| 5678|
    |   ... |  ...|
    +-------+-----+
    """
    query = f"SELECT {column_name}, COUNT(*) as count FROM `{dataset}`"
    if item_id is not None:
        query += f" WHERE {column_name} = '{item_id}'"
    query += f" GROUP BY {column_name}"
    if limit is not None:
        query += f" LIMIT {limit}"
    
    return bq.run_query(self.spark, query)



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


def get_abnormal_item_analysis(self, item_id, item_column, value_column, bounds, dataset):
    """
    Analyzes instances of abnormal values for an item, based on specified bounds.

    Parameters:
    - item_id (int or str): Identifier for the item being analyzed.
    - item_column (str): Column name corresponding to the item.
    - value_column (str): Column name for the item's value.
    - bounds (dict): A dictionary with keys 'lower', 'upper', or both, specifying bounds.
    - dataset (str): Dataset name containing the item and its values.

    Returns:
    DataFrame: A DataFrame listing instances of abnormal item values based on the bounds.

    Example Output:
    +-----------+--------+---------+------------+
    | subject_id| hadm_id| item_id | value      |
    +-----------+--------+---------+------------+
    |     10001 | 50001  |  51248  | 0.5        | # Below lower bound
    |     10002 | 50002  |  51248  | 7.8        | # Above upper bound
    +-----------+--------+---------+------------+
    """
    conditions = []
    if 'lower' in bounds:
        conditions.append(f"{value_column} < {bounds['lower']}")
    if 'upper' in bounds:
        conditions.append(f"{value_column} > {bounds['upper']}")
    condition_str = " OR ".join(conditions)

    query = f"""
    SELECT subject_id, hadm_id, {item_column}, {value_column}
    FROM `{dataset}`
    WHERE {item_column} = '{item_id}' AND ({condition_str})
    """
    return bq.run_query(self.spark, query)


def get_provider_activity_analysis(self, provider_id, dataset_columns):
    """
    Analyzes the activities of a specific provider across various datasets with different activity columns.

    Parameters:
    - provider_id (int or str): Identifier for the provider of interest.
    - dataset_columns (dict): A dictionary where keys are dataset names and values are dicts with keys 'provider' and 'activity' and values being the names of the respective columns in the dataset.

    Returns:
    DataFrame: A DataFrame showing the activities performed by the provider across the specified datasets.

    Example Output:
    +-------------+-----------+-----------------------+-----+
    | provider_id | dataset   | activity              |count|
    +-------------+-----------+-----------------------+-----+
    |   P003AB    | labevents | Lab Test Ordered      | 150 |
    |   P003AB    | rxevents  | Medication Prescribed | 75  |
    +-------------+-----------+-----------------------+-----+
    """
    queries = []
    for dataset, columns in dataset_columns.items():
        provider_column = columns['provider']
        activity_column = columns['activity']
        query = f"""
        SELECT '{dataset}' AS dataset, {activity_column} AS activity, COUNT(*) AS count
        FROM `{dataset}`
        WHERE {provider_column} = '{provider_id}'
        GROUP BY {activity_column}
        """
        queries.append(query)

    full_query = " UNION ALL ".join(queries)
    return bq.run_query(self.spark, full_query)


def get_co_occurrence_analysis(self, dataset, primary_column, secondary_column, threshold=0.1):
    """
    Identifies frequent co-occurrences or combinations of events in a given dataset.

    Parameters:
    - dataset (str): Name of the dataset to analyze.
    - primary_column (str): The main column of interest for co-occurrence analysis.
    - secondary_column (str): The secondary column to be analyzed for co-occurrence with the primary column.
    - threshold (float): The minimum ratio of co-occurrence for an event to be considered significant.

    Returns:
    DataFrame: A DataFrame showing the patterns or combinations of events that co-occur frequently.

    Example Output:
    +---------------+------------------+-----------+
    | primary_event | secondary_event  | frequency |
    +---------------+------------------+-----------+
    |   Med_A       |   Med_B          |   0.15    |
    |   Diag_X      |   Diag_Y         |   0.12    |
    +---------------+------------------+-----------+
    """
    query = f"""
    WITH co_occurrence AS (
        SELECT {primary_column}, {secondary_column}, COUNT(*) AS freq
        FROM `{dataset}`
        GROUP BY {primary_column}, {secondary_column}
    )
    SELECT {primary_column} AS primary_event, {secondary_column} AS secondary_event, 
           (freq / SUM(freq) OVER (PARTITION BY {primary_column})) AS frequency
    FROM co_occurrence
    WHERE (freq / SUM(freq) OVER (PARTITION BY {primary_column})) >= {threshold}
    """
    return bq.run_query(self.spark, query)


def get_cross_dataset_co_occurrence(self, dataset1, dataset2, primary_column, secondary_column, threshold=0.1):
    """
    Identifies co-occurrence patterns between events across two different datasets.

    Parameters:
    - dataset1 (str): First dataset for analysis.
    - dataset2 (str): Second dataset for analysis.
    - primary_column (str): Column from the first dataset to analyze for co-occurrence.
    - secondary_column (str): Column from the second dataset to analyze for co-occurrence.
    - threshold (float): Minimum ratio of co-occurrence for an event to be significant.

    Returns:
    DataFrame: A DataFrame showing co-occurrence patterns between two datasets.

    Example Output:
    +---------------+------------------+-----------+
    | primary_event | secondary_event  | frequency |
    +---------------+------------------+-----------+
    |   Event_1     |   Event_A        |   0.18    |
    |   Event_2     |   Event_B        |   0.14    |
    +---------------+------------------+-----------+
    """
    query = f"""
    WITH combined_data AS (
        SELECT d1.subject_id, d1.hadm_id, d1.{primary_column}, d2.{secondary_column}
        FROM `{dataset1}` d1
        JOIN `{dataset2}` d2 ON d1.subject_id = d2.subject_id AND d1.hadm_id = d2.hadm_id
    ),
    co_occurrence AS (
        SELECT {primary_column}, {secondary_column}, COUNT(*) AS freq
        FROM combined_data
        GROUP BY {primary_column}, {secondary_column}
    )
    SELECT {primary_column} AS primary_event, {secondary_column} AS secondary_event, 
           (freq / SUM(freq) OVER (PARTITION BY {primary_column})) AS frequency
    FROM co_occurrence
    WHERE (freq / SUM(freq) OVER (PARTITION BY {primary_column})) >= {threshold}
    """
    return bq.run_query(self.spark, query)


def calculate_event_to_death_interval(self, event_date_column, event_dataset):
    """
    Calculates the time between a specified event and the patient's death.

    Parameters:
    - event_date_column (str): Column name in the event dataset that indicates the date of the event.
    - event_dataset (str): Dataset name containing the event information.

    Returns:
    DataFrame: A DataFrame showing the time between the specified event and patient death.

    Example Output:
    +-----------+--------+-------------+------------------+-----------------+
    | subject_id| hadm_id| event_date  | date_of_death    | days_to_death   |
    +-----------+--------+-------------+------------------+-----------------+
    |     12345 | 54321  | 2020-01-01  | 2020-01-15       | 14              |
    +-----------+--------+-------------+------------------+-----------------+
    """
    patients_table = "physionet-data.mimiciv_hosp.patients"
    query = f"""
    WITH death_dates AS (
        SELECT subject_id, hadm_id, dod AS date_of_death
        FROM `{patients_table}`
        WHERE dod IS NOT NULL
    ),
    events AS (
        SELECT subject_id, hadm_id, {event_date_column} AS event_date
        FROM `{event_dataset}`
    )
    SELECT e.subject_id, e.hadm_id, e.event_date, d.date_of_death,
           DATEDIFF(d.date_of_death, e.event_date) AS days_to_death
    FROM events e
    JOIN death_dates d ON e.subject_id = d.subject_id AND e.hadm_id = d.hadm_id
    """
    return bq.run_query(self.spark, query)


def calculate_event_to_event_interval(self, event_params):
    """
    Calculates the time interval between two specified events for each patient.

    Parameters:
    - event_params (dict): A dictionary with keys 'first_event' and 'second_event', each containing another dict with 'dataset', 'event_type', and 'time_column'.

    Returns:
    - DataFrame: A DataFrame showing the time intervals between the two events for each patient.

    Example Output:
    +-----------+--------+--------------+---------------+
    | subject_id| hadm_id| first_event  | second_event  |
    +-----------+--------+--------------+---------------+
    |     12345 | 54321  | 2020-01-01   | 2020-01-05    |
    |     23456 | 65432  | 2020-02-01   | 2020-02-03    |
    +-----------+--------+--------------+---------------+

    Example Usage:
    event_params = {
        'first_event': {'dataset': 'dataset1', 'event_type': 'type1', 'time_column': 'time1'},
        'second_event': {'dataset': 'dataset2', 'event_type': 'type2', 'time_column': 'time2'}
    }
    """
    fe = event_params['first_event']
    se = event_params['second_event']

    query = f"""
    WITH FirstEvent AS (
        SELECT subject_id, hadm_id, {fe['time_column']} AS first_event_time
        FROM `{fe['dataset']}`
        WHERE event_type = '{fe['event_type']}'
    ),
    SecondEvent AS (
        SELECT subject_id, hadm_id, {se['time_column']} AS second_event_time
        FROM `{se['dataset']}`
        WHERE event_type = '{se['event_type']}'
    )
    SELECT f.subject_id, f.hadm_id, f.first_event_time, s.second_event_time
    FROM FirstEvent f
    JOIN SecondEvent s ON f.subject_id = s.subject_id AND f.hadm_id = s.hadm_id
    """
    return bq.run_query(self.spark, query)


def search_dataset_by_value(self, dataset, column_name, search_value, columns='*', closeness='exact'):
    """
    Searches a dataset for rows where a specified column's value matches or is close to a given string,
    and returns specified columns or all columns by default.
    
    Parameters:
    - dataset (str): Name of the dataset to search.
    - column_name (str): Column to search in.
    - search_value (str): String value to search for.
    - columns (str or list): Columns to return. Default is '*', which returns all columns.
    - closeness (str): Matching closeness ('exact', 'contains', 'starts_with', or 'ends_with').

    Returns:
    - DataFrame: Rows from the dataset where the column matches or is close to the search value.

    Example Output:
    +-----------+--------+-------------+
    | subject_id| hadm_id| column_value |
    +-----------+--------+-------------+
    |     10001 | 50001  | search_val   |
    |     10002 | 50002  | search_val   |
    +-----------+--------+-------------+

    Example Usage:
    search_dataset_by_value('dataset_name', 'medication', 'Aspirin', ['subject_id', 'medication'], 'contains')
    """

    search_value = search_value.lower()
    if closeness == 'exact':
        condition = f"LOWER({column_name}) = '{search_value}'"
    elif closeness == 'contains':
        condition = f"LOWER({column_name}) LIKE '%{search_value}%'"
    elif closeness == 'starts_with':
        condition = f"LOWER({column_name}) LIKE '{search_value}%'"
    elif closeness == 'ends_with':
        condition = f"LOWER({column_name}) LIKE '%{search_value}'"
    else:
        raise ValueError("Closeness parameter must be 'exact', 'contains', 'starts_with', or 'ends_with'.")

    select_columns = ', '.join(columns) if isinstance(columns, list) else columns

    query = f"""
    SELECT {select_columns}
    FROM `{dataset}`
    WHERE {condition}
    """
    return bq.run_query(self.spark, query)

