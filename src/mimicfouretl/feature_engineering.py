from pyspark.sql import Window

from pyspark.sql.functions import col, count, datediff, expr, lead, max, when
from pyspark.sql import DataFrame

class FeatureEngineering:
    def __init__(self, data: DataFrame, subject_column='subject_id'):
        self.data = data
        self.subject_column = subject_column

    def count_events(self, event_column, specific_value=None):
        """
        Counts occurrences of specific events for each subject.

        Parameters:
        - event_column (str): Column name representing the event to count.
        - specific_value (optional, value): If provided, count only events where event_column equals this value.
        """
        if specific_value is not None:
            # Count events with the specific value
            event_count = self.data.withColumn('event_flag', when(col(event_column) == specific_value, 1).otherwise(0)) \
                                       .groupBy(self.subject_column) \
                                       .sum('event_flag') \
                                       .withColumnRenamed('sum(event_flag)', event_column + '_count')
        else:
            # Count all unique events
            event_count = self.data.groupBy(self.subject_column, event_column) \
                                       .count() \
                                       .groupBy(self.subject_column) \
                                       .sum('count') \
                                       .withColumnRenamed('sum(count)', event_column + '_count')

        # Merge the event count into the dataset
        self.data = self.data.join(event_count, on=self.subject_column, how='left')

    
    def flag_events(self, event_column, specific_value=None):
        """
        Flags the presence or absence of specific events for each subject.

        Parameters:
        - event_column (str): Column name representing the event to flag.
        - specific_value (optional, value): If provided, flags only when event_column equals this value.
        """
        if specific_value is not None:
            # Flag events with the specific value
            event_flag = self.data.withColumn('event_flag', when(col(event_column) == specific_value, 1).otherwise(0))
        else:
            # Flag all occurrences of the event
            event_flag = self.data.withColumn('event_flag', when(col(event_column).isNotNull(), 1).otherwise(0))

        # Aggregate flags to ensure one row per subject
        event_flag_agg = event_flag.groupBy(self.subject_column) \
                                   .agg(max('event_flag').alias(event_column + '_flag'))

        # Merge the event flag into the dataset
        self.data = self.data.join(event_flag_agg, on=self.subject_column, how='left')

    
    def count_previous_events(self, partition_column, order_column, event_column, event_name=None):
        """
        Counts the previous occurrences of a specific event for each subject in the dataset.
    
        Parameters:
        - partition_column (str): The column name to partition the data (e.g., 'subject_id').
        - order_column (str): The column name to order the data (e.g., 'admittime').
        - event_column (str): The column name representing the event to count (e.g., 'diagnosis').
        - event_name (str, optional): The specific event to count. If provided, only occurrences of this event are counted.
                                       If None, all events in the event_column are counted.
    
        Returns:
        - DataFrame: A DataFrame with an additional column named 'previous_events' indicating the count of previous occurrences
                     of the specified event for each subject.
    
        The function creates a new column in the dataset that represents the count of the specified event
        that occurred before the current record for each subject. This can be particularly useful for tracking
        historical data like previous admissions or diagnoses.
        """
        # Sort dataset by the specified partition and order columns
        window_spec = Window.partitionBy(partition_column).orderBy(order_column)
        
        # Count previous occurrences of the specified event
        if event_name:
            count_condition = (count(col(event_column)).over(window_spec) - 1).alias("previous_" + event_column)
            self.data = self.data.withColumn(f"previous_{event_column}_{event_name}", count_condition).filter(col(event_column) == event_name)
        else:
            count_condition = (count(col(event_column)).over(window_spec) - 1).alias("previous_" + event_column)
            self.data = self.data.withColumn(f"previous_{event_column}", count_condition)

    
    def check_event_within_timeframe(self, partition_column, event_column, event_value=None, timeframe=30):
        """
        Checks for the occurrence of a specified event within a given timeframe.

        Parameters:
        - partition_column (str): The column name to partition the data (e.g. 'subject_id').
        - event_column (str): The column to check for the event.
        - event_value (optional, value): The specific value to check within the event_column.
        - timeframe (int): Timeframe in days to check for the event.

        Adds a column indicating if the specified event occurred within the timeframe.
        """
        # Define window specification for lead function
        windowSpec = Window.partitionBy(partition_column).orderBy(col(event_column))

        # Use lead to get the date of the next occurrence of the event for each subject
        if f'next_{event_column}_date' not in self.data.columns:
            self.data = self.data.withColumn(f'next_{event_column}_date', lead(col(event_column), 1).over(windowSpec))

        # Calculate the days to the next event
        if f'days_to_next_{event_column}' not in self.data.columns:
            self.data = self.data.withColumn(f'days_to_next_{event_column}', datediff(col(f'next_{event_column}_date'), col(event_column)))

        # Define the condition for within timeframe, considering NaN values
        condition_str = f"""CASE WHEN (days_to_next_{event_column} <= {timeframe} 
                                   AND days_to_next_{event_column} > 0 
                                   AND next_{event_column}_date IS NOT NULL) 
                                 THEN 1 ELSE 0 END"""
        
        if event_value is not None:
            condition_str = f"CASE WHEN {event_column} = '{event_value}' AND " + condition_str[9:]
    
        self.data = self.data.withColumn(f'{event_column}_within_{timeframe}_days', expr(condition_str))

    
    def encode_categorical(self, categorical_columns):
        # Implement encoding for categorical variables
        pass

    
    def calculate_statistics(self, numeric_column, statistics=['mean', 'stddev', 'percentile']):
        """
        Calculates statistical measures like mean, standard deviation, and percentile for a numeric column.

        Parameters:
        - numeric_column (str): The name of the numeric column.
        - statistics (list): List of statistics to calculate (options: 'mean', 'stddev', 'percentile').
        """
        aggregations = []
        if 'mean' in statistics:
            aggregations.append(mean(numeric_column).alias(numeric_column + '_mean'))
        if 'stddev' in statistics:
            aggregations.append(stddev(numeric_column).alias(numeric_column + '_stddev'))
        if 'percentile' in statistics:
            aggregations.append(percentile_approx(numeric_column, 0.5).alias(numeric_column + '_median'))

        # Calculate the statistics
        stats_data = self.data.groupBy(self.subject_column).agg(*aggregations)

        # Merge the statistics into the dataset
        self.data = self.data.join(stats_data, on=self.subject_column, how='left')

    
    def create_conditional_feature(self, condition_str, new_feature_name):
        """
        Creates a new feature based on a given condition.
    
        Parameters:
        - condition_str (str): The condition in SQL-like syntax to evaluate.
        - new_feature_name (str): The name for the new feature column.
    
        The method adds a new column to the DataFrame, setting its value to 1 where the condition is true, and 0 otherwise.
        """
        self.data = self.data.withColumn(new_feature_name, expr(f"CASE WHEN {condition_str} THEN 1 ELSE 0 END"))

    
    def create_composite_index_score(self, scoring_rules, adjustment_factors=None):
        """
        Calculates a composite index score based on predefined scoring rules and adjustments.

        Parameters:
        - scoring_rules (dict): A dictionary where keys are column names and values are functions or lambda expressions defining how each variable contributes to the score.
        - adjustment_factors (dict, optional): A dictionary for additional adjustments, where keys are column names and values are functions or lambda expressions for adjustments.

        Returns:
        - DataFrame: The DataFrame with an additional column for the composite index score.
        """

        # Apply scoring rules
        for column, scoring_function in scoring_rules.items():
            self.data = self.data.withColumn(f"{column}_score", scoring_function(col(column)))

        # Aggregate scores
        score_columns = [col(f"{column}_score") for column in scoring_rules.keys()]
        self.data = self.data.withColumn("composite_score", sum(score_columns))

        # Apply adjustment factors if any
        if adjustment_factors:
            for column, adjustment_function in adjustment_factors.items():
                self.data = self.data.withColumn("composite_score", adjustment_function(col("composite_score"), col(column)))

    
    def apply_clinical_prediction_rule(self, rule_config):
        """
        Applies a Clinical Prediction Rule to the dataset.

        Parameters:
        - rule_config (dict): Configuration of the clinical prediction rule. It should include
                              variable names, criteria for scoring, and the scoring values.

        Example Rule Config:
        {
            'mortality_prediction': {
                'age': {'>65': 2, '<=65': 1},
                'lab_test': {'abnormal': 3, 'normal': 0},
                # Add more variables and their criteria
            }
        }
        """
        for rule_name, criteria in rule_config.items():
            score_expressions = []
            for variable, scoring in criteria.items():
                for condition, points in scoring.items():
                    score_expressions.append(expr(f"CASE WHEN {variable} {condition} THEN {points} ELSE 0 END"))
            
            combined_score = ' + '.join(score_expressions)
            self.data = self.data.withColumn(f"{rule_name}_score", expr(combined_score))

    
    def measure_event_duration(self, start_event_column, end_event_column, subject_column='subject_id', most_recent=False, aggregate_method=None, percentile_value=None):
        """
        Measures and adds the duration between two events as a new column in the DataFrame.
    
        Parameters:
        - start_event_column (str): Column name for the start event.
        - end_event_column (str): Column name for the end event.
        - subject_column (str): Column name representing the subject ID.
        - most_recent (bool): If True, only considers the most recent start event.
        - aggregate_method (str, optional): Method to aggregate durations ('mean' or 'percentile').
        - percentile_value (float, optional): Percentile value for duration calculation.
        """
    
        # Calculate duration for each subject
        duration_expr = col(end_event_column).cast('timestamp').cast('long') - col(start_event_column).cast('timestamp').cast('long')
        durations = self.data.withColumn('event_duration', duration_expr)
    
        if most_recent:
            # Determine the most recent start event for each subject
            recent_start = durations.groupBy(subject_column).agg(max(start_event_column).alias('max_start'))
            durations = durations.join(recent_start, subject_column).where(col(start_event_column) == col('max_start'))
    
        if aggregate_method:
            if aggregate_method == 'mean':
                durations = durations.groupBy(subject_column).agg(avg('event_duration').alias('event_duration'))
            elif aggregate_method == 'percentile' and percentile_value is not None:
                percentile_expr = expr(f'percentile_approx(event_duration, {percentile_value})')
                durations = durations.groupBy(subject_column).agg(percentile_expr.alias('event_duration'))
    
        # Merge the duration feature into the dataset
        self.data = self.data.join(durations.select(subject_column, 'event_duration'), on=subject_column, how='left')


    def get_processed_data(self):
        # Returns the processed data
        return self.data