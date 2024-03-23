from mimicfouretl.bigquery_utils import run_query as bq_run_query

class icuChartEventsProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.dataset = "physionet-data.mimiciv_icu.chartevents"

    def run_query(self, query):
        return bq_run_query(self.spark, query)

    def get_vital_signs_trends(self, vital_sign_itemid):
        sql_query = f"""
        SELECT subject_id, stay_id, itemid, ARRAY_AGG(valuenum ORDER BY charttime) AS trend
        FROM `{self.dataset}`
        WHERE itemid = {vital_sign_itemid}
        GROUP BY subject_id, stay_id, itemid
        LIMIT 1000
        """
        return self.run_query(sql_query)

    def get_abnormal_values_count(self, vital_sign_itemid, min_threshold, max_threshold):
        sql_query = f"""
        SELECT subject_id, stay_id, itemid, COUNT(*) AS abnormal_count
        FROM `{self.dataset}`
        WHERE itemid = {vital_sign_itemid} AND (valuenum < {min_threshold} OR valuenum > {max_threshold})
        GROUP BY subject_id, stay_id, itemid
        """
        return self.run_query(sql_query)

    def get_daily_averages(self, vital_sign_itemid):
        sql_query = f"""
        SELECT subject_id, stay_id, itemid, DATE(charttime) as date, AVG(valuenum) AS daily_avg
        FROM `{self.dataset}`
        WHERE itemid = {vital_sign_itemid}
        GROUP BY subject_id, stay_id, itemid, DATE(charttime)
        """
        return self.run_query(sql_query)

    def get_rate_of_change(self, vital_sign_itemid):
        sql_query = f"""
        SELECT subject_id, stay_id, itemid, LAG(valuenum, 1) OVER (PARTITION BY subject_id, stay_id, itemid ORDER BY charttime) as prev_val, valuenum
        FROM `{self.dataset}`
        WHERE itemid = {vital_sign_itemid}
        """
        return self.run_query(sql_query)

    def get_aggregated_statistics(self, vital_sign_itemid):
        sql_query = f"""
        SELECT subject_id, stay_id, itemid, AVG(valuenum) as mean, STDDEV(valuenum) as stddev, MIN(valuenum) as min, MAX(valuenum) as max
        FROM `{self.dataset}`
        WHERE itemid = {vital_sign_itemid}
        GROUP BY subject_id, stay_id, itemid
        """
        return self.run_query(sql_query)

    def get_warning_flags_analysis(self, vital_sign_itemid):
        sql_query = f"""
        SELECT subject_id, stay_id, itemid, SUM(warning) AS total_warnings
        FROM `{self.dataset}`
        WHERE itemid = {vital_sign_itemid}
        GROUP BY subject_id, stay_id, itemid
        """
        return self.run_query(sql_query)
