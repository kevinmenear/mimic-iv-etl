from pyspark.sql import functions as F
from pyspark.sql.window import Window

from mimicfouretl.bigquery_utils import run_query as bq_run_query

class HospOMRProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.dataset = "physionet-data.mimiciv_hosp.omr"

    def run_query(self, query):
        return bq_run_query(self.spark, query)

    def get_vital_measurements_trend(self, measurement_name):
        sql_query = f"""
        SELECT subject_id, chartdate, result_value
        FROM `{self.dataset}`
        WHERE result_name = '{measurement_name}'
        ORDER BY subject_id, chartdate
        """
        return self.run_query(sql_query)

    def detect_abnormal_measurements(self, measurement_name, threshold_min, threshold_max):
        sql_query = f"""
        SELECT subject_id, chartdate, result_value
        FROM `{self.dataset}`
        WHERE result_name = '{measurement_name}' AND (result_value < {threshold_min} OR result_value > {threshold_max})
        """
        return self.run_query(sql_query)

    def correlate_with_hospitalization(self, measurement_name):
        # This will require joining with admissions table and more complex logic
        pass

    def correlate_with_demographics(self, measurement_name):
        # This will involve joining with another dataset like patients and requires more complex logic
        pass


class HospProviderProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.dataset = "physionet-data.mimiciv_hosp.provider"

    def run_query(self, query):
        return bq_run_query(self.spark, query)

    def get_provider_activity_analysis(self, related_table, provider_column):
        sql_query = f"""
        SELECT {provider_column}, COUNT(*) AS activity_count
        FROM `{self.dataset}` JOIN `{related_table}`
        ON {self.dataset}.provider_id = {related_table}.{provider_column}
        GROUP BY {provider_column}
        """
        return self.run_query(sql_query)

    def get_provider_patient_ratio(self, related_table, provider_column, time_period='YEAR'):
        # This requires more complex logic and might need additional parameters
        pass


class HospAdmissionsProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.dataset = "physionet-data.mimiciv_hosp.admissions"

    def run_query(self, query):
        return bq_run_query(self.spark, query)

    def get_length_of_stay(self):
        sql_query = f"""
        SELECT subject_id, hadm_id, admittime, dischtime,
               TIMESTAMPDIFF(HOUR, admittime, dischtime) AS length_of_stay_hours
        FROM `{self.dataset}`
        """
        return self.run_query(sql_query)

    def get_time_to_readmission(self):
        sql_query = f"""
        WITH ranked_admissions AS (
            SELECT *, LAG(dischtime) OVER (PARTITION BY subject_id ORDER BY admittime) as previous_dischtime
            FROM `{self.dataset}`
        )
        SELECT subject_id, hadm_id, admittime, previous_dischtime,
               TIMESTAMPDIFF(DAY, previous_dischtime, admittime) as days_to_readmission
        FROM ranked_admissions
        """
        return self.run_query(sql_query)

    def get_admission_source_analysis(self):
        sql_query = f"""
        SELECT admission_location, COUNT(*) as count
        FROM `{self.dataset}`
        GROUP BY admission_location
        """
        return self.run_query(sql_query)

    def get_discharge_status(self):
        sql_query = f"""
        SELECT discharge_location, COUNT(*) as count
        FROM `{self.dataset}`
        GROUP BY discharge_location
        """
        return self.run_query(sql_query)

    def get_mortality_flag_analysis(self):
        sql_query = f"""
        SELECT hospital_expire_flag, COUNT(*) as count
        FROM `{self.dataset}`
        GROUP BY hospital_expire_flag
        """
        return self.run_query(sql_query)

    def get_demographic_analysis(self):
        sql_query = f"""
        SELECT race, marital_status, insurance, language,
               COUNT(*) as count
        FROM `{self.dataset}`
        GROUP BY race, marital_status, insurance, language
        """
        return self.run_query(sql_query)


class HospDHcpcsProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.dataset = "physionet-data.mimiciv_hosp.d_hcpcs"

    def run_query(self, query):
        return bq_run_query(self.spark, query)

    def get_categorization_of_procedures(self):
        sql_query = f"""
        SELECT category, COUNT(*) AS procedure_count
        FROM `{self.dataset}`
        GROUP BY category
        """
        return self.run_query(sql_query)

    def get_code_description_analysis(self):
        sql_query = f"""
        SELECT code, long_description, short_description
        FROM `{self.dataset}`
        """
        return self.run_query(sql_query)

    def associate_with_patient_outcomes(self, outcome_table):
        # This will require a JOIN with another table and more complex analysis
        pass


class HospDIcdDiagnosesProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.dataset = "physionet-data.mimiciv_hosp.d_icd_diagnoses"

    def run_query(self, query):
        return bq_run_query(self.spark, query)

    def get_diagnosis_category_analysis(self):
        # Additional logic needed to categorize diagnoses
        pass

    def correlate_with_patient_outcomes(self, outcome_table):
        # Requires JOIN with outcome_table and complex analysis
        pass



class HospDIcdProceduresProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.dataset = "physionet-data.mimiciv_hosp.d_icd_procedures"

    def run_query(self, query):
        return bq_run_query(self.spark, query)

    def correlate_with_patient_outcomes(self, outcome_table, outcome_column):
        """
        Links procedure codes to patient outcomes to identify trends or patterns.
        This requires a JOIN with an outcome table and analysis based on the procedure codes.
        """
        # Complex SQL query with JOIN needed
        pass


class HospDLabitemsProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.dataset = "physionet-data.mimiciv_hosp.d_labitems"

    def run_query(self, query):
        return bq_run_query(self.spark, query)

    def get_lab_item_frequency_analysis(self):
        """
        Determines the frequency of different lab items being used in patient records.
        """
        sql_query = f"""
        SELECT itemid, COUNT(*) AS frequency
        FROM `{self.dataset}`
        GROUP BY itemid
        """
        return self.run_query(sql_query)

    def get_fluid_based_analysis(self):
        """
        Groups and analyzes lab items based on the type of fluid they examine.
        """
        sql_query = f"""
        SELECT fluid, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY fluid
        """
        return self.run_query(sql_query)

    def get_category_based_trends(self):
        """
        Identifies trends or commonalities within certain lab item categories.
        """
        sql_query = f"""
        SELECT category, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY category
        """
        return self.run_query(sql_query)

    def correlate_with_patient_conditions(self, conditions_table):
        """
        Explores relationships between certain lab item measurements and patient diagnoses or conditions.
        This requires a JOIN with a conditions table and more complex analysis.
        """
        # Complex SQL query with JOIN needed
        pass


class HospDiagnosesIcdProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.dataset = "physionet-data.mimiciv_hosp.diagnoses_icd"

    def run_query(self, query):
        return bq_run_query(self.spark, query)

    def get_diagnosis_frequency_analysis(self):
        """
        Evaluates the most frequently occurring diagnoses in the dataset.
        """
        sql_query = f"""
        SELECT icd_code, COUNT(*) AS frequency
        FROM `{self.dataset}`
        GROUP BY icd_code
        ORDER BY frequency DESC
        """
        return self.run_query(sql_query)

    def get_patient_diagnoses_profile(self, patient_id):
        """
        Creates comprehensive profiles for patients based on their diagnosis history.
        """
        sql_query = f"""
        SELECT subject_id, icd_code, icd_version
        FROM `{self.dataset}`
        WHERE subject_id = {patient_id}
        """
        return self.run_query(sql_query)


class HospDrgcodesProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.dataset = "physionet-data.mimiciv_hosp.drgcodes"

    def run_query(self, query):
        return bq_run_query(self.spark, query)

    def get_drg_code_frequency_analysis(self):
        """
        Analyzes the frequency of different DRG codes across hospitalizations.
        """
        sql_query = f"""
        SELECT drg_code, COUNT(*) AS frequency
        FROM `{self.dataset}`
        GROUP BY drg_code
        ORDER BY frequency DESC
        """
        return self.run_query(sql_query)

    def get_severity_and_mortality_analysis(self):
        """
        Explores the distribution and implications of DRG severity and mortality ratings.
        """
        sql_query = f"""
        SELECT drg_code, AVG(drg_severity) as avg_severity, AVG(drg_mortality) as avg_mortality
        FROM `{self.dataset}`
        GROUP BY drg_code
        """
        return self.run_query(sql_query)

    def get_drg_type_analysis(self):
        """
        Compares and analyzes hospitalizations based on different DRG ontologies.
        """
        sql_query = f"""
        SELECT drg_type, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY drg_type
        """
        return self.run_query(sql_query)

    def correlate_with_diagnoses(self, diagnoses_table):
        """
        Investigates correlations between DRG codes and specific diagnoses or treatments.
        Requires complex JOIN operations with a diagnoses table.
        """
        # Complex SQL query with JOIN needed
        pass


class HospEmarProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.dataset = "physionet-data.mimiciv_hosp.emar"

    def run_query(self, query):
        return bq_run_query(self.spark, query)

    def get_medication_administration_frequency(self):
        """
        Assesses how often different medications are administered across the dataset.
        """
        sql_query = f"""
        SELECT medication, COUNT(*) AS frequency
        FROM `{self.dataset}`
        GROUP BY medication
        ORDER BY frequency DESC
        """
        return self.run_query(sql_query)

    def get_provider_administration_analysis(self):
        """
        Examines the frequency and types of medications administered by different providers.
        """
        sql_query = f"""
        SELECT enter_provider_id, medication, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY enter_provider_id, medication
        """
        return self.run_query(sql_query)

    def get_time_based_trends_in_medication_administration(self):
        """
        Analyzes trends in medication administration over different times of the day or for different hospital stays.
        """
        sql_query = f"""
        SELECT medication, EXTRACT(HOUR FROM charttime) AS hour_of_day, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY medication, EXTRACT(HOUR FROM charttime)
        """
        return self.run_query(sql_query)


class HospEmarDetailProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.dataset = "physionet-data.mimiciv_hosp.emar_detail"

    def run_query(self, query):
        return bq_run_query(self.spark, query)

    def get_detailed_medication_administration_analysis(self):
        """
        Examines the specifics of medication dosages administered, focusing on variations in dose given versus dose due.
        """
        sql_query = f"""
        SELECT medication, dose_due, dose_given, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY medication, dose_due, dose_given
        """
        return self.run_query(sql_query)

    def get_administration_type_distribution(self):
        """
        Analyzes the distribution of different administration types across the dataset.
        """
        sql_query = f"""
        SELECT administration_type, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY administration_type
        """
        return self.run_query(sql_query)

    def get_barcode_usage_analysis(self):
        """
        Investigates the frequency and patterns of barcode usage in medication administration.
        """
        sql_query = f"""
        SELECT barcode_type, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY barcode_type
        """
        return self.run_query(sql_query)

    def get_medication_delivery_analysis(self):
        """
        Explores details of medication delivery, such as infusion rates, product descriptions, and administration sites.
        """
        sql_query = f"""
        SELECT product_description, infusion_rate, route, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY product_description, infusion_rate, route
        """
        return self.run_query(sql_query)



class HospHpcseventsProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.dataset = "physionet-data.mimiciv_hosp.hpcsevents"

    def run_query(self, query):
        return bq_run_query(self.spark, query)

    def get_hcpcs_code_frequency_analysis(self):
        """
        Evaluates the most common HCPCS codes in the dataset.
        """
        sql_query = f"""
        SELECT hcpcs_cd, COUNT(*) AS frequency
        FROM `{self.dataset}`
        GROUP BY hcpcs_cd
        ORDER BY frequency DESC
        """
        return self.run_query(sql_query)

    def get_temporal_trends_of_hcpcs_codes(self):
        """
        Examines HCPCS code usage over different time frames.
        """
        sql_query = f"""
        SELECT hcpcs_cd, EXTRACT(YEAR FROM chartdate) AS year, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY hcpcs_cd, year
        ORDER BY year, count DESC
        """
        return self.run_query(sql_query)

    def get_hospitalization_duration_correlation(self):
        """
        Correlates specific HCPCS codes with the length of hospital stays.
        """
        # This requires a join with the admissions table
        pass

    def get_hcpcs_patient_demographics_correlation(self):
        """
        Analyzes the relationship between HCPCS codes and patient demographics.
        """
        # Assuming a join with a patients table for demographic data
        # Replace 'patients_table' with the actual table name and join conditions
        sql_query = f"""
        SELECT h.hcpcs_cd, p.age, p.gender, COUNT(*) AS count
        FROM `{self.dataset}` AS h
        JOIN `patients_table` AS p ON h.subject_id = p.subject_id
        GROUP BY h.hcpcs_cd, p.age, p.gender
        """
        return self.run_query(sql_query)

    def get_co_occurrence_of_hcpcs_codes(self):
        """
        Identifies which HCPCS codes frequently appear together.
        """
        # This is a simplistic approach; advanced methods might be needed for deeper analysis
        sql_query = f"""
        SELECT a.hcpcs_cd AS code1, b.hcpcs_cd AS code2, COUNT(*) AS count
        FROM `{self.dataset}` AS a
        JOIN `{self.dataset}` AS b ON a.hadm_id = b.hadm_id AND a.hcpcs_cd < b.hcpcs_cd
        GROUP BY code1, code2
        """
        return self.run_query(sql_query)

    def get_hcpcs_codes_and_readmission_rates(self):
        """
        Investigates if certain HCPCS codes are linked to higher hospital readmission rates.
        """
        # This is a placeholder. Actual implementation depends on how readmissions are defined and tracked
        pass

class HospLabeventsProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.dataset = "physionet-data.mimiciv_hosp.labevents"

    def run_query(self, query):
        return bq_run_query(self.spark, query)

    def get_lab_result_frequency_analysis(self):
        """
        Determines the most commonly recorded lab measurements.
        """
        sql_query = f"""
        SELECT itemid, COUNT(*) AS frequency
        FROM `{self.dataset}`
        GROUP BY itemid
        ORDER BY frequency DESC
        """
        return self.run_query(sql_query)

    def get_temporal_trends_in_lab_results(self, itemid):
        """
        Examines how specific lab results change over time.
        """
        sql_query = f"""
        SELECT EXTRACT(YEAR FROM charttime) AS year, COUNT(*) AS count
        FROM `{self.dataset}`
        WHERE itemid = {itemid}
        GROUP BY year
        ORDER BY year
        """
        return self.run_query(sql_query)

    def get_abnormal_lab_result_analysis(self):
        """
        Focuses on the frequency and patterns of lab results that are flagged as abnormal.
        """
        sql_query = f"""
        SELECT flag, COUNT(*) AS count
        FROM `{self.dataset}`
        WHERE flag = 'abnormal'
        GROUP BY flag
        """
        return self.run_query(sql_query)

    def get_correlation_with_hospitalization_outcomes(self, itemid):
        """
        Analyzes how specific lab results correlate with hospitalization outcomes.
        """
        # Placeholder - Requires joining with admissions table and possibly using advanced statistical methods.
        pass

    def get_itemid_usage_analysis(self):
        """
        Assesses which types of lab measurements are common in different patient groups.
        """
        # Placeholder - Requires joining with demographic data and analysis.
        pass

    def get_provider_ordering_patterns(self):
        """
        Examines the patterns of lab tests ordered by different providers.
        """
        sql_query = f"""
        SELECT order_provider_id, itemid, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY order_provider_id, itemid
        ORDER BY count DESC
        """
        return self.run_query(sql_query)

    def get_reference_range_deviation_analysis(self):
        """
        Analyzes cases where lab results fall outside the reference range.
        """
        sql_query = f"""
        SELECT itemid, COUNT(*) AS out_of_range_count
        FROM `{self.dataset}`
        WHERE valuenum < ref_range_lower OR valuenum > ref_range_upper
        GROUP BY itemid
        """
        return self.run_query(sql_query)

    def get_priority_based_analysis(self):
        """
        Investigates the effects of lab measurement priorities on their administration.
        """
        sql_query = f"""
        SELECT priority, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY priority
        """
        return self.run_query(sql_query)


class MicrobiologyEventsProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.dataset = "physionet-data.mimiciv_hosp.microbiologyevents"

    def run_query(self, query):
        return bq_run_query(self.spark, query)

    def get_microbial_growth_analysis(self):
        """
        Identifies common organisms grown in cultures and their frequency.
        """
        sql_query = f"""
        SELECT org_name, COUNT(*) AS count
        FROM `{self.dataset}`
        WHERE org_name IS NOT NULL
        GROUP BY org_name
        ORDER BY count DESC
        """
        return self.run_query(sql_query)

    def get_antibiotic_sensitivity_patterns(self):
        """
        Analyzes patterns in antibiotic sensitivity for different organisms.
        """
        sql_query = f"""
        SELECT org_name, ab_name, interpretation, COUNT(*) AS count
        FROM `{self.dataset}`
        WHERE org_name IS NOT NULL AND ab_name IS NOT NULL
        GROUP BY org_name, ab_name, interpretation
        """
        return self.run_query(sql_query)

    def get_temporal_analysis_of_cultures(self):
        """
        Examines trends in the types of cultures and their results over time.
        """
        sql_query = f"""
        SELECT spec_type_desc, YEAR(chartdate) AS year, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY spec_type_desc, YEAR(chartdate)
        """
        return self.run_query(sql_query)

    def get_correlation_with_hospitalization(self):
        """
        Investigates how microbiology test results correlate with hospitalization outcomes.
        """
        sql_query = f"""
        SELECT a.hadm_id, m.org_name, m.interpretation, DATEDIFF(day, a.admittime, a.dischtime) AS los
        FROM `{self.dataset}` m
        JOIN `physionet-data.mimiciv_hosp.admissions` a ON m.hadm_id = a.hadm_id
        WHERE m.org_name IS NOT NULL
        """
        return self.run_query(sql_query)

    def get_specimen_type_analysis(self):
        """
        Assesses the distribution of different specimen types and associated findings.
        """
        sql_query = f"""
        SELECT spec_type_desc, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY spec_type_desc
        """
        return self.run_query(sql_query)

    def get_provider_ordered_microbiology_tests(self):
        """
        Analyzes patterns in microbiology tests ordered by different providers.
        """
        sql_query = f"""
        SELECT order_provider_id, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY order_provider_id
        """
        return self.run_query(sql_query)

    def get_analysis_of_negative_cultures(self):
        """
        Studies the frequency and context of negative culture results.
        """
        sql_query = f"""
        SELECT COUNT(*) AS negative_count
        FROM `{self.dataset}`
        WHERE org_name IS NULL
        """
        return self.run_query(sql_query)

    def get_association_with_patient_demographics(self):
        """
        Explores the relationship between microbiology test results and patient demographics.
        """
        sql_query = f"""
        SELECT m.org_name, p.gender, p.dob, COUNT(*) AS count
        FROM `{self.dataset}` m
        JOIN `physionet-data.mimiciv_hosp.patients` p ON m.subject_id = p.subject_id
        WHERE m.org_name IS NOT NULL
        GROUP BY m.org_name, p.gender, p.dob
        """
        return self.run_query(sql_query)


class HospPatientsProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.dataset = "physionet-data.mimiciv_hosp.patients"

    def run_query(self, query):
        return bq_run_query(self.spark, query)

    def gender_based_analysis(self):
        """
        Assesses patient distribution and health outcomes based on gender.
        """
        sql_query = f"""
        SELECT gender, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY gender
        """
        return self.run_query(sql_query)

    def age_analysis(self):
        """
        Examines health trends or outcomes in relation to patients' ages.
        """
        sql_query = f"""
        SELECT anchor_age, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY anchor_age
        """
        return self.run_query(sql_query)

    def year_group_analysis(self):
        """
        Analyzes patient data across different anchor year groups.
        """
        sql_query = f"""
        SELECT anchor_year_group, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY anchor_year_group
        """
        return self.run_query(sql_query)

    def mortality_analysis(self):
        """
        Investigates the date of death data for insights into patient mortality rates and trends.
        """
        sql_query = f"""
        SELECT dod, COUNT(*) AS count
        FROM `{self.dataset}`
        WHERE dod IS NOT NULL
        GROUP BY dod
        """
        return self.run_query(sql_query)

    def cross_dataset_correlation(self, other_dataset):
        """
        Explores correlations with other datasets to assess broader health trends among different demographic groups.
        """
        # Implementation depends on the other dataset chosen for correlation.
        # Example SQL query structure:
        sql_query = f"""
        SELECT p.gender, p.anchor_age, COUNT(o.some_column) AS correlation_count
        FROM `{self.dataset}` p
        JOIN `{other_dataset}` o ON p.subject_id = o.subject_id
        GROUP BY p.gender, p.anchor_age
        """
        return self.run_query(sql_query)

class HospPharmacyProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.dataset = "physionet-data.mimiciv_hosp.pharmacy"

    def run_query(self, query):
        return bq_run_query(self.spark, query)

    def medication_frequency_analysis(self):
        """
        Analyzes the frequency of different medications prescribed.
        """
        sql_query = f"""
        SELECT medication, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY medication
        """
        return self.run_query(sql_query)

    def route_of_administration_analysis(self):
        """
        Examines the distribution of different medication administration routes.
        """
        sql_query = f"""
        SELECT route, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY route
        """
        return self.run_query(sql_query)

    def provider_prescription_patterns(self):
        """
        Investigates patterns in medication prescriptions made by different providers.
        """
        sql_query = f"""
        SELECT order_provider_id, medication, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY order_provider_id, medication
        """
        return self.run_query(sql_query)

    def medication_duration_analysis(self):
        """
        Assesses the duration of medication prescriptions.
        """
        sql_query = f"""
        SELECT medication, AVG(duration) AS avg_duration
        FROM `{self.dataset}`
        GROUP BY medication
        """
        return self.run_query(sql_query)

    def infusion_type_distribution(self):
        """
        Analyzes the distribution and usage patterns of different infusion types.
        """
        sql_query = f"""
        SELECT infusion_type, COUNT(*) AS count
        FROM `{self.dataset}`
        WHERE infusion_type IS NOT NULL
        GROUP BY infusion_type
        """
        return self.run_query(sql_query)


class HospPoeProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.dataset = "physionet-data.mimiciv_hosp.poe"

    def run_query(self, query):
        return bq_run_query(self.spark, query)

    def order_type_analysis(self):
        """
        Analyzes the distribution of different order types.
        """
        sql_query = f"""
        SELECT order_type, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY order_type
        """
        return self.run_query(sql_query)

    def provider_order_patterns(self):
        """
        Investigates patterns in orders made by different providers.
        """
        sql_query = f"""
        SELECT order_provider_id, order_type, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY order_provider_id, order_type
        """
        return self.run_query(sql_query)

    def order_status_distribution(self):
        """
        Analyzes the distribution of order statuses (active vs inactive).
        """
        sql_query = f"""
        SELECT order_status, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY order_status
        """
        return self.run_query(sql_query)

    def transaction_type_analysis(self):
        """
        Examines the distribution of different transaction types in the ordering process.
        """
        sql_query = f"""
        SELECT transaction_type, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY transaction_type
        """
        return self.run_query(sql_query)

    def order_subtype_distribution(self):
        """
        Analyzes the distribution and frequency of different order subtypes.
        """
        sql_query = f"""
        SELECT order_subtype, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY order_subtype
        """
        return self.run_query(sql_query)


class HospPoeDetailProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.dataset = "physionet-data.mimiciv_hosp.poe_detail"

    def run_query(self, query):
        return bq_run_query(self.spark, query)

    def field_name_analysis(self):
        """
        Analyzes the distribution and commonality of different field names in POE orders.
        """
        sql_query = f"""
        SELECT field_name, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY field_name
        """
        return self.run_query(sql_query)

    def field_value_distribution(self):
        """
        Examines the distribution of values for specific fields in POE orders.
        """
        sql_query = f"""
        SELECT field_name, field_value, COUNT(*) AS count
        FROM `{self.dataset}`
        WHERE field_name = 'YourSpecificFieldName'  # Replace with a specific field name of interest
        GROUP BY field_name, field_value
        """
        return self.run_query(sql_query)

    def admit_discharge_analysis(self):
        """
        Investigates the relationship between admission and discharge fields in POE orders.
        """
        sql_query = f"""
        SELECT field_name, field_value, COUNT(*) AS count
        FROM `{self.dataset}`
        WHERE field_name IN ('Admit to', 'Discharge Planning')
        GROUP BY field_name, field_value
        """
        return self.run_query(sql_query)

    def consult_status_analysis(self):
        """
        Analyzes the patterns in consult statuses documented in POE orders.
        """
        sql_query = f"""
        SELECT field_value, COUNT(*) AS count
        FROM `{self.dataset}`
        WHERE field_name = 'Consult Status'
        GROUP BY field_value
        """
        return self.run_query(sql_query)


class HospPrescriptionsProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.dataset = "physionet-data.mimiciv_hosp.prescriptions"

    def run_query(self, query):
        return bq_run_query(self.spark, query)

    def get_medication_frequency(self):
        """
        Analyzes the frequency of different medications prescribed.
        """
        sql_query = f"""
        SELECT drug, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY drug
        """
        return self.run_query(sql_query)

    def get_provider_prescription_patterns(self):
        """
        Examines the prescribing patterns of different providers.
        """
        sql_query = f"""
        SELECT order_provider_id, drug, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY order_provider_id, drug
        """
        return self.run_query(sql_query)

    def get_medication_duration(self):
        """
        Analyzes the typical duration for which medications are prescribed.
        """
        sql_query = f"""
        SELECT drug, AVG(EXTRACT(EPOCH FROM (stoptime - starttime))/3600) AS avg_duration_hours
        FROM `{self.dataset}`
        WHERE stoptime IS NOT NULL AND starttime IS NOT NULL
        GROUP BY drug
        """
        return self.run_query(sql_query)

    def get_route_of_administration_analysis(self):
        """
        Investigates the distribution of different routes of medication administration.
        """
        sql_query = f"""
        SELECT route, COUNT(*) AS count
        FROM `{self.dataset}`
        GROUP BY route
        """
        return self.run_query(sql_query)





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
