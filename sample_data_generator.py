# Populates the data folder with sample data from Physionet so the package can be explored 
# without having obtained access Physionet
import mimicfouretl.bigquery_utils as bq
import pandas as pd


def choose_random_sample(spark, approx_sample_size):
    ''' Selects random patients from the patients dataset '''
    
    query = f'''
        SELECT subject_id
        FROM `micro-vine-412020.mimiciv_hosp.patients`
        WHERE RAND() < {approx_sample_size}/(SELECT COUNT(*) FROM `micro-vine-412020.mimiciv_hosp.patients`)
    '''
    result = bq.run_query(spark, query)
    rows = result.collect()
    patients_list = [row.asDict()['subject_id'] for row in rows]
    print(f"Sample size: {len(patients_list)}")
    return patients_list


def extract_direct(spark, sample_patients):
    '''Extracts data on the selected patients from tables that have subject_id'''
    
    tables = [
        'micro-vine-412020.mimiciv_hosp.admissions',
        'micro-vine-412020.mimiciv_hosp.diagnoses_icd',
        'micro-vine-412020.mimiciv_hosp.emar',
        'micro-vine-412020.mimiciv_hosp.emar_detail',
        'micro-vine-412020.mimiciv_hosp.hcpcsevents',
        'micro-vine-412020.mimiciv_hosp.labevents',
        'micro-vine-412020.mimiciv_hosp.microbiologyevents',
        'micro-vine-412020.mimiciv_hosp.omr',
        'micro-vine-412020.mimiciv_hosp.patients',
        'micro-vine-412020.mimiciv_hosp.pharmacy',
        'micro-vine-412020.mimiciv_hosp.poe',
        'micro-vine-412020.mimiciv_hosp.poe_detail',
        'micro-vine-412020.mimiciv_hosp.prescriptions',
        'micro-vine-412020.mimiciv_hosp.procedures_icd',
        'micro-vine-412020.mimiciv_hosp.services',
        'micro-vine-412020.mimiciv_hosp.transfers'
    ]
    patients_string = ','.join(map(str, sample_patients))
    
    for table in tables:
        query = f'''
            SELECT * FROM {table}
            WHERE subject_id IN ({patients_string})
        '''
        data = bq.run_query(spark, query)
        pandas_df = data.toPandas()
        pandas_df.to_csv(f"data/sample/{table[18:]}", index=False)
        

def extract_related(spark):
    '''Extracts data from tables which don't have subject id'''
    
    # d_icd_diagnoses
    diagnoses_icd = pd.read_csv("data/sample/mimiciv_hosp.diagnoses_icd")
    icd_codes = set(diagnoses_icd['icd_code'])
    icd_codes_string = ','.join(map(str, [f'\'{code}\'' for code in icd_codes]))
    
    query = f'''
        SELECT * FROM `micro-vine-412020.mimiciv_hosp.d_icd_diagnoses`
        WHERE icd_code IN ({icd_codes_string})
    '''
    data = bq.run_query(spark, query)
    pandas_df = data.toPandas()
    pandas_df.to_csv(f"data/sample/mimiciv_hosp.d_icd_diagnoses", index=False)
    
    # d_icd_procedures
    procedures_icd = pd.read_csv("data/sample/mimiciv_hosp.procedures_icd")
    icd_codes = set(procedures_icd['icd_code'])
    icd_codes_string = ','.join([f'\'{code}\'' for code in icd_codes])
    
    query = f'''
        SELECT * FROM `micro-vine-412020.mimiciv_hosp.d_icd_procedures`
        WHERE icd_code IN ({icd_codes_string})
    '''
    data = bq.run_query(spark, query)
    pandas_df = data.toPandas()
    pandas_df.to_csv(f"data/sample/mimiciv_hosp.d_icd_procedures", index=False)
    
    # hcpcsevents
    hcpcsevents = pd.read_csv("data/sample/mimiciv_hosp.hcpcsevents")
    hcpcs_codes = set(hcpcsevents['hcpcs_cd'])
    hcpcs_codes_string = ','.join([f'\'{code}\'' for code in hcpcs_codes])
    
    query = f'''
        SELECT * FROM `micro-vine-412020.mimiciv_hosp.d_hcpcs`
        WHERE code IN ({hcpcs_codes_string})
    '''
    data = bq.run_query(spark, query)
    pandas_df = data.toPandas()
    pandas_df.to_csv(f"data/sample/mimiciv_hosp.d_hcpcs", index=False)
    
    # labevents
    labevents = pd.read_csv("data/sample/mimiciv_hosp.labevents")
    item_ids = set(labevents['itemid'])
    item_ids_string = ','.join(map(str, item_ids))
    
    query = f'''
        SELECT * FROM `micro-vine-412020.mimiciv_hosp.d_labitems`
        WHERE itemid IN ({item_ids_string})
    '''
    data = bq.run_query(spark, query)
    pandas_df = data.toPandas()
    pandas_df.to_csv(f"data/sample/mimiciv_hosp.d_labitems", index=False)
    

if __name__ == '__main__':
    # connect to BQ
    bq.set_credentials_file('bq_credentials/client_secret.json')
    bq.set_project_id('micro-vine-412020')
    client = bq.get_client(use_service_account_auth=True)
    spark = bq.get_spark_session(use_service_account_auth=True)
    
    # extract data
    approx_sample_size = 10
    sample_patients = choose_random_sample(spark, approx_sample_size)
    print(sample_patients)
    extract_direct(spark, sample_patients)
    extract_related(spark)