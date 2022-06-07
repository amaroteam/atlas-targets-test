import time
import json
import pandas as pd

from citadel.logging import logging_setup
from citadel.database_operations import read_sql

log = logging_setup()

class ExtractValidation:
    def __init__(self, url, stage, project_name):
        self.url = url
        self.project_name = project_name
        self.schema_raw_proccesed = f"{stage}_raw_processed.validation"
        self.table_validation_raw = "validation_raw"
        self.schema_processed = f"{stage}_raw_processed.ecs_ecommerce"
        self.table_normalized_temp = f"{self.project_name}_normalized_temp"
    
    def main(self, event):
        """
        start_time = time.strftime("%X", time.gmtime(time.time()))
        log.info("Start time Extract Validation: " + start_time)

        log.info("Creating DF for validation...")
        df_ge = ge.from_pandas(
                    select_all(
                        schema=self.schema_processed,
                        table=self.table_normalized_temp,
                        url=self.url,
                    )
                )
        context = ge.data_context.DataContext()
        expectation_suite_name = 'expec_extract_validation'

        batch_kwargs = {
            'dataset': df_ge,
            'datasource': 'ge_data__dir',
        }
        
        log.info("Creating GE expectations...")
        batch = context.get_batch(batch_kwargs, expectation_suite_name=expectation_suite_name)

        log.info("Running GE validations...")
        result_ge = batch.validate()
        data = {}
        data['src'] = json.dumps(result_ge.to_json_dict())

        log.info("Uploading GE results...")
        upload_df(
            pd.DataFrame(data, index=[0]),
            schema=self.schema_raw_proccesed,
            table=self.table_validation_raw,
            url=self.url,
        )

        if result_ge["success"] == False:
            log.info('Extraction Validation with error')
        else:
            log.info("Extract Validation Passed: " + str(result_ge["success"]))

        end_time = time.strftime("%X", time.gmtime(time.time()))
        log.info("Finish time Extract Validation: " + end_time)
        """
        return event

class ExtractValidationBackfill:
    def __init__(self, url, stage, project_name):
        self.url = url
        self.project_name = project_name
        self.schema_processed = f"{stage}_raw_processed.ecs_ecommerce"
        self.table_processed = self.project_name
        self.table_backfill = f"{self.project_name}_backfill"
    
    def main(self, event):
        start_time = time.strftime("%X", time.gmtime(time.time()))
        log.info("Start time Extract Validation: " + start_time)
        '''
        # Create df for validantion
        df = read_sql(
            query= extract_validation_backfill_query(
                schema=self.schema_processed,
                table_origin=self.table_processed,
                table_backfill=self.table_backfill,
            ),
            url=self.url
        )
        log.info("Creating DF for validation...")

        if df['lines'][0]:
            log.info("Extraction Validation Backfill Passed" )
        else:
            raise Exception('Extraction Validation Backfill with error')
        '''
        end_time = time.strftime("%X", time.gmtime(time.time()))
        log.info("Finish time Extract Validation Backfill: " + end_time)

        return event

def extract_validation_backfill_query(schema, table_origin, table_backfill):
    query = """
        WITH max_date AS (
                SELECT 
                    TO_DATE(max(TO_TIMESTAMP(ts_received)) - interval '1 day') AS last_event_date
                FROM {schema}.{table_origin}
            ),
        old AS (
            SELECT 
                COUNT(DISTINCT CONCAT(id, ts_received)) AS lines
            FROM {schema}.{table_origin}
            WHERE (SELECT last_event_date FROM max_date) > TO_DATE(ts_received)
        ), new AS (
            SELECT 
                COUNT(DISTINCT CONCAT(id, ts_received)) AS lines
            FROM {schema}.{table_backfill}
            WHERE (SELECT last_event_date FROM max_date) > TO_DATE(ts_received) 
        )
        SELECT
            CASE WHEN new.lines = old.lines THEN TRUE ELSE FALSE END as lines
        FROM old
        JOIN new
            """.format(
                    schema=schema,
                    table_origin=table_origin,
                    table_backfill=table_backfill,
                )
    return query