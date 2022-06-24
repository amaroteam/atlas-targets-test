import time
import json
import sys

from citadel.logging import logging_setup
from citadel.database_operations import execute, truncate_table, create_replace_table, clone_table, create_stream, create_snowpipe
from citadel.standard import get_standard_configs, get_aws_credentials, get_ssm_credentials, get_services
from extraction import get_normalized_query
from transformation import get_transformed_query
from citadel.s3_functions import S3

log = logging_setup()

class Snow_tasks:
    def __init__(self, stage, env):
        self.url = get_standard_configs(stage, env)["sfl_transformer_url"]
        self.url_loader = get_standard_configs(stage, env)["sfl_loader_url"]
        self.stage = stage
        self.env = env
        self.project_name = "company_targets_looker"
        self.schema_raw = f"{stage}_raw.ecs_ecommerce"
        self.origin_schema = "prod_raw.ecs_ecommerce"
        self.schema_processed = f"{stage}_raw_processed.ecs_ecommerce"
        self.schema_staging = f"{stage}_analytics.analytics_staging"
        self.schema_input = f"{stage}_raw_processed.etl_input"
        self.table_raw = self.project_name
        self.table_streamed = f"{self.project_name}_streamed"
        self.table_processed = self.project_name
        self.table_staging = f"stg_{self.project_name}"
        self.table_input = "etl_input"
        self.etl_name = f"atlas-{self.project_name}".replace("_","-")
        self.procedure = f"procedure_atlas_{self.project_name}"
        self.stage_aws = "prd" if self.stage == 'prod' else 'dev'
        self.s3_bucket_url = f's3://{self.stage_aws}-amaro-data-lake-bucket/google-sheets/company_targets_looker/' # here
        self.s3_bucket_name = f'{self.stage_aws}-amaro-data-lake-bucket' # here
        self.prefix = 'google-sheets/company_targets_looker/' # here
        self.access_key, self.secret_key = get_aws_credentials(stage, env)
        self.s3_file_format = 'json'
        ssm, sts = get_services()
        self.sqs = get_ssm_credentials(ssm, '/chapter/dataengineering/atlas/snowpipe_sqs')['Parameter']['Value'] # here
        self.snowpipe = f'{self.schema_raw}.{self.table_raw}.pipe_{self.project_name}' # here
        
        
    def main(self):
        start_time = time.strftime("%X", time.gmtime(time.time()))
        log.info("Start Snowflake stream and tasks creations: " + start_time)

        # Creating dev raw table
        if(self.stage == 'dev'):
            clone_table(
                schema=self.schema_raw,
                new_table=self.project_name,
                origin_schema=self.origin_schema,
                origin_table=self.project_name,
                url=self.url # transformer url
            )
            log.info("Created Dev raw table...")

        # Create normalized table
        create_replace_table(
            schema=self.schema_processed,
            table=self.table_processed,
            query=get_normalized_query(
                schema=self.schema_raw,
                table=self.table_raw
            ),
            url=self.url
        )
        log.info("Created normalized table...")

        # Create normalized streamed table
        create_replace_table(
            schema=self.schema_processed,
            table=self.table_streamed,
            query="""
                    SELECT * FROM {schema}.{table} LIMIT 10;
                    """.format(
                        schema=self.schema_raw,
                        table=self.table_raw
                    ),
            url=self.url
        )
        log.info("Created normalized streamed table...")

        # Truncate normalized streamed table
        truncate_table(
            schema=self.schema_processed,
            table=self.table_streamed,
            url=self.url
        )
        log.info("Truncated normalized streamed table...")

        # Create transformed table
        create_replace_table(
            schema=self.schema_staging,
            table=self.table_staging,
            query=get_transformed_query(
                schema=self.schema_processed,
                table=self.table_processed
            ),
            url=self.url
        )
        log.info("Created transformed table...")

        # Create stream on raw table
        create_stream(
            schema=self.schema_raw, 
            table=self.table_raw, 
            url=self.url
        )
        log.info("Created Stream...")

        # Create task to move new data to streamed table
        execute(
            query=create_task1(self.schema_raw,
                self.table_raw,
                self.schema_processed,
                self.table_streamed,
            ),
            url=self.url,
        )
        log.info("Created Task1...")

        # Create procedure and task to trigger etl lambda
        execute(
            query=create_procedure(self.schema_processed,
                self.table_streamed,
                self.schema_input,
                self.table_input,
                self.schema_raw,
                self.procedure,
                self.etl_name,
                self.stage,
            ),
            url=self.url,
        )
        log.info("Created Procedure...")

        execute(
            query=create_task2(self.schema_raw,
                self.table_raw,
                self.table_processed,
                self.procedure,
            ),
            url=self.url,
        )
        log.info("Created Task2...")

        # Update table that contains ETL Input
        input = {
            "stage": self.stage,
            "env": "aws",
            "task": "extract",
            "mode": "nobackfill",
            "lambdaName": f"{self.stage}-amaro-{self.etl_name}-lambda", # here
            "hasExtraction": True,
            "kwarg": "null",
            "data": {}
        }

        execute(
            query=update_tbl_etl_input(self.schema_input,
                self.table_input,
                self.etl_name,
                json.dumps(input)),
            url=self.url,
        )
        log.info("Updated ETL_INPUT table...")


        execute(
            query=active_task2(self.schema_raw, self.table_processed),
            url=self.url,
        )
        log.info("Activated Task2...")

        # Activate tasks
        execute(
            query=active_task1(self.schema_raw, self.table_raw),
            url=self.url,
        )
        log.info("Activated Task1...")

        # Creating snowpipe 
        # here
        
        create_snowpipe(
            schema=self.schema_raw,
             table=self.table_raw,
             s3_bucket=self.s3_bucket_url,
             access_key=self.access_key,
             secret_key=self.secret_key,
             s3_file_format=self.s3_file_format,
             url=self.url
         )
        log.info("Created Snowpipe...")
        
        
        # Creating bucket notification
        """
        s3 = S3(self.env, self.stage, self.s3_bucket_url)
        
        s3.create_bucket_notification(
             bucket_name=self.s3_bucket_name,
             pipe=self.snowpipe,
             snowpipe_sqs=self.sqs,
             prefix=self.prefix
         )
        log.info('Bucket notification created...')
        """
def create_task1(schema_raw, table_raw, schema_processed, table_streamed):
    query = """
        CREATE OR REPLACE TASK {schema_raw}.{table_raw}_task1
        WAREHOUSE = TRANSFORMATION
        SCHEDULE = '5 MINUTE'
        AS
        INSERT INTO {schema_processed}.{table_streamed}
        SELECT src
        FROM {schema_raw}.{table_raw}_stream
    """.format(
        schema_raw=schema_raw,
        table_raw=table_raw,
        schema_processed=schema_processed,
        table_streamed=table_streamed,
    )
    return query

def create_procedure(schema_processed, table_streamed, schema_input, table_input, schema_raw, procedure, etl_name, stage):
    query = """
        CREATE OR REPLACE PROCEDURE {schema_raw}.{procedure}()
            RETURNS VARCHAR
            LANGUAGE javascript
            AS
            $$
            var rs = snowflake.execute( {{ sqlText: 
                `SELECT {schema_input}.{stage}_start_atlas_etl_execution(input)
                FROM {schema_input}.{table_input}
                WHERE ETL_NAME = '{etl_name}'
                AND (SELECT count(1)
                    FROM {schema_processed}.{table_streamed}) > 500
                LIMIT 1;`
                    }} );
            return 'Done.';
            $$;
    """.format(
        schema_processed=schema_processed,
        table_streamed=table_streamed,
        schema_input=schema_input,
        table_input=table_input,
        schema_raw=schema_raw,
        procedure=procedure,
        etl_name=etl_name,
        stage=stage
    )
    return query


def create_task2(schema_raw, table_raw, table_processed, procedure):
    query = """
        CREATE OR REPLACE TASK {schema_raw}.{table_processed}_task2
        WAREHOUSE = TRANSFORMATION
        AFTER {schema_raw}.{table_raw}_task1
        AS
        CALL {schema_raw}.{procedure}()
    """.format(
        schema_raw=schema_raw,
        table_raw=table_raw,
        table_processed=table_processed,
        procedure=procedure,
    )
    return query

def update_tbl_etl_input(schema_processed, table_input, etl_name, input):
        query = """
            MERGE INTO {schema_processed}.{table_input} using (SELECT '{etl_name}' as etl_name, '{input}' as input) AS aux
                ON {table_input}.etl_name = aux.etl_name
                WHEN MATCHED THEN 
                    UPDATE SET {table_input}.input = aux.input
                WHEN NOT MATCHED THEN 
                    INSERT (etl_name, input) VALUES (aux.etl_name, aux.input);
        """.format(
            schema_processed=schema_processed,
            table_input=table_input,
            etl_name=etl_name,
            input=input
        )
        return query

def active_task1(schema_raw, table_raw):
    query = """
        ALTER TASK {schema_raw}.{table_raw}_task1 RESUME
    """.format(
        schema_raw=schema_raw,
        table_raw=table_raw,
    )
    return query

def active_task2(schema_raw, table_processed):
    query = """
        ALTER TASK {schema_raw}.{table_processed}_task2 RESUME
    """.format(
        schema_raw=schema_raw,
        table_processed=table_processed,
    )
    return query

def main(argv):
    stage, env = argv[1], argv[2]
    Snow_tasks(stage, env).main()


if __name__ == "__main__":
    main(sys.argv)