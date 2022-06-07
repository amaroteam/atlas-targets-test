import time
import json
import pandas as pd

from citadel.logging import logging_setup
from citadel.database_operations import truncate_table, drop_table, rename_table, task_execution, delete_from_table, insert_table

log = logging_setup()

class Loading:
    def __init__(self, url, stage, project_name):
        self.url = url
        self.stage = stage
        self.project_name = project_name
        self.schema_raw = f"{stage}_raw.ecs_ecommerce"
        self.schema_processed = f"{stage}_raw_processed.ecs_ecommerce"
        self.schema_staging = f"{stage}_analytics.analytics_staging"
        self.table_streamed = f"{self.project_name}_streamed"
        self.table_processed = self.project_name
        self.table_staging = f"stg_{self.project_name}"
        self.table_transformed_temp = f"{self.project_name}_transformed_temp"
        self.task_trigger1 = f"{self.project_name}_task1"
        self.task_trigger2 = f"{self.project_name}_task2"
    
    def main(self, event):
        start_time = time.strftime("%X", time.gmtime(time.time()))
        log.info("Start time Loading: " + start_time)
        
        # Insert normalizated table on processed from temporary transformed table
        log.info("Removing data to update...")
        delete_from_table(
            schema=self.schema_staging,
            table=self.table_staging,
            id='order_number',
            schema_temp=self.schema_processed,
            table_temp=self.table_transformed_temp,
            id_list='order_number',
            url=self.url
        )

        log.info("Updating table...")
        insert_table(
            target_schema=self.schema_staging,
            target_table=self.table_staging,
            source_schema=self.schema_processed,
            source_table=self.table_transformed_temp,
            url=self.url
        )
        
        # Truncate streamed table 
        log.info("Uploading done, truncating streamed table...")
        truncate_table(
            schema=self.schema_processed,
            table=self.table_streamed,
            url=self.url
        )

        log.info("Dropping transformed table...")
        drop_table(
            schema=self.schema_processed,
            table=self.table_transformed_temp,
            url=self.url,
        )
        if(self.stage == 'prod'):
            # Start Snowflake trigger task
            task_execution(
                schema=self.schema_raw,
                task=self.task_trigger2,
                status= 'RESUME',
                url=self.url
            )
            log.info("Started Snowflake trigger task2...")

            # Start Snowflake trigger task
            task_execution(
                schema=self.schema_raw,
                task=self.task_trigger1,
                status= 'RESUME',
                url=self.url
            )
            log.info("Started Snowflake trigger task1...")

        event["triggerStepfunction"] = False

        log.info("Loading done")
        end_time = time.strftime("%X", time.gmtime(time.time()))
        log.info("Finish time Loading: " + end_time)

        return event

class LoadingBackfill:
    def __init__(self, url, stage, project_name):
        self.url = url
        self.stage = stage
        self.project_name = project_name
        self.schema_raw = f"{stage}_raw.ecs_ecommerce"
        self.schema_staging = f"{stage}_analytics.analytics_staging"
        self.table_backfill = f"stg_{self.project_name}_backfill"
        self.table_staging = f"stg_{self.project_name}"
        self.task_trigger1 = f"{self.project_name}_task1"
        self.task_trigger2 = f"{self.project_name}_task2"
    
    def main(self, event):
        start_time = time.strftime("%X", time.gmtime(time.time()))
        log.info("Start time Loading: " + start_time)

        # Drop old stg table
        drop_table(
            schema=self.schema_staging,
            table=self.table_staging,
            url=self.url,
        )
        log.info("stg table dropped...")

        # Change backfill table to became new stg table
        rename_table(
            schema=self.schema_staging,
            old_name=self.table_backfill,
            new_name=self.table_staging,
            url=self.url,
        )
        log.info("Renamed temp table to stg table...")

        if(self.stage == 'prod'):
            # Start Snowflake trigger task
            task_execution(
                schema=self.schema_raw,
                task=self.task_trigger2,
                status= 'RESUME',
                url=self.url
            )
            log.info("Started Snowflake trigger task2...")

        if(self.stage == 'prod'):
            # Start Snowflake trigger task
            task_execution(
                schema=self.schema_raw,
                task=self.task_trigger1,
                status= 'RESUME',
                url=self.url
            )
            log.info("Started Snowflake trigger task1...")

        event["triggerStepfunction"] = False

        end_time = time.strftime("%X", time.gmtime(time.time()))
        log.info("Finish time Loading Backfill: " + end_time)

        return event