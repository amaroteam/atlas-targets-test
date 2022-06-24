import time

from citadel.logging import logging_setup
from citadel.database_operations import drop_table, create_replace_table

log = logging_setup()

class Transformation:
    def __init__(self, url, stage, project_name):
        self.url = url
        self.project_name = project_name
        self.schema_processed = f"{stage}_raw_processed.ecs_ecommerce"
        self.table_normalized_temp = f"{self.project_name}_normalized_temp"
        self.table_transformed_temp = f"{self.project_name}_transformed_temp"
    
    def main(self, event):
        start_time = time.strftime("%X", time.gmtime(time.time()))
        log.info("Start time Transformation: " + start_time)

        log.info("Creating transformed temporary table...")
        
        # Get normalizated table on streamed processed table
        create_replace_table(
            schema=self.schema_processed,
            table=self.table_transformed_temp,
            query=get_transformed_query(
                schema=self.schema_processed,
                table=self.table_normalized_temp,
            ),
            url=self.url,
        )

        log.info("Table transformed created, dropping normalized temporary table...")
        drop_table(
            schema=self.schema_processed,
            table=self.table_normalized_temp,
            url=self.url,
        )

        log.info("Transformation done")
        end_time = time.strftime("%X", time.gmtime(time.time()))
        log.info("Finish time Transformation: " + end_time)

        return event

class TransformationBackfill:
    def __init__(self, url, stage, project_name):
        self.url = url
        self.project_name = project_name
        self.schema_processed = f"{stage}_raw_processed.ecs_ecommerce"
        self.schema_staging = f"{stage}_analytics.analytics_staging"
        self.table_extract_backfill = f"{self.project_name}_backfill"
        self.table_backfill = f"stg_{self.project_name}_backfill"
        
    def main(self, event):
        start_time = time.strftime("%X", time.gmtime(time.time()))
        log.info("Start time Transformation Backfill: " + start_time)

        # Create Temporary table for backfill process
        create_replace_table(
            schema=self.schema_staging,
            table=self.table_backfill,
            query=get_transformed_query(
                schema=self.schema_processed,
                table=self.table_extract_backfill,
            ),
            url=self.url,
        )
        log.info("Temporary table created...")

        event["triggerStepfunction"] = False

        end_time = time.strftime("%X", time.gmtime(time.time()))
        log.info("Finish time Transformation Backfill: " + end_time)

        return event

def get_transformed_query(schema, table):
    """Get Source Table Stage Order Items."""
    return """
            SELECT
             uploaded_at::timestamp as uploaded_at 
            ,date::date as date
            ,day_of_week
            ,moth
            ,year::integer as year
            ,gross_revenue::float as gross_revenue
            ,shipped_orders
            ,markup::float as markup
            ,first_time_buyers
            ,returning_buyers
            ,dr_media_budget
            ,aov
            ,omnicos
            ,online_cac
            FROM {schema}.{table}
    """.format(
        schema=schema,
        table=table
    )