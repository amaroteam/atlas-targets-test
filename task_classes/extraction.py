import time

from citadel.logging import logging_setup
from citadel.database_operations import task_execution, create_replace_table, truncate_table, create_stream, insert_table

log = logging_setup()


class Extraction:
    def __init__(self, url, stage, project_name):
        self.url = url
        self.project_name = project_name
        self.schema_raw = f"{stage}_raw.ecs_ecommerce"
        self.schema_processed = f"{stage}_raw_processed.ecs_ecommerce"
        self.table_streamed = f"{self.project_name}_streamed"
        self.table_processed = self.project_name
        self.table_normalized_temp = f"{self.project_name}_normalized_temp"
        self.task_trigger1 = f"{self.project_name}_task1"
        self.task_trigger2 = f"{self.project_name}_task2"

    def main(self, event):
        start_time = time.strftime("%X", time.gmtime(time.time()))
        log.info("Start time Extraction: " + start_time)

        # Pause Snowflake trigger task
        task_execution(
            schema=self.schema_raw,
            task=self.task_trigger1,
            status= 'SUSPEND',
            url=self.url
        )
        log.info("Suspending task1 execution until end of backfill...")

        task_execution(
            schema=self.schema_raw,
            task=self.task_trigger2,
            status= 'SUSPEND',
            url=self.url
        )
        log.info("Suspending task2 execution until end of backfill...") 
        """
        #Create streamed processed table
        create_replace_table(
            schema=self.schema_processed,
            table=self.table_normalized_temp,
            query=get_normalized_query(
                schema=self.schema_processed,
                table=self.table_streamed,
            ),
            url=self.url,
        )
        log.info("Streamed_processed table created")  

        # Insert normalized table
        insert_table(
            target_schema=self.schema_processed,
            target_table=self.table_processed,
            source_schema=self.schema_processed,
            source_table=self.table_normalized_temp,
            url=self.url
        )
        log.info("Insert normalized table on processed table ...")  
    
        end_time = time.strftime("%X", time.gmtime(time.time()))
        log.info("Finish time Extraction: " + end_time)
        """
        return event
        
class ExtractionBackfill:
    def __init__(self, url, stage, project_name):
        self.url = url
        self.project_name = project_name
        self.schema_raw = f"{stage}_raw.ecs_ecommerce"
        self.schema_processed = f"{stage}_raw_processed.ecs_ecommerce"
        self.table_raw = self.project_name
        self.task_trigger1 = f"{self.project_name}_task1"
        self.task_trigger2 = f"{self.project_name}_task2"
        self.table_backfill = f"{self.project_name}_backfill"
        self.table_streamed = f"{self.project_name}_streamed"

    def main(self, event):
        start_time = time.strftime("%X", time.gmtime(time.time()))
        log.info("Start time Extraction Backfill: " + start_time)
        
        # Pause Snowflake trigger task
        task_execution(
            schema=self.schema_raw,
            task=self.task_trigger1,
            status= 'SUSPEND',
            url=self.url
        )
        log.info("Suspending task1 execution until end of backfill...")

        task_execution(
            schema=self.schema_raw,
            task=self.task_trigger2,
            status= 'SUSPEND',
            url=self.url
        )
        log.info("Suspending task2 execution until end of backfill...")

        create_stream(
            schema=self.schema_raw, 
            table=self.table_raw, 
            url=self.url
        )
        log.info("Recreate Stream...")

        truncate_table(
            schema=self.schema_processed,
            table=self.table_streamed,
            url=self.url
        )
        log.info("Truncated normalized streamed table...")  
        """
        # Create Temporary table for backfill process
        create_replace_table(
            schema=self.schema_processed,
            table=self.table_backfill,
            query=get_normalized_query(
                schema=self.schema_raw,
                table=self.table_raw,
            ),
            url=self.url,
        )
        log.info("Temporary table created...")

        end_time = time.strftime("%X", time.gmtime(time.time()))
        log.info("Finish time Extraction Backfill: " + end_time)
        """
        return event


def get_normalized_query(schema, table):
    """Get Source Table Stage Company Targets Looker."""
    return """
        SELECT
         src:ts_sent as uploaded_at
        ,p.value:Date as date
        ,p.value:"Day of week" as day_of_week
        ,p.value:Month as moth
        ,p.value:Year as year
        ,p.value:"Gross Revenue" as gross_revenue
        ,p.value:"Shipped Orders" as shipped_orders
        ,p.value:"Markup" as markup
        ,p.value:"First-time Buyers" as first_time_buyers
        ,p.value:"Returning Buyers" as returning_buyers
        ,p.value:"DR Media Budget" as dr_media_budget
        ,p.value:AOV as aov
        ,p.value:OmniCOS as omnicos
        ,p.value:"Online CAC" as online_cac
    
        FROM {schema}.{table}
            , LATERAL FLATTEN(INPUT => src:payload) p
    """.format(
        schema=schema,
        table=table
    )

