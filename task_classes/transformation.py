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
        WITH 
            normalized AS (
                SELECT *
                FROM ({schema}.{table}) oi
                QUALIFY ROW_NUMBER() OVER(PARTITION BY order_number, sku ORDER BY updated DESC) = 1
                )
            , stg_ecomm_order_items AS (
                SELECT *
                    , COALESCE(product_price / (SUM(product_price) OVER(PARTITION by order_number)),1) AS share_of_item_in_order_ratio_price
                FROM normalized
                )
        SELECT 
            share_of_item_in_order_ratio_price
            , is_hybris_order
            , order_date
            , order_number
            , order_consignment
            , user_id
            , updated
            , order_status
            , product_price AS order_subtotal
            , order_discount * share_of_item_in_order_ratio_price AS order_discount
            , order_total * share_of_item_in_order_ratio_price AS order_total
            , order_total_paid * share_of_item_in_order_ratio_price AS order_total_paid
            , payment_coupon
            , address_city
            , address_country
            , address_neighborhood
            , address_state
            , address_zip
            , shipping_carrier
            , shipping_cost * share_of_item_in_order_ratio_price AS shipping_paid_by_customer
            , shipping_estimated_delivery_date
            , shipping_method_id
            , shipping_pickup_enabled
            , shipping_pickup_store
            , shipping_quote_id AS shipping_quote_id
            , shipping_service
            , store_name
            , salesperson
            , product_color
            , product_name
            , product_size
            , sku
            , shipping_package_name
            , product_price / quantity AS product_price
            , quantity
            , ((product_price / quantity) * (order_total_paid * share_of_item_in_order_ratio_price))/nullif(product_price,0) AS product_price_with_discount
            , source_application
            , device_type
            , CASE WHEN source_application = 'WEB' and device_type = 'DESKTOP'
                THEN 'desktop web'
                WHEN source_application = 'WEB' and device_type = 'MOBILE'
                THEN 'mobile web'
                WHEN source_application = 'IOS' and device_type = 'MOBILE'
                THEN 'ios app'
                WHEN source_application = 'ANDROID' and device_type = 'MOBILE'
                THEN 'android app'
                ELSE NULL
                END AS order_source_and_device
            , country_iso
            , local_currency
            , base_currency
            , base_shipping
            , base_subtotal
            , base_total_value
            FROM stg_ecomm_order_items
    """.format(
        schema=schema,
        table=table
    )