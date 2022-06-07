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

        return event


def get_normalized_query(schema, table):
    """Get Source Table Stage Order Items."""
    return """
        SELECT
            TRUE AS is_hybris_order
            , src:ts_received::varchar AS updated
            , src:createdDate::timestamp AS created_date
            , src:uid::string AS order_number
            , src:payload.orderDate::timestamp AS order_date
            , src:payload.customerData:emailAddressEncrypted::varchar AS user_id
            , src:payload.orderStatus::varchar AS order_status
            , src:payload:orderPlacedGuideShop:name::varchar AS store_name
            , src:payload:orderPlacedGuideShop:salespersonName::varchar AS salesperson
            , src:payload.orderDeliveryGuideShop.name::varchar AS shipping_pickup_store 
            , src:payload.shippingAddress:zipCode::varchar AS address_zip
            , src:payload.shippingAddress:state::varchar AS address_state
            , src:payload.shippingAddress:neighborhood::varchar AS address_neighborhood
            , src:payload.shippingAddress:country::varchar AS address_country
            , src:payload.shippingAddress:countryIsoCode::varchar AS country_iso
            , src:payload.shippingAddress:city::varchar AS address_city
            , src:payload.shippingOption::varchar AS shipping_service
            , src:payload.shippingQuoteId::varchar AS shipping_quote_id
            , src:payload:shippingETA::varchar AS shipping_estimated_delivery_date
            , src:payload.shipmentType::varchar AS shipping_method_id
            , src:payload.shippingCompany::varchar AS shipping_carrier
            , src:payload.ispuFlag::boolean AS shipping_pickup_enabled
            , src:payload.totalShipping::varchar AS shipping_cost
            , src:payload:totalAmount::varchar AS order_total_paid
            , src:payload.totalDiscount::varchar AS order_discount
            , src:payload.totalAmount::number AS order_total
            , src:payload.couponCode::varchar AS payment_coupon
            , src:payload:salesApplication::varchar AS source_application
            , src:payload:deviceType::varchar AS device_type
            , src:payload.currency::varchar AS local_currency
            , src:payload:baseValues.currency::varchar AS base_currency
            , src:payload:baseValues.shipping::varchar AS base_shipping
            , src:payload:baseValues.subtotal::varchar AS base_subtotal
            , src:payload:baseValues.total::varchar AS base_total_value
            , p.value:productCode::string AS sku
            , p.value:color::string AS product_color
            , p.value:size::string AS product_size
            , p.value:price::string AS product_price
            , p.value:productName::string AS product_name
            , p.value:qty::number AS quantity
            , p.value:packageType::string AS shipping_package_name
            , p.value:guideShopQty::string AS guideshop_qty
            , p.value:siteURL::string AS site_url
            , p.value:imageURL::string AS image_url
            , c.value:code::string AS order_consignment
        FROM {schema}.{table}
            , LATERAL FLATTEN(INPUT => src:payload.products) p
            , LATERAL FLATTEN(INPUT => src:payload.consignments) c
    """.format(
        schema=schema,
        table=table
    )