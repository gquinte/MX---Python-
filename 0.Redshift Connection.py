#Import the module
import sqlalchemy
import pandas as pd
import psycopg2
from operator import mul
from sqlalchemy import create_engine
import timeit

#Defining required parameters for the DB connection
user = "retail_gquinte"
pw = "VQdlynod2#"
host = "narx-redshift-db.cgfvwaou999l.us-east-1.redshift.amazonaws.com"
port = 8192
dbname = "narxredshiftdb"

#Establishing the DB connection to Redshift and naming this connection as "conn"
connenction_string = 'redshift+psycopg2://%s:%s@%s:%d/%s' % (user, pw, host, port, dbname)

engine = create_engine(connenction_string)

# Extract data with SQL
sql_str_train = ''' 
with t1 as (
select 
o.asin,
o.customer_order_item_id,
d.customer_shipment_item_id,
o.order_day,
d.ship_day,
p.promotion_type,
p.paws_promotion_id

from  PDM.fact_promotion_orders o join booker.D_UNIFIED_CUST_SHIPMENT_ITEMS d 
    on o.customer_order_item_id = d.customer_order_item_id and o.asin = d.asin
    join  PDM.dim_promotion p on p.promotion_key = o.promotion_key

    where 
    d.region_id = 1
    and d.marketplace_id = 771770
    and d.merchant_customer_id = 8833336105
    and d.gl_product_group IN (201,79,199,196,86)
    and o.order_day BETWEEN TO_DATE('2019/01/01', 'YYYY/MM/DD') AND TO_DATE('2020/01/31', 'YYYY/MM/DD')) ,
    
t2 as (
Select 
asin, 
customer_order_item_id,
customer_shipment_item_id,
order_day,
ship_day,
CASE WHEN promotion_type = 'Deal of the Day' THEN paws_promotion_id ELSE NULL END AS DOTD, 
CASE WHEN promotion_type = 'Lightning Deal' THEN paws_promotion_id  ELSE NULL END AS LD, 
CASE WHEN promotion_type = 'Best Deal' THEN paws_promotion_id ELSE NULL END AS BD, 
CASE WHEN promotion_type = 'Price Discount' THEN  paws_promotion_id ELSE NULL END AS PD, 
CASE WHEN promotion_type = 'Sales Discount' THEN  paws_promotion_id ELSE NULL END AS SD

FROM T1 ),

t3 as (
select 
asin, 
customer_order_item_id,
customer_shipment_item_id,
order_day,
ship_day,
MIN(DOTD) AS PAWS_ID_DOTD,
MIN(LD) AS PAWS_ID_LD,
MIN(BD) AS PAWS_ID_BD,
MIN(PD) AS PAWS_ID_PD,
MIN(SD) AS PAWS_ID_SD
FROM T2
GROUP BY 1,2,3,4,5)

select 
TO_CHAR(t.order_day, 'YYYY') AS YEAR,
TO_CHAR(t.order_day, 'Q') AS QUARTER,
TO_CHAR(t.order_day, 'MM') AS MONTH,
TO_CHAR(t.order_day + 1, 'IW') AS WEEK,
DMP.GL_PRODUCT_GROUP,
UPPER(DMP.merchant_brand_name) as BRAND,
t.PAWS_ID_DOTD,
t.PAWS_ID_LD,
t.PAWS_ID_BD,
t.PAWS_ID_PD,
t.PAWS_ID_SD,
 SUM(CP.ASSOCIATE_FEES_AMT) AS ASSOCIATE_FEES_AMT,
  SUM(CP.BAD_DEBT_AMT) AS BAD_DEBT_AMT,
            SUM(CP.CLOSING_FEES_AMT) AS CLOSING_FEES_AMT,
            SUM(CP.DISPLAY_ADS_AMT) AS DISPLAY_ADS_AMT,
            SUM(CP.COGS_ADJUSTMENTS_AMT) AS COGS_ADJUSTMENT_AMT,
            SUM(CP.CONTRIBUTION_PROFIT_AMT) AS CONTRIBUTION_PROFIT_AMT,
            SUM(CP.DISCRETIONARY_COOP_AMT) AS DISCRETIONARY_COOP_AMT,
            SUM(CP.FREE_REPLACEMENT_COST_AMT) AS FREE_REPLACEMENT_COST_AMT,
            SUM(CP.FULFILLMENT_FEES_AMT) AS FULFILLMENT_FEES_AMT,
            SUM(CP.GIFTWRAP_COGS_AMT) AS GIFTWRAP_COGS_AMT,
            SUM(CP.GIFTWRAP_REVENUE_AMT) AS GIFTWRAP_REVENUE_AMT,
            SUM(CP.GMS) AS GMS,
            SUM(CP.INBOUND_FREIGHT_AMT) AS INBOUND_FREIGHT_AMT,
            SUM(CP.INPUT_PRICE_VARIANCE) AS INPUT_PRICE_VARIANCE,
            SUM(CP.INVENTORY_ADJUSTMENTS_AMT) AS INVENTORY_ADJUSTMENTS_AMT,
            SUM(CP.LIQUIDATION_COGS_AMT) AS LIQUIDATION_COGS_AMT,
            SUM(CP.LIQUIDATION_REVENUE_AMT) AS LIQUIDATION_REVENUE_AMT,
            SUM(CP.OTHER_REVENUE_AMT) AS OTHER_REVENUE_AMT,
            SUM(CP.PAYMENT_FEES_AMT) AS PAYMENT_FEES_AMT,
            SUM(CP.PRODUCT_COGS_AMT) AS PRODUCT_COGS_AMT,
            SUM(CP.PRODUCT_GMS) AS PRODUCT_GMS,
            SUM(CP.PURCHASE_PRICE_VARIANCE) AS PURCHASE_PRICE_VARIANCE,
                                                            SUM(CP.QUICK_PAY_DISCOUNTS_AMT) AS QUICK_PAY_DISCOUNTS_AMT,
                                                            SUM(CP.REFUNDS_COST_AMT) AS REFUNDS_COST_AMT,
                                                            SUM(CP.REFUNDS_REVENUE_AMT) AS REFUNDS_REVENUE_AMT,
                                                            SUM(CP.REVENUE_SHARE_AMT) AS REVENUE_SHARE_AMT,
                                                            SUM(CP.SALES_DISCOUNTS_AMT) AS SALES_DISCOUNTS_AMT,
                                                            SUM(CP.SELLER_CREDITS_AMT) AS SELLER_CREDITS_AMT,
                                                            SUM(CP.SHIPPING_COST_AMT) AS SHIPPING_COST_AMT,
                                                            SUM(CP.SHIPPING_REVENUE_AMT) AS SHIPPING_REVENUE_AMT,
                                                            SUM(CP.SPONSORED_LINK_FEES_AMT) AS SPONSORED_LINK_FEES_AMT,
                                                            SUM(CP.SUBSCRIPTION_REVENUE_AMT) AS SUBSCRIPTION_REVENUE_AMT,
                                                            SUM(CP.SYNDICATED_STORES_AMT) AS SYNDICATED_STORES_AMT,
                                                            SUM(CP.VARIABLE_CS_COST_AMT) AS VARIABLE_CS_COST_AMT,
                                                            SUM(CP.VARIABLE_FC_COST_AMT) AS VARIABLE_FC_COST_AMT,
                                                            SUM(CP.VENDOR_ALLOWANCES_AMT) AS VENDOR_ALLOWANCES_AMT,
                                                            SUM(CP.VENDOR_RETURNS_AMT) AS VENDOR_RETURNS_AMT,
                                                            SUM(CP.WD_LIQUIDATION_COST_AMT) AS WD_LIQUIDATION_COST_AMT,
                                                            SUM(CP.WD_LIQUIDATION_REVENUE_AMT) AS WD_LIQUIDATION_REVENUE_AMT,
                                                            SUM(CP.QUANTITY_SHIPPED) AS QUANTITY_SHIPPED
from t3 as t join booker.O_WBR_CP_NA as cp on t.asin = cp.asin and t.ship_day = cp.ship_day and t.customer_shipment_item_id = cp.customer_shipment_item_id
INNER JOIN BOOKER.D_MP_ASINS AS DMP ON CP.ASIN = DMP.ASIN
WHERE cp.merchant_customer_id = 8833336105
and cp.marketplace_id = 771770
AND DMP.REGION_ID = 1
         AND DMP.MARKETPLACE_ID = 771770
group BY 1,2,3,4,5,6,7,8,9,10,11

     '''



# Create a dataframe
df1 = pd.read_sql_query(sql_str_train, con=engine)
df1.to_csv('TESTX4.csv')

# Close connection to cluster
engine.dispose()









