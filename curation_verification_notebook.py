# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,array,ArrayType,DateType,TimestampType
from pyspark.sql import functions as f
from pyspark.sql.functions import udf
import hashlib
import datetime
from datetime import timedelta

# COMMAND ----------

# DBTITLE 1,Parameters
STORAGE_ACCOUNT="traininglakehousede"
ADLS_KEY="RIDJB43YxJ+o9jYUUMJwIy8+gMQ02PTqtg1/92gmg7Y+ziPxNqLnJVSNoqattcRQhoK4WFdSRIcd+ASt7cLMgA=="

spark.conf.set("fs.azure.account.key."+STORAGE_ACCOUNT+".blob.core.windows.net", ADLS_KEY)

# COMMAND ----------

store_orders = spark.read.format('delta').load('wasbs://silver@traininglakehousede.blob.core.windows.net/sales/store_orders')

store_orders.show(4)

# COMMAND ----------

# DBTITLE 1,Verify Unstandardized Data in Store Orders
# MAGIC %sql
# MAGIC SELECT * FROM store_orders;

# COMMAND ----------

# DBTITLE 1,Verify Unstandardized Data in ecommerce Orders
# MAGIC %sql
# MAGIC SELECT order_date FROM esalesns;

# COMMAND ----------

# DBTITLE 1,Verify Store Customers Email
# MAGIC %sql
# MAGIC SELECT email FROM store_customers;

# COMMAND ----------

# DBTITLE 1,Verify Schema of products
# MAGIC %sql
# MAGIC DESCRIBE products

# COMMAND ----------

# DBTITLE 1,Verify Schema of store_orders
# MAGIC %sql
# MAGIC DESCRIBE store_orders

# COMMAND ----------

# DBTITLE 1,Verify country in store_customers
# MAGIC %sql
# MAGIC select customer_name,country from store_customers

# COMMAND ----------

# DBTITLE 1,Verify country in ecommerce transactions
# MAGIC %sql
# MAGIC select customer_name,country from esalesns

# COMMAND ----------

# DBTITLE 1,Verify currency conversion
# MAGIC %sql
# MAGIC select order_number, sale_price, sale_price_usd FROM store_orders WHERE currency='EUR'

# COMMAND ----------

# MAGIC %sql
# MAGIC select order_number, sale_price, sale_price_usd FROM esalesns WHERE currency='EUR'

# COMMAND ----------

# DBTITLE 1,Verify customer name merge
# MAGIC %sql
# MAGIC SELECT customer_name FROM store_customers WHERE email = 'phasellus.vitae@vitae.co.uk'

# COMMAND ----------

# DBTITLE 1,Verify store orders price adjustments
# MAGIC %sql
# MAGIC SELECT order_number, order_date,sale_price FROM store_orders WHERE order_number in (500, 1254, 1501, 2234, 2345)

# COMMAND ----------

# DBTITLE 1,Verify Data Masking
# MAGIC %sql
# MAGIC select customer_name, address, phone, credit_card from store_customers

# COMMAND ----------


