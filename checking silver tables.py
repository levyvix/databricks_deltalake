# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------



# COMMAND ----------

dbutils.fs.mount(
    source=f"wasbs://silver@traininglakehousede.blob.core.windows.net",
    mount_point=f"/mnt/silver",
    extra_configs={
        f"fs.azure.account.key.traininglakehousede.blob.core.windows.net": "RIDJB43YxJ+o9jYUUMJwIy8+gMQ02PTqtg1/92gmg7Y+ziPxNqLnJVSNoqattcRQhoK4WFdSRIcd+ASt7cLMgA=="
    }
)

# COMMAND ----------

df_geo = spark.read.format('delta').load('/mnt/silver/geolocation')

# COMMAND ----------

df_logs = spark.read.format('delta').load('/mnt/silver/logs')

# COMMAND ----------

df_geo.printSchema()

# COMMAND ----------

df_geo.groupBy('country_name').count().orderBy(desc("count")).show()

# COMMAND ----------

df_logs.count()

# COMMAND ----------


