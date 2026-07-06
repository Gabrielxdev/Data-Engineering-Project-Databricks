# Databricks notebook source
from pyspark.sql import functions as F 
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-dp-gabriel/{data_source}/*.csv'

print(base_path)

# COMMAND ----------

df = ( 
      spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(base_path)
        .withColumn("read_timestamp", F.current_timestamp())
        .select("*", "_metadata.file_name", "_metadata.file_size")
)

display(df.limit(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Salvar na camada Bronze usando modo append
df.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("append") \
    .saveAsTable(f"{catalog}.{bronze}.{data_source}")

print(f"✅ Dados salvos com sucesso em: {catalog}.{bronze}.{data_source}")

# COMMAND ----------

df_bronze = spark.sql(f"select * from {catalog}.{bronze}.{data_source};")
df_bronze.show(10)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

df_duplicates = df_bronze.groupBy("customer_id").count().filter(F.col("count") > 1)
display(df_duplicates)

# COMMAND ----------

df_silver = df_bronze.dropDuplicates(['customer_id'])

# COMMAND ----------

print('Rows before duplicates dropped: ', df_bronze.count())
df_silver = df_bronze.dropDuplicates(['customer_id'])
print('Rows after duplicates dropped: ', df_silver.count())

# COMMAND ----------

display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "customer_name",
    F.trim(F.col("customer_name"))
)

# COMMAND ----------

display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)

# COMMAND ----------

df_silver.select('city').distinct().show()

# COMMAND ----------

# DBTITLE 1,City name mapping
# typos → correct names
city_mapping = {
    'Bengaluruu': 'Bengaluru',
    'Bengalore': 'Bengaluru',
    
    'Hyderabad': 'Hyderabad',
    'Hyderbad': 'Hyderabad',
    
    'NewDelhi': 'New Delhi',
    'Newdelhi': 'New Delhi',
    'NewDelhie': 'New Delhi'
}

allowed = ['Bengaluru', 'Hyderabad', 'New Delhi']

df_silver = (

    df_silver
    .replace(city_mapping, subset=["city"])
    .withColumn(
        "city",
        F.when(F.col("city").isNull(), None)
        .when(F.col("city").isin(allowed), F.col("city"))
        .otherwise(None)
    )
)

df_silver.select('city').distinct().show()

# COMMAND ----------

df_silver.select('customer_name').distinct().show()

# COMMAND ----------

# DBTITLE 1,Cell 20
df_silver = df_silver.withColumn(
    "customer_name",
    F.when(F.col("customer_name").isNull(), None)
    .otherwise(F.initcap(F.col("customer_name")))
)

df_silver.select('customer_name').distinct().show()

# COMMAND ----------

df_silver.filter(F.col("city").isNull()).show(truncate=False)

# COMMAND ----------

null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', "Recovery Lane"]

df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

customer_city_fix = {
    789403: "New Delhi",
    789420: "Bengaluru",
    789521: "Hyderabad",
    789603: "Hyderabad",
    789221: "Bengaluru",
    789522: "Hyderabad",
    789422: "Hyderabad"

}

df_fix = spark.createDataFrame(
    [(k, v) for k, v in customer_city_fix.items()],
    ["customer_id", "fixed_city"]
)

display(df_fix)

# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix, "customer_id", "left")
    .withColumn(
        "city",
        F.coalesce("city", "fixed_city")
    )
    .drop("fixed_city")
)

display(df_silver)

# COMMAND ----------

null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', "Recovery Lane"]

df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

df_silver.filter(F.col("city").isNull()).show(truncate=False)

# COMMAND ----------

df_silver = df_silver.withColumn(
    "customer_id",
    F.col("customer_id").cast("string")
)

print(df_silver.printSchema())

# COMMAND ----------

# DBTITLE 1,Cell 28
df_silver = (
    df_silver.withColumn(
        "customer",
        #significa "concatenate with separator" (concatenar com separador)
        F.concat_ws("-", "customer_name", F.coalesce(F.col("city"), F.lit("Unknown")))
    )
    .withColumn("market", F.lit("India"))
    .withColumn("platform", F.lit("Sports Bar"))
    .withColumn("channel", F.lit("Acquisition"))
    .drop("platformm")  # Remove coluna antiga com typo
)

display(df_silver.limit(5))

# COMMAND ----------

df_silver.write\
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# DBTITLE 1,Cell 30
df_gold = df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")

# COMMAND ----------

display(df_gold.limit(5))

# COMMAND ----------

# DBTITLE 1,Cell 32
df_gold.write\
    .format("delta") \
    .option("delta.enableChangeDataFeed","true") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

# DBTITLE 1,Cell 33
delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
df_child_customers = spark.table("fmcg.gold.sb_dim_customers").select(
    F.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()