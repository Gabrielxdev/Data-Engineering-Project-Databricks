# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze, silver_schema, gold_schema)

# COMMAND ----------

# DBTITLE 1,Cell 4
dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-dp-gabriel/{data_source}'
landing_path = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"

print("Landing Path: ", landing_path)   
print("Processed Path: ", processed_path)  

bronze_table = f"{catalog}.{bronze}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"

print(f"Bronze Table: {bronze_table}")
print(f"Silver Table: {silver_table}")
print(f"Gold Table: {gold_table}")


# COMMAND ----------

# DBTITLE 1,Cell 5
files = dbutils.fs.ls(landing_path)
csv_files = [f.path for f in files if f.name.endswith('.csv')]

if not csv_files:
    print(f"No new CSV files in {landing_path}. Nothing to process.")
    df = None  # Define explicitamente como None
    dbutils.notebook.exit("No new data")

df = spark.read.options(header=True, inferSchema=True).csv(f"{landing_path}/*.csv").withColumn("read_timestamp", F.current_timestamp()).select("*", "_metadata.file_name", "_metadata.file_size")

print("Total Rows: ", df.count())
df.show(5)

# COMMAND ----------

# DBTITLE 1,Cell 6
# Only write if df exists and is not None (Cell 5 creates df only when there are new files)
if 'df' in locals() and df is not None:
    df.write\
        .format("delta") \
        .option("delta.enableChangeDataFeed", "true") \
        .mode("append") \
        .saveAsTable(bronze_table)
    print(f"Data written to {bronze_table}")
else:
    print("No DataFrame to write (no new data in landing)")

# COMMAND ----------

# DBTITLE 1,Cell 7
# Only move files if there are files to move
files = dbutils.fs.ls(landing_path)
csv_files = [f for f in files if f.name.endswith('.csv')]

if csv_files:
    for file_info in csv_files:
        dbutils.fs.mv(
            file_info.path,
            f"{processed_path}/{file_info.name}",
            True
        )
    print(f"Moved {len(csv_files)} files to processed")
else:
    print("No files to move")

# COMMAND ----------

# DBTITLE 1,Cell 8
# Read from bronze table (includes all historical data, not just current run)
df_orders = spark.sql(f"select * from {bronze_table}")
print(f"Total records in {bronze_table}: {df_orders.count()}")
df_orders.show(2)

# COMMAND ----------

df_orders = df_orders.filter(F.col("order_qty").isNotNull())

# COMMAND ----------

df_orders = df_orders.withColumn(
    "customer_id", 
    F.when(F.col("customer_id").rlike("^[0-9]+$"), F.col("customer_id")) # keep only digits
    .otherwise("999999")
    .cast("string")
)

#3. Remove weekday name from the date text
# "Tuesday, July 01, 2025" -> "July 01, 2025"
# ^ = start of string
# [A-Za-z]+ = um ou mais caracteres alfabeticos minusculo ou maiusculo
# , = uma virgula literal
# \s* = zero ou mais espaços em branco
# "" = substituir por nada
# entrada = "Tuesday, July 01, 2025"
# saida = "July 01, 2025"
df_orders = df_orders.withColumn(
    "order_placement_date", 
    F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)


#4. Parse order_placement_date using multiple possible formats 
df_orders = df_orders.withColumn(
    "order_placement_date", 
    F.coalesce(
        F.try_to_date("order_placement_date", "yyyy/MM/dd"),
        F.try_to_date("order_placement_date", "dd-MM-yyyy"),
        F.try_to_date("order_placement_date", "dd/MM/yyyy"),
        F.try_to_date("order_placement_date", "MMMM dd, yyyy"),
    )
)


#5. Drop duplicates 
df_orders = df_orders.dropDuplicates(["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"])

#6. Convert product id to string
df_orders = df_orders.withColumn('product_id', F.col('product_id').cast('string'))



# COMMAND ----------

# check what's the maximum and minimum date
df_orders.agg(
    F.min("order_placement_date").alias("min_date"),
    F.max("order_placement_date").alias("max_date")
).show()

# COMMAND ----------

display(df_orders.limit(20))

# COMMAND ----------

df_products = spark.table("fmcg.silver.products")

display(df_products.limit(5))

# COMMAND ----------

df_joined = df_orders.join(df_products, on="product_id", how="inner").select(df_orders["*"], df_products["product_code"])

display(df_joined.limit(10))

# COMMAND ----------

if not (spark.catalog.tableExists(silver_table)):
    df_joined.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(silver_table)
else:
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias("silver").merge(df_joined.alias("bronze"), "silver.order_placement_date = bronze.order_placement_date and silver.order_id = bronze.order_id and silver.product_code = bronze.product_code and silver.customer_id = bronze.customer_id").whenMatchedUpdateAll().whenNotMatchedInsertAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold

# COMMAND ----------

df_gold = spark.sql(f"select order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity from {silver_table};")

df_gold.show(2)

# COMMAND ----------

gold_table

# COMMAND ----------

if not (spark.catalog.tableExists(gold_table)):
    print("creating new table")
    df_gold.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias("source").merge(df_gold.alias("gold"), "source.date = gold.date and source.order_id = gold.order_id and source.product_code = gold.product_code and source.customer_code = gold.customer_code ").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge with Parent Company

# COMMAND ----------

df_child = spark.sql(f"select date, product_code, customer_code, sold_quantity from {gold_table} ")
df_child.show(10)

# COMMAND ----------

df_child.count()

# COMMAND ----------

# first change the date to first day of the month 
# 2025-07-12 --> 2025-07-1


df_monthly = (
    df_child
    .withColumn("month_start", F.trunc("date", "MM")) #truncar para o início do mês 

    .groupBy("month_start", "product_code", "customer_code")
    .agg(
        F.sum("sold_quantity").alias("sold_quantity")
    )
    .withColumnRenamed("month_start", "date")
)

display(df_monthly.limit(10))

# COMMAND ----------

df_monthly.count()

# COMMAND ----------

gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
gold_parent_delta.alias("parent_gold").merge(df_monthly.alias("child"), "parent_gold.date = child.date and parent_gold.product_code = child.product_code and parent_gold.customer_code = child.customer_code ").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

