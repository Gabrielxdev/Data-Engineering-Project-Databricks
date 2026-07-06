# Databricks notebook source
# MAGIC %md
# MAGIC **Import Required Libraries**

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC **Load Project Utilities & Initialize Notebook Widgets**

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "gross_price", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-dp-gabriel/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze

# COMMAND ----------

df = (
    spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(base_path)
        .withColumn("read_timestamp", F.current_timestamp())
        .select("*", "_metadata.file_name", "_metadata.file_size")
)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

f"{catalog}.{bronze}.{data_source}"

# COMMAND ----------

df.write\
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{bronze}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver

# COMMAND ----------

df_bronze = spark.sql(f"select * from {catalog}.{bronze}.{data_source}")
df_bronze.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformations**

# COMMAND ----------

# MAGIC %md
# MAGIC - 1: Normalise `month` field

# COMMAND ----------

df_bronze.select('month').distinct().show()

# COMMAND ----------

# DBTITLE 1,Cell 18
date_formats = ["yyyy/MM/dd", "dd/MM/yyyy", "yyyy-MM-dd", "dd-MM-yyyy"]

df_silver = df_bronze.withColumn(
    "month",
    F.coalesce(
        F.try_to_date(F.col("month"), "yyyy/MM/dd"),
        F.try_to_date(F.col("month"), "dd/MM/yyyy"),
        F.try_to_date(F.col("month"), "yyyy-MM-dd"),
        F.try_to_date(F.col("month"), "dd-MM-yyyy")
    )
)

# COMMAND ----------

df_silver.select('month').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC - 2: Handling `gross_price`

# COMMAND ----------

df_silver.show(10)

# COMMAND ----------

# DBTITLE 1,Cell 22
# Validação e limpeza da coluna gross_price
# 
# REGEX EXPLICADO: r'^-?\d+(\.\d+)?$'
# Este regex valida se a string é um número válido (inteiro ou decimal, positivo ou negativo)
#
# ^          → Início da string (garante que checamos desde o começo)
# -?         → Hífen opcional (permite números negativos como -50)
#    -       → Caractere hífen literal
#     ?      → Zero ou uma ocorrência (o número pode ter ou não o sinal de menos)
# \d+        → Um ou mais dígitos obrigatórios (0-9)
#    \d      → Qualquer dígito
#      +     → Um ou mais (garante pelo menos um dígito)
# (\.\d+)?   → Grupo opcional: ponto decimal seguido de dígitos
#    \.      → Ponto literal (escapa o . porque ele é especial em regex)
#      \d+   → Um ou mais dígitos após o ponto
#         ?  → O grupo inteiro é opcional (aceita inteiros sem decimal)
# $          → Fim da string (garante que não há caracteres extras)
#
# EXEMPLOS QUE PASSAM (são números válidos):
# "123"      → inteiro positivo ✅
# "-45"      → inteiro negativo ✅
# "123.45"   → decimal positivo ✅
# "-78.99"   → decimal negativo ✅
# "0.5"      → decimal começando com zero ✅
#
# EXEMPLOS QUE FALHAM (não são números válidos):
# "abc"      → letras ❌
# "12.34.56" → múltiplos pontos ❌
# "12a34"    → letra no meio ❌
# ""         → vazio ❌
# "--5"      → dois hífens ❌
#
# LÓGICA COMPLETA:
# 1. Se gross_price é número válido (regex passa):
#    a. Se negativo → multiplica por -1 (torna positivo)
#    b. Se positivo → mantém como está
#    c. Converte para double
# 2. Se não é número válido (regex falha) → substitui por 0

df_silver = df_silver.withColumn(
    "gross_price", 
    F.when(
        F.col("gross_price").rlike(r'^-?\d+(\.\d+)?$'),
        F.when(
            F.col("gross_price").cast("double") < 0, 
            -1 * F.col("gross_price").cast("double")
        ).otherwise(
            F.col("gross_price").cast("double")
        )
    ).otherwise(0)
)

# COMMAND ----------

df_silver.show(10)

# COMMAND ----------

# We enrich the silver dataset by performing an inner join with the products table to fetch the correct product_code for each product_id.
df_products = spark.table("fmcg.silver.products")
df_joined = df_silver.join(df_products.select("product_id", "product_code"), on="product_id", how="inner")
df_joined = df_joined.select("product_id", "product_code", "month", "gross_price", "read_timestamp", "file_name", "file_size")
df_joined.show(5)

# COMMAND ----------

df_joined.write\
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

df_silver = spark.sql(f"select * from {catalog}.{silver_schema}.{data_source};")

# COMMAND ----------

df_gold = df_silver.select("product_code", "month", "gross_price")
df_gold.show(5)

# COMMAND ----------

df_gold.write\
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merging Data source with parent

# COMMAND ----------

df_gold_price = spark.table("fmcg.gold.sb_dim_gross_price")
df_gold_price.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC - Get the price for each product_code (aggregated by year)

# COMMAND ----------

df_gold_price = (
    df_gold_price
    .withColumn("year", F.year("month"))
    .withColumn("is_zero", F.when(F.col("gross_price") == 0, 1).otherwise(0))
)

w = (
    Window
    .partitionBy("product_code", "year")
    .orderBy(F.col("is_zero"), F.col("month").desc())
)

df_gold_latest_price = (
    df_gold_price
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") == 1)
)


# COMMAND ----------

display(df_gold_latest_price)

# COMMAND ----------

df_gold_latest_price = df_gold_latest_price.select("product_code", "year", "gross_price").withColumnRenamed(
    "gross_price", "price_inr").select("product_code", "price_inr", "year")

df_gold_latest_price = df_gold_latest_price.withColumn("year", F.col("year").cast("string"))

df_gold_latest_price.show(5)

# COMMAND ----------

df_gold_latest_price.printSchema()

# COMMAND ----------

delta_table = DeltaTable.forName(spark, f"fmcg.gold.dim_gross_price")

delta_table.alias("target").merge(
    source=df_gold_latest_price.alias("source"), 
    condition="target.product_code = source.product_code"
).whenMatchedUpdate(
    set = {
        "price_inr": "source.price_inr", 
        "year": "source.year"
    }
).whenNotMatchedInsert(
    values = {
        "product_code": "source.product_code",
        "price_inr": "source.price_inr", 
        "year": "source.year"
    }
).execute()