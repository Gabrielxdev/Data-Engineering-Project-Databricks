# Databricks notebook source
# MAGIC %md
# MAGIC **Import Required Libraries**

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC **Load Project Utilities & Initialize Notebook Widgets**

# COMMAND ----------

# DBTITLE 1,Cell 6
dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "products", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-dp-gabriel/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

# DBTITLE 1,Cell 5
# MAGIC %run ../1_setup/utilities

# COMMAND ----------

print(bronze, silver_schema, gold_schema)

# COMMAND ----------

# DBTITLE 1,Cell 6
dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "products", "Data Source")

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

# print check data type
df.printSchema()

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

df.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{bronze}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver
# MAGIC

# COMMAND ----------

df_bronze = spark.sql(f"select * from {catalog}.{bronze}.{data_source};")
df_bronze.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformations**

# COMMAND ----------

# MAGIC %md
# MAGIC - 1: Drop Duplicates

# COMMAND ----------

print('Rows before duplicates dropped: ', df_bronze.count())
df_silver = df_bronze.dropDuplicates(['product_id'])
print('Rows after duplicates dropped: ', df_silver.count())

# COMMAND ----------

# MAGIC %md
# MAGIC - 2: Title case fix
# MAGIC
# MAGIC (energy bars ---> Energy Bars, protien bars ---> Protien Bars etc)

# COMMAND ----------

df_silver.select('category').distinct().show()

# COMMAND ----------

df_silver = df_silver.withColumn(
    "category",
    F.when(F.col("category").isNull(), None)
        .otherwise(F.initcap("category"))
)

# COMMAND ----------

df_silver.select("category").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC - 3: Fix Spelling Mistake for `Protien`

# COMMAND ----------

df_silver = (
    df_silver
    .withColumn(
        "product_name",
        F.regexp_replace(F.col("product_name"), "(?i)Protein", "Protein")
    )
    .withColumn(
        "category",
        F.regexp_replace(F.col("category"), "(?i)Protein", "Protein")
    )
)

# COMMAND ----------

display(df_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardizing Customer Attributes to Match Parent Company Data Model

# COMMAND ----------

# DBTITLE 1,Cell 26
### 1: Add division column
df_silver = (
    df_silver
    .withColumn(
        "division",
        F.when(F.col("category") == "Energy Bars",        "Nutrition Bars")
         .when(F.col("category") == "Protein Bars",       "Nutrition Bars")
         .when(F.col("category") == "Granola & Cereals",  "Breakfast Foods")
         .when(F.col("category") == "Recovery Dairy",     "Dairy & Recovery")
         .when(F.col("category") == "Healthy Snacks",     "Healthy Snacks")
         .when(F.col("category") == "Electrolyte Mix",    "Hydration & Electrolytes")
         .otherwise("Other")
    )
)


### 2: Variant column - Extrai texto entre parênteses do nome do produto
# 
# REGEX EXPLICADO: r"\((.*?)\)"
#
# \(      → Busca o caractere '(' literal (escapa com \ porque ( é especial em regex)
# (.*?)   → Grupo de captura 1: captura qualquer texto entre os parênteses
#    .    → Qualquer caractere
#    *    → Zero ou mais vezes
#    ?    → Não-guloso (para no primeiro ')' encontrado)
# \)      → Busca o caractere ')' literal
# 1       → Retorna o grupo de captura 1 (o texto dentro dos parênteses)
#
# EXEMPLOS:
# "Energy Bar (Chocolate)"  → extrai "Chocolate"
# "Protein Shake (Vanilla)" → extrai "Vanilla"
# "Granola Mix"             → sem parênteses, retorna "" (vazio)
#
df_silver = df_silver.withColumn(
    "variant",
    F.regexp_extract(F.col("product_name"), r"\((.*?)\)", 1)
)


### 3: Create new column: product_code  

# Invalid product_ids are replaced with a fallback value to avoid losing fact records and ensure downstream joins remain consistent

df_silver = (
    df_silver
    # 1. Generate deterministic product_code from product_name
    .withColumn(
        "product_code",
        #F.sha2() = função de hash criptografada SHA-256
        #256 tamanho do hash 256 bits
    
        F.sha2(F.col("product_name").cast("string"), 256)
    )
    # 2. Clean product_id: keep only numeric IDs, else set to 999999
    .withColumn(
        "product_id",
        F.when(
            F.col("product_id").cast("string").rlike("^[0-9]+$"),
            F.col("product_id").cast("string")
        ).otherwise(F.lit(999999).cast("string"))
    )
    # 3. Rename product_name → product
    .withColumnRenamed("product_name", "product")
)

# COMMAND ----------

df_silver = df_silver.select("product_code", "division", "category", "product", "variant", "product_id", "read_timestamp", "file_name", "file_size")

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")
df_gold = df_silver.select("product_code", "product_id", "division", "category", "product", "variant")
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

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_products")
df_child_products = spark.sql(f"SELECT product_code, division, category, product, variant FROM fmcg.gold.sb_dim_products;")
df_child_products.show(5)

# COMMAND ----------

delta_table.alias("target").merge(
    source=df_child_products.alias("source"),
    condition="target.product_code = source.product_code"
).whenMatchedUpdate(
    set={
        "division": "source.division",
        "category": "source.category",
        "product": "source.product",
        "variant": "source.variant"
    }
).whenNotMatchedInsert(
    values={
        "product_code": "source.product_code",
        "division": "source.division",
        "category": "source.category",
        "product": "source.product",
        "variant": "source.variant"
    }
).execute()

# COMMAND ----------

