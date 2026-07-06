# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists fmcg;
# MAGIC use catalog fmcg;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists fmcg.gold;
# MAGIC create schema if not exists fmcg.silver;
# MAGIC create schema if not exists fmcg.bronze;
# MAGIC
# MAGIC