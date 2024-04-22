# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import lower
from pyspark.sql.functions import regexp_replace, lower

# get data from CSVs
issues_file = "/mnt/data/yasas/issues.csv"
prs_file = "/mnt/data/yasas/prs.csv"
repos_file = "/mnt/data/yasas/repos.csv"

# Read the file into a dataframe
issues = spark.read.csv(issues_file, header=True, inferSchema=True)
prs = spark.read.csv(prs_file, header=True, inferSchema=True)
repos = spark.read.csv(repos_file, header=True, inferSchema=True)


# COMMAND ----------

# Convert to lower cases
issues = issues.withColumn('name', lower(issues['name']))
prs = prs.withColumn('name', lower(prs['name']))
repos = repos.withColumn('language', lower(repos['language']))

# Remove line breaks
issues = issues.withColumn('name', regexp_replace('name', '\n', ''))
prs = prs.withColumn('name', regexp_replace('name', '\n', ''))
repos = repos.withColumn('language', regexp_replace('language', '\n', ''))


# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS yasas_db")

# COMMAND ----------

from pyspark.sql import DataFrame

def write_data_to_delta_table(source_data_frame: DataFrame, hive_table: str):
    """
    Writes data to a Delta table in append mode, with options for mergeSchema.

    Parameters:
    - source_data_frame: The source DataFrame to write.
    - hive_table: The full name of the Hive table to write to (including database name).
    """
    (source_data_frame
     .write
     .format("delta")
     .mode("append")
     .option("mergeSchema", "true")
     .saveAsTable(hive_table))

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# Writing data to the repos table
write_data_to_delta_table(issues, "yasas_db.issues")

# Writing data to the issues table
write_data_to_delta_table(prs, "yasas_db.prs")

# Writing data to the prs table
write_data_to_delta_table(repos, "yasas_db.repos")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW yasas_db.Vw_Languages AS (
# MAGIC SELECT name, SUM(num_repos) AS total_repos
# MAGIC FROM (
# MAGIC     SELECT language AS name, SUM(num_repos) AS num_repos
# MAGIC     FROM yasas_db.repos
# MAGIC     GROUP BY language
# MAGIC     UNION
# MAGIC     SELECT name, 0 AS num_repos
# MAGIC     FROM yasas_db.issues
# MAGIC     UNION
# MAGIC     SELECT name, 0 AS num_repos
# MAGIC     FROM yasas_db.prs
# MAGIC )
# MAGIC GROUP BY name)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS yasas_db.Git_Hub
# MAGIC AS (
# MAGIC     SELECT Sub.name, year, quarter, sum(issue_count) as issue_count, sum(prs_count) as prs_count, sum(Vw_Languages.total_repos) as total_repos
# MAGIC     FROM (
# MAGIC         SELECT DISTINCT name, year, quarter, sum(count) as issue_count, sum(0) as prs_count
# MAGIC         FROM yasas_db.issues
# MAGIC         GROUP BY name, year, quarter
# MAGIC         UNION
# MAGIC         SELECT DISTINCT name, year, quarter, sum(0) as issue_count ,sum(count) as prs_count
# MAGIC         FROM yasas_db.prs
# MAGIC         GROUP BY name, year, quarter
# MAGIC     ) Sub
# MAGIC     INNER JOIN yasas_db.Vw_Languages ON Sub.name = Vw_Languages.name
# MAGIC     GROUP BY Sub.name, year, quarter
# MAGIC     ORDER BY Sub.name
# MAGIC )
