from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Global Sales Data Merge") \
    .getOrCreate()

# ------------------------------
# 1. GCS file paths
# ------------------------------
gcs_bucket = "b-sales-data"
gcs_path = f"gs://{gcs_bucket}"

# Load files from GCS
df_japan = spark.read.option("header", True).csv(f"{gcs_path}/japan_sales.csv") \
    .withColumn("Country", F.lit("Japan"))

df_hk = spark.read.option("header", True).format("com.crealytics.spark.excel") \
    .option("dataAddress", "'Sheet1'!A1") \
    .option("useHeader", "true") \
    .option("inferSchema", "true") \
    .load(f"{gcs_path}/hongkong_sales.xlsx") \
    .withColumn("Country", F.lit("Hong Kong"))

df_sl = spark.read.option("multiline", "true").json(f"{gcs_path}/srilanka_sales.json") \
    .withColumn("Country", F.lit("Sri Lanka"))

# ------------------------------
# 2. Load from MySQL, PostgreSQL, SQL Server
# ------------------------------
def read_jdbc(dbtype, url, driver, table, user, password, country):
    return spark.read.format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password", password) \
        .load().withColumn("Country", F.lit(country))

# MySQL: Oman, Germany, Qatar
mysql_url = "jdbc:mysql://34.27.63.20:3306/sales_data"
mysql_user = "kmk"
mysql_pass = "your_password"
mysql_driver = "com.mysql.cj.jdbc.Driver"

df_oman = read_jdbc("mysql", mysql_url, mysql_driver, "oman_sales", mysql_user, mysql_pass, "Oman")
df_germany = read_jdbc("mysql", mysql_url, mysql_driver, "germany_sales", mysql_user, mysql_pass, "Germany")
df_qatar = read_jdbc("mysql", mysql_url, mysql_driver, "qatar_sales", mysql_user, mysql_pass, "Qatar")

# PostgreSQL: Norway
postgres_url = "jdbc:postgresql://35.186.189.50:5432/sales_data"
postgres_user = "postgres"
postgres_pass = "12345"
postgres_driver = "org.postgresql.Driver"

df_norway = read_jdbc("postgres", postgres_url, postgres_driver, "norway_sales", postgres_user, postgres_pass, "Norway")

# SQL Server: India
sqlserver_url = "jdbc:sqlserver://34.29.113.36:1433;databaseName=sales_data"
sqlserver_user = "sqlserver"
sqlserver_pass = "12345"
sqlserver_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

df_india = read_jdbc("sqlserver", sqlserver_url, sqlserver_driver, "india_sales", sqlserver_user, sqlserver_pass, "India")

# ------------------------------
# 3. Merge all DataFrames
# ------------------------------
final_df = df_japan.unionByName(df_hk, allowMissingColumns=True) \
    .unionByName(df_sl, allowMissingColumns=True) \
    .unionByName(df_oman, allowMissingColumns=True) \
    .unionByName(df_germany, allowMissingColumns=True) \
    .unionByName(df_qatar, allowMissingColumns=True) \
    .unionByName(df_norway, allowMissingColumns=True) \
    .unionByName(df_india, allowMissingColumns=True)

# ------------------------------
# 4. Save to GCS
# ------------------------------
output_path = f"gs://{gcs_bucket}/final/global_sales_combined"
final_df.write.option("header", True).mode("overwrite").csv(output_path)

print("✅ Merged file written to:", output_path)
