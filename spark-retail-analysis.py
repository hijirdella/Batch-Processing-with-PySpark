import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, countDistinct, when, max, to_date, current_date, datediff
from dotenv import load_dotenv

# Load environment variables from .env file
#dotenv_path = Path('/opt/app/.env')
#load_dotenv(dotenv_path=dotenv_path)

# Fetch environment variables
postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = 'warehouse'
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')
#spark_stringtype = os.getenv('SPARK_STRINGTYPE')

# Validate the necessary environment variables are set
#if not all([postgres_host, postgres_dw_db, postgres_user, postgres_password]):
    #raise ValueError("PostgreSQL environment variables are not all set properly.")

# Initialize Spark session with PostgreSQL JDBC driver using packages
try:
    spark = (
        SparkSession.builder
        .appName("Dibimbing")
        .master("local")  
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18")  # Include PostgreSQL JDBC driver
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("SparkSession created successfully.")
except Exception as e:
    print(f"Error creating SparkSession: {e}")
    raise

# Set up PostgreSQL connection details
jdbc_url = f'jdbc:postgresql://{postgres_host}/{postgres_dw_db}'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}
print(jdbc_url)
print(jdbc_properties)
try:
    # Load retail data from PostgreSQL
    retail_df = spark.read.jdbc(
        jdbc_url,
        'public.retail',
        properties=jdbc_properties
    )

    # Example 1: Total Sales by Country
    sales_by_country = retail_df.groupBy("Country").agg(
        _sum(col("Quantity") * col("UnitPrice")).alias("TotalSales")
    ).orderBy(col("TotalSales").desc())
    sales_by_country.show()

    # Example 2: Churn-Retention Analysis
    retail_df = retail_df.withColumn("InvoiceDate", to_date("InvoiceDate", "MM/dd/yyyy"))
    churn_df = retail_df.groupBy("CustomerID").agg(
        _sum(col("Quantity") * col("UnitPrice")).alias("TotalSpent"),
        countDistinct("InvoiceNo").alias("TotalOrders"),
        max("InvoiceDate").alias("LastPurchaseDate")
    ).withColumn(
        "ChurnRisk",
        when(datediff(current_date(), col("LastPurchaseDate")) > 180, "High").otherwise("Low")
    )
    churn_df.show()

    # Save the results to PostgreSQL
    sales_by_country.write.jdbc(url=jdbc_url, table="sales_by_country", mode="overwrite", properties=jdbc_properties)
    churn_df.write.jdbc(url=jdbc_url, table="churn_analysis", mode="overwrite", properties=jdbc_properties)
    print("Results successfully saved to PostgreSQL.")

    # Save the results to CSV
    sales_by_country.write.csv("/opt/airflow/results/sales_by_country.csv", header=True, mode="overwrite")
    churn_df.write.csv("/opt/airflow/results/churn_analysis.csv", header=True, mode="overwrite")
    print("Results successfully saved to CSV.")
except Exception as e:
    print(f"Error processing data: {e}")
    raise
finally:
    try:
        spark.stop()
        print("Spark session stopped successfully.")
    except Exception as e:
        print(f"Error stopping Spark session: {e}")
