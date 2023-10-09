from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import col, when, current_timestamp
from pyspark.sql.types import BooleanType
from google.cloud import storage  # Import the Google Cloud Storage library

# Initialize a Spark session
spark = SparkSession.builder.appName("DataTransformation").getOrCreate()

# Define your DAG's default arguments
default_args = {
    'owner': 'Taiwo',
    'start_date': datetime(2023, 10, 9),
    'retries': 2,
}

# Create the DAG
dag = DAG(
    'my_data_pipeline3',
    default_args=default_args,
    schedule_interval=None,  # Set the schedule_interval as needed
)
# Initialize a GCS client
storage_client = storage.Client()

# Define the GCS bucket name
bucket_name = 'your-gcs-bucket'

# Define the source blob names for both CSV files
source_blob_name_1 = 'RAW/movie_review.csv'
source_blob_name_2 = 'RAW/log_reviews.csv'

# Define local file paths for both CSV files
local_file_path_1 = 'RAW/first_file.csv'
local_file_path_2 = 'RAW/second_file.csv'

# Function to extract data from GCS for the first file
def extract_data_from_gcs_1(bucket_name, source_blob_name_1, local_file_path_1):
    try:
        # Download the data from GCS to the local filesystem
        transfer = GCSToLocalFilesystemTransfer(
            task_id='download_first_file',
            bucket_name=bucket_name,
            object_name=source_blob_name_1,
            filename=local_file_path_1,
            move_object=True,
        )
        transfer.execute(context=None)

    except Exception as e:
        raise Exception(f"Failed to extract data from GCS (First File): {str(e)}")

# Function to extract data from GCS for the second file
def extract_data_from_gcs_2(bucket_name, source_blob_name_2, local_file_path_2):
    try:
        # Download the data from GCS to the local filesystem
        transfer = GCSToLocalFilesystemTransfer(
            task_id='download_second_file',
            bucket_name=bucket_name,
            object_name=source_blob_name_2,
            filename=local_file_path_2,
            move_object=True,
        )
        transfer.execute(context=None)

    except Exception as e:
        raise Exception(f"Failed to extract data from GCS (Second File): {str(e)}")



# Function to extract data from PostgreSQL
def extract_data_from_postgresql():
    try:
        # PostgreSQL database connection parameters
        db_params = {
            'database': 'userDB',
            'user': 'user',
            'password': '123456',
            'host': '34.70.69.32',
            'port': '5432'
        }

        conn = psycopg2.connect(**db_params)
        user_purchase_query = "SELECT * FROM public.user_purchase;"
        user_purchase_df = pd.read_sql_query(user_purchase_query, conn)
        conn.close()

        return user_purchase_df
    except Exception as e:
        raise Exception(f"Failed to extract data from PostgreSQL: {str(e)}")

# Function to transform movie reviews
def transform_movie_reviews(movie_reviews_df):
    try:
        # Transform movie_reviews_df
        # Tokenize and preprocess the review text
        tokenizer = Tokenizer(inputCol="review_str", outputCol="review_tokens")
        movie_reviews_df = tokenizer.transform(movie_reviews_df)

        # Remove stop words if necessary
        remover = StopWordsRemover(inputCol="review_tokens", outputCol="filtered_tokens")
        movie_reviews_df = remover.transform(movie_reviews_df)

        # Identify positive reviews based on the presence of the word "good"
        movie_reviews_df = movie_reviews_df.withColumn("positive_review", 
            when(col("review_str").contains("good"), True).otherwise(False).cast(BooleanType())
        )

        # Add a timestamp column (insert_date)
        movie_reviews_df = movie_reviews_df.withColumn("insert_date", current_timestamp())

        # Store the transformed DataFrame in the STAGE area
        movie_reviews_df.write.mode("overwrite").csv("gs://us-central1-bootcamp-be3a2980-bucket/STAGE/movie_reviews_transformed")
    except Exception as e:
        raise Exception(f"Failed to transform movie reviews: {str(e)}")

# Function to transform log reviews
def transform_log_reviews(log_reviews_df):
    try:
        # Transform log_reviews_df
        # Your code to process XML data in the log column and extract metadata
        # Assuming log_reviews_df contains a 'log' column with XML data

        # Store the transformed data in a new DataFrame
        transformed_log_reviews_df = log_reviews_df.select(
            col("log_id"), col("log_date"), col("device"), col("os"), col("location"),
            col("browser"), col("ip"), col("phone_number")
        )

        # Store the transformed DataFrame in the STAGE area
        transformed_log_reviews_df.write.mode("overwrite").csv("gs://us-central1-bootcamp-be3a2980-bucket/STAGE/log_reviews_transformed")
    except Exception as e:
        raise Exception(f"Failed to transform log reviews: {str(e)}")

# Task to extract and transform data from GCS
def extract_transform_gcs_data():
    try:
        # Define GCS bucket and file paths
        bucket_name = 'your-gcs-bucket'
        movie_review_gcs_path = 'RAW/movie_review.csv'
        log_reviews_gcs_path = 'RAW/log_reviews.csv'

        # Define local file paths for downloading
        movie_review_local_path = '/tmp/movie_review.csv'
        log_reviews_local_path = '/tmp/log_reviews.csv'

        # Extract data from GCS
        
        
        extract_data_from_gcs_1(bucket_name, movie_review_gcs_path, movie_review_local_path)
        extract_data_from_gcs_2(bucket_name, log_reviews_gcs_path, log_reviews_local_path)

        # Load the downloaded data as DataFrames
        movie_reviews_df = spark.read.csv(movie_review_local_path, header=True)
        log_reviews_df = spark.read.csv(log_reviews_local_path, header=True)

        # Transform movie reviews
        transform_movie_reviews(movie_reviews_df)

        # Transform log reviews
        transform_log_reviews(log_reviews_df)

    except Exception as e:
        raise Exception(f"Failed to extract and transform data from GCS: {str(e)}")

# Task to extract data from PostgreSQL
def extract_from_postgresql():
    try:
        user_purchase_df = extract_data_from_postgresql()

        # Placeholder for your code to transform user_purchase_df if needed
        # ...
        user_purchase_df = spark.read.format("jdbc").option("url", "jdbc:postgresql://34.70.69.32:5432/userDB").option("dbtable", "public.user_purchase").option("user", "user").option("password", "123456").load()
        user_purchase_df.write.mode("overwrite").parquet("STAGE/user_purchase")

        # Placeholder for your code to load the transformed data into the target destination
        # ...

    except Exception as e:
        raise Exception(f"Failed to extract data from PostgreSQL: {str(e)}")

# Task to load data into the target destination (Placeholder - Implement your own logic)
def load_data():
    try:
        # Initialize a Spark session
        spark = SparkSession.builder.appName("DataLoad").getOrCreate()

        # Load the transformed data from the STAGE area
        movie_reviews_transformed_df = spark.read.parquet("STAGE/movie_reviews_transformed")
        log_reviews_transformed_df = spark.read.parquet("STAGE/log_reviews_transformed")
        user_purchase_df = spark.read.parquet("STAGE/user_purchase")

        # Build dimension tables
        # Dim Date table
        dim_date_df = movie_reviews_transformed_df.select("insert_date").distinct()
        dim_date_df = dim_date_df.withColumn("day", dayofmonth("insert_date"))
        dim_date_df = dim_date_df.withColumn("month", month("insert_date"))
        dim_date_df = dim_date_df.withColumn("year", year("insert_date"))
        dim_date_df = dim_date_df.withColumn("season", when((month("insert_date") >= 3) & (month("insert_date") <= 5), "Spring")
                                              .when((month("insert_date") >= 6) & (month("insert_date") <= 8), "Summer")
                                              .when((month("insert_date") >= 9) & (month("insert_date") <= 11), "Fall")
                                              .otherwise("Winter"))

        # Dim Devices table (Assuming you have a device column in log_reviews_transformed_df)
        dim_devices_df = log_reviews_transformed_df.select("device").distinct()

        # Dim Location table (Assuming you have a location column in log_reviews_transformed_df)
        dim_location_df = log_reviews_transformed_df.select("location").distinct()

        # Dim OS table (Assuming you have an os column in log_reviews_transformed_df)
        dim_os_df = log_reviews_transformed_df.select("os").distinct()

        # Dim Browser table (Assuming you have a browser column in log_reviews_transformed_df)
        dim_browser_df = log_reviews_transformed_df.select("browser").distinct()

        # Build fact table fact_movie_analytics
        fact_movie_analytics_df = user_purchase_df.join(movie_reviews_transformed_df, user_purchase_df["customerid"] == movie_reviews_transformed_df["cid"], "inner")
        fact_movie_analytics_df = fact_movie_analytics_df.groupBy("customerid").agg(
            sum(col("quantity") * col("unit_price")).alias("amount_spent"),
            sum(col("positive_review_int")).alias("review_score"),
            count("review_id").alias("review_count")
        )

        # Showcase and demonstrate the outcome
        # You can run analytic queries here to answer the provided questions
        # Example query: fact_movie_analytics_df.filter(col("location").isin("California", "NY", "Texas")).groupBy("device").count().show()

        # Stop the Spark session
        spark.stop()


    except Exception as e:
        raise Exception(f"Failed to load data into the target destination: {str(e)}")

# Task to trigger the entire pipeline
extract_transform_task = PythonOperator(
    task_id='extract_transform_data',
    python_callable=extract_transform_gcs_data,
    dag=dag,
)

# Task to extract data from PostgreSQL
extract_from_postgresql_task = PythonOperator(
    task_id='extract_from_postgresql',
    python_callable=extract_from_postgresql,
    dag=dag,
)

# Task to load data into the target destination
load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Define the task dependencies
# extract_transform_task >> extract_from_postgresql_task >> load_data_task

# ...
# Previous code for DAG setup, extraction, and transformation

# Function to create Data Warehouse tables
def create_dw_tables():
    try:
        # Initialize a connection to your Data Warehouse (e.g., PostgreSQL)
        dw_connection_params = {
            'database': 'userDB',
            'user': 'user',
            'password': '123456',
            'host': '34.70.69.32',
            'port': '5432'
        }

        dw_conn = psycopg2.connect(**dw_connection_params)
        dw_cursor = dw_conn.cursor()

        # Define SQL commands to create fact and dimension tables
        create_fact_table_sql = """
        CREATE TABLE IF NOT EXISTS fact_movie_analytics (
            customerid INTEGER,
            id_dim_date INTEGER,
            id_dim_devices INTEGER,
            id_dim_location INTEGER,
            id_dim_os INTEGER,
            id_dim_browser INTEGER,
            amount_spent DECIMAL(18, 5),
            review_score INTEGER,
            review_count INTEGER,
            insert_date DATE
        );
        """
        create_dim_date_table_sql = """
        CREATE TABLE IF NOT EXISTS dim_date (
            id_dim_date INTEGER,
            log_date DATE,
            day VARCHAR,
            month VARCHAR,
            year VARCHAR,
            season VARCHAR
        );
        """
        create_dim_devices_table_sql = """
        CREATE TABLE IF NOT EXISTS dim_devices (
            id_dim_devices INTEGER,
            device VARCHAR
        );
        """
        create_dim_location_table_sql = """
        CREATE TABLE IF NOT EXISTS dim_location (
            id_dim_location INTEGER,
            location VARCHAR
        );
        """
        create_dim_os_table_sql = """
        CREATE TABLE IF NOT EXISTS dim_os (
            id_dim_os INTEGER,
            os VARCHAR
        );
        """
        create_dim_browser_table_sql = """
        CREATE TABLE IF NOT EXISTS dim_browser (
            id_dim_browser INTEGER,
            browser VARCHAR
        );
        """

        # Execute SQL commands to create tables
        dw_cursor.execute(create_fact_table_sql)
        dw_cursor.execute(create_dim_date_table_sql)
        dw_cursor.execute(create_dim_devices_table_sql)
        dw_cursor.execute(create_dim_location_table_sql)
        dw_cursor.execute(create_dim_os_table_sql)
        dw_cursor.execute(create_dim_browser_table_sql)

        # Commit the changes and close the connection
        dw_conn.commit()
        dw_conn.close()

    except Exception as e:
        raise Exception(f"Failed to create Data Warehouse tables: {str(e)}")

# Task to create Data Warehouse tables
create_dw_tables_task = PythonOperator(
    task_id='create_dw_tables',
    python_callable=create_dw_tables,
    dag=dag,
)

# Task to trigger the entire pipeline
extract_transform_task >> extract_from_postgresql_task >> create_dw_tables_task >> load_data_task
