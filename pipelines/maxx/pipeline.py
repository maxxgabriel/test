from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, substring, instr, concat, lit, count, when, expr
)
from pyspark.sql.types import StringType, IntegerType
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_spark_session(app_name="maxx_SpecForge_Analytics"):
    """
    Initialize SparkSession with appropriate configurations
    """
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .getOrCreate()
        
        logger.info(f"SparkSession created successfully: {app_name}")
        return spark
    except Exception as e:
        logger.error(f"Error creating SparkSession: {str(e)}")
        raise


def read_source_users(spark, jdbc_url, connection_properties):
    """
    Source Qualifier: SQ_users
    Reads users table from PostgreSQL database
    """
    try:
        df_users = spark.read \
            .jdbc(
                url=jdbc_url,
                table="public.users",
                properties=connection_properties
            ) \
            .select(
                col("id"),
                col("email")
            )
        
        logger.info(f"Successfully read users source. Record count: {df_users.count()}")
        return df_users
    except Exception as e:
        logger.error(f"Error reading users source: {str(e)}")
        raise


def read_source_projects(spark, jdbc_url, connection_properties):
    """
    Source Qualifier: SQ_projects
    Reads projects table from PostgreSQL database
    """
    try:
        df_projects = spark.read \
            .jdbc(
                url=jdbc_url,
                table="public.projects",
                properties=connection_properties
            ) \
            .select(
                col("id").alias("project_id"),
                col("user_id").alias("p_user_id")
            )
        
        logger.info(f"Successfully read projects source. Record count: {df_projects.count()}")
        return df_projects
    except Exception as e:
        logger.error(f"Error reading projects source: {str(e)}")
        raise


def transform_mask_data(df_users):
    """
    Expression Transformation: EXP_Mask_Data
    Masks PII data (email) by keeping username and replacing domain with @***.com
    Original expression: SUBSTR(email, 1, INSTR(email, '@') - 1) || '@***.com'
    """
    try:
        df_masked = df_users.withColumn(
            "MASKED_EMAIL",
            concat(
                substring(
                    col("email"),
                    1,
                    when(instr(col("email"), lit("@")) > 0, 
                         instr(col("email"), lit("@")) - 1)
                    .otherwise(0)
                ),
                lit("@***.com")
            )
        ).withColumn(
            "user_id", 
            col("id")
        ).select(
            col("user_id"),
            col("MASKED_EMAIL")
        )
        
        logger.info("Successfully applied PII masking transformation")
        return df_masked
    except Exception as e:
        logger.error(f"Error in mask_data transformation: {str(e)}")
        raise


def join_user_projects(df_masked_users, df_projects):
    """
    Joiner Transformation: JNR_User_Projects
    Normal Join (Inner Join) on user_id = p_user_id
    Master: users (masked), Detail: projects
    """
    try:
        df_joined = df_masked_users.join(
            df_projects,
            df_masked_users.user_id == df_projects.p_user_id,
            "inner"
        ).select(
            df_masked_users.user_id,
            df_masked_users.MASKED_EMAIL,
            df_projects.project_id
        )
        
        logger.info(f"Successfully joined users and projects. Record count: {df_joined.count()}")
        return df_joined
    except Exception as e:
        logger.error(f"Error in join_user_projects transformation: {str(e)}")
        raise


def aggregate_project_count(df_joined):
    """
    Aggregator Transformation: AGG_Project_Count
    Groups by user_id and MASKED_EMAIL, counts projects per user
    Expression: COUNT(project_id)
    """
    try:
        df_aggregated = df_joined.groupBy(
            col("user_id"),
            col("MASKED_EMAIL")
        ).agg(
            count(col("project_id")).alias("TOTAL_PROJECTS")
        ).select(
            col("user_id").alias("USER_ID"),
            col("MASKED_EMAIL"),
            col("TOTAL_PROJECTS").cast(IntegerType())
        )
        
        logger.info(f"Successfully aggregated project counts. Record count: {df_aggregated.count()}")
        return df_aggregated
    except Exception as e:
        logger.error(f"Error in aggregate_project_count transformation: {str(e)}")
        raise


def write_target_analytics(df_aggregated, jdbc_url, connection_properties, write_mode="overwrite"):
    """
    Target: USER_PROJECT_ANALYTICS
    Writes aggregated user project analytics to PostgreSQL target table
    """
    try:
        df_aggregated.write \
            .jdbc(
                url=jdbc_url,
                table="public.USER_PROJECT_ANALYTICS",
                mode=write_mode,
                properties=connection_properties
            )
        
        logger.info("Successfully written data to USER_PROJECT_ANALYTICS target")
    except Exception as e:
        logger.error(f"Error writing to target: {str(e)}")
        raise


def run_specforge_analytics_pipeline(
    jdbc_url,
    db_user,
    db_password,
    write_mode="overwrite"
):
    """
    Main ETL pipeline function
    Orchestrates the complete SpecForge Analytics workflow
    Mapping: m_SpecForge_Analytics
    Workflow: wkf_SpecForge_Analytics
    """
    spark = None
    try:
        # Initialize Spark Session
        spark = create_spark_session()
        
        # Database connection properties
        connection_properties = {
            "user": db_user,
            "password": db_password,
            "driver": "org.postgresql.Driver"
        }
        
        logger.info("Starting SpecForge Analytics ETL pipeline")
        
        # Step 1: Read source tables
        logger.info("Step 1: Reading source tables")
        df_users = read_source_users(spark, jdbc_url, connection_properties)
        df_projects = read_source_projects(spark, jdbc_url, connection_properties)
        
        # Step 2: Apply PII masking expression transformation
        logger.info("Step 2: Applying PII masking transformation")
        df_masked_users = transform_mask_data(df_users)
        
        # Step 3: Join users and projects
        logger.info("Step 3: Joining users and projects")
        df_joined = join_user_projects(df_masked_users, df_projects)
        
        # Step 4: Aggregate project counts
        logger.info("Step 4: Aggregating project counts per user")
        df_aggregated = aggregate_project_count(df_joined)
        
        # Step 5: Write to target table
        logger.info("Step 5: Writing to target table")
        write_target_analytics(df_aggregated, jdbc_url, connection_properties, write_mode)
        
        logger.info("SpecForge Analytics ETL pipeline completed successfully")
        
        return True
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession stopped")


def main():
    """
    Entry point for the PySpark job
    """
    try:
        # Configuration parameters (can be passed via command line or config file)
        jdbc_url = "jdbc:postgresql://localhost:5432/SpecForge_DB"
        db_user = "your_db_user"
        db_password = "your_db_password"
        write_mode = "overwrite"
        
        # Run the pipeline
        success = run_specforge_analytics_pipeline(
            jdbc_url=jdbc_url,
            db_user=db_user,
            db_password=db_password,
            write_mode=write_mode
        )
        
        if success:
            logger.info("Job completed successfully")
            sys.exit(0)
        else:
            logger.error("Job failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()