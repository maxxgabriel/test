import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def initialize_spark(app_name="SpecForge_Analytics_ETL"):
    """Initialize Spark session with necessary configurations"""
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        logger.info(f"Spark session initialized: {app_name}")
        return spark
    except Exception as e:
        logger.error(f"Failed to initialize Spark session: {str(e)}")
        raise


def read_source_users(spark, jdbc_url, connection_properties):
    """
    Source Qualifier: SQ_users
    Reads users table from PostgreSQL database
    """
    try:
        logger.info("Reading users source table")
        df_users = spark.read \
            .jdbc(
                url=jdbc_url,
                table="public.users",
                properties=connection_properties
            ) \
            .select(
                F.col("id"),
                F.col("email")
            )
        
        logger.info(f"Users source loaded: {df_users.count()} records")
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
        logger.info("Reading projects source table")
        df_projects = spark.read \
            .jdbc(
                url=jdbc_url,
                table="public.projects",
                properties=connection_properties
            ) \
            .select(
                F.col("id").alias("project_id"),
                F.col("user_id").alias("p_user_id")
            )
        
        logger.info(f"Projects source loaded: {df_projects.count()} records")
        return df_projects
    except Exception as e:
        logger.error(f"Error reading projects source: {str(e)}")
        raise


def apply_expression_mask_data(df_users):
    """
    Expression Transformation: EXP_Mask_Data
    Masks PII data by transforming email addresses
    Expression: SUBSTR(email, 1, INSTR(email, '@') - 1) || '@***.com'
    """
    try:
        logger.info("Applying expression transformation: EXP_Mask_Data")
        
        df_masked = df_users \
            .withColumn("user_id", F.col("id")) \
            .withColumn(
                "MASKED_EMAIL",
                F.concat(
                    F.substring(
                        F.col("email"),
                        1,
                        F.expr("INSTR(email, '@') - 1")
                    ),
                    F.lit("@***.com")
                )
            ) \
            .select(
                "user_id",
                "MASKED_EMAIL"
            )
        
        logger.info("Expression transformation completed")
        return df_masked
    except Exception as e:
        logger.error(f"Error in expression transformation: {str(e)}")
        raise


def apply_joiner_user_projects(df_users_masked, df_projects):
    """
    Joiner Transformation: JNR_User_Projects
    Join Type: Normal Join (Inner Join)
    Join Condition: user_id = p_user_id
    Master: df_users_masked (user_id, MASKED_EMAIL)
    Detail: df_projects (project_id, p_user_id)
    """
    try:
        logger.info("Applying joiner transformation: JNR_User_Projects")
        
        df_joined = df_users_masked.join(
            df_projects,
            df_users_masked["user_id"] == df_projects["p_user_id"],
            "inner"
        ).select(
            df_users_masked["user_id"],
            df_users_masked["MASKED_EMAIL"],
            df_projects["project_id"]
        )
        
        logger.info(f"Joiner transformation completed: {df_joined.count()} records")
        return df_joined
    except Exception as e:
        logger.error(f"Error in joiner transformation: {str(e)}")
        raise


def apply_aggregator_project_count(df_joined):
    """
    Aggregator Transformation: AGG_Project_Count
    Group By: user_id, MASKED_EMAIL
    Aggregate: COUNT(project_id) as TOTAL_PROJECTS
    """
    try:
        logger.info("Applying aggregator transformation: AGG_Project_Count")
        
        df_aggregated = df_joined \
            .groupBy(
                "user_id",
                "MASKED_EMAIL"
            ) \
            .agg(
                F.count("project_id").alias("TOTAL_PROJECTS")
            ) \
            .select(
                F.col("user_id").alias("USER_ID"),
                F.col("MASKED_EMAIL"),
                F.col("TOTAL_PROJECTS").cast(IntegerType())
            )
        
        logger.info(f"Aggregator transformation completed: {df_aggregated.count()} records")
        return df_aggregated
    except Exception as e:
        logger.error(f"Error in aggregator transformation: {str(e)}")
        raise


def write_target_user_project_analytics(df_final, jdbc_url, connection_properties):
    """
    Target: USER_PROJECT_ANALYTICS
    Writes the final aggregated data to PostgreSQL target table
    """
    try:
        logger.info("Writing to target table: USER_PROJECT_ANALYTICS")
        
        df_final.write \
            .jdbc(
                url=jdbc_url,
                table="public.USER_PROJECT_ANALYTICS",
                mode="overwrite",
                properties=connection_properties
            )
        
        logger.info("Target write completed successfully")
    except Exception as e:
        logger.error(f"Error writing to target: {str(e)}")
        raise


def run_specforge_analytics_etl(jdbc_url, db_user, db_password):
    """
    Main ETL pipeline: m_SpecForge_Analytics
    Workflow: wkf_SpecForge_Analytics
    
    Pipeline flow:
    1. Read users and projects sources
    2. Mask PII data in users
    3. Join users with projects
    4. Aggregate project counts per user
    5. Write to target analytics table
    """
    spark = None
    try:
        spark = initialize_spark("wkf_SpecForge_Analytics")
        
        connection_properties = {
            "user": db_user,
            "password": db_password,
            "driver": "org.postgresql.Driver"
        }
        
        logger.info("Starting SpecForge Analytics ETL pipeline")
        
        df_users = read_source_users(spark, jdbc_url, connection_properties)
        
        df_projects = read_source_projects(spark, jdbc_url, connection_properties)
        
        df_masked = apply_expression_mask_data(df_users)
        
        df_joined = apply_joiner_user_projects(df_masked, df_projects)
        
        df_aggregated = apply_aggregator_project_count(df_joined)
        
        write_target_user_project_analytics(df_aggregated, jdbc_url, connection_properties)
        
        logger.info("SpecForge Analytics ETL pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python specforge_analytics_etl.py <jdbc_url> <db_user> <db_password>")
        print("Example: python specforge_analytics_etl.py jdbc:postgresql://localhost:5432/SpecForge_DB admin password123")
        sys.exit(1)
    
    jdbc_url = sys.argv[1]
    db_user = sys.argv[2]
    db_password = sys.argv[3]
    
    run_specforge_analytics_etl(jdbc_url, db_user, db_password)