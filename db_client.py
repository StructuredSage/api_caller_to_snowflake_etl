import utils # This ensures that the logging configuration in utils.py is applied
import snowflake.connector

from config import RunSettings

class Snowflake:
    def __init__(self, settings: RunSettings):
        self.settings = settings

    def connect(self):
        try:
            conn = snowflake.connector.connect(
                user=self.settings.snowflake_user,
                password=self.settings.snowflake_password,
                account=self.settings.snowflake_account,
                warehouse=self.settings.snowflake_warehouse,
                database=self.settings.snowflake_database,
                schema=self.settings.snowflake_schema,
                role=self.settings.snowflake_role
                )
            utils.log_info("Successfully connected to Snowflake.")
            return conn
        except Exception as e:
            utils.log_error(f"Error connecting to Snowflake: {e}")
    
    def call_stored_procedure(self,job_name: str):
        conn = self.connect()
        try:
            cursor = conn.cursor()
            cursor.execute(f"CALL SCHEMA.USP_TRANSFORM_DATA('{job_name}')")
            cursor.close()

            utils.log_info("Stored procedure executed successfully.")
        except Exception as e:
            utils.log_error(f"Error executing stored procedure: {e}")
        finally:
            if conn:
                conn.close()