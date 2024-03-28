import utils # This ensures that the logging configuration in utils.py is applied
import os
from enum import Enum

class RunSettings:

    @staticmethod
    def _parse_env_var(env_var_value):
        return dict(item.split("=") for item in env_var_value.split(";") if item)
    
    def __init__(self):
        self.snowflake_config = self._parse_env_var(os.environ.get('SNOWFLAKE_CONNECTION',''))
        self.clientapi_config = self._parse_env_var(os.environ.get('CLIENTAPI_CONNECTION',''))

    @property
    def clientapi_client_id(self):
        return self.clientapi_config.get('client_id')
    
    @property
    def clientapi_client_secret(self):
        return self.clientapi_config.get('client_secret')
    
    @property
    def clientapi_application_name(self):
        return self.clientapi_config.get('application_name')
    
    @property
    def snowflake_user(self):
        return self.snowflake_config.get('user')
    
    @property
    def snowflake_password(self):
        return self.snowflake_config.get('password')
    
    @property
    def snowflake_account(self):
        return self.snowflake_config.get('account')
    
    @property
    def snowflake_warehouse(self):
        return self.snowflake_config.get('warehouse')
    
    @property
    def snowflake_database(self):
        return self.snowflake_config.get('database')
    
    @property
    def snowflake_schema(self):
        return self.snowflake_config.get('schema')
    
    @property
    def snowflake_role(self):
        return self.snowflake_config.get('role')
    
class UATSettings(RunSettings):
    base_url: str = "https://uat.clientapi.com/apiv2/"


class PRODSettings(RunSettings):
    base_url: str = "https://clientapi.com/apiv2/"


class LocalSettings(RunSettings):
    base_url: str = "http://localhost:8000/apiv2/"


class Environment(str, Enum):
    PROD = "prod"
    UAT = "uat"
    LOCAL = "local"

class Mode(str, Enum):
    JOB = 'job'
    repository = 'repository'
    SNOWFLAKE = 'snowflake'
    FULLJOB = 'fulljob'
    FULLrepository = 'fullrepository'

def settings_factory(env: Environment):
    try:
        if env == Environment.PROD:
            cls = PRODSettings
        elif env == Environment.UAT:
            cls = UATSettings
        elif env == Environment.LOCAL:
            cls = LocalSettings
        else:
            utils.log_error(f"No environment of type {env}")
        return cls()
    except Exception as e:
        utils.log_error(f"Error loading settings from file: {e}")