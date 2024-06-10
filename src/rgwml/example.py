import json
import os
import pandas as pd
import pyodbc
import pymysql
from clickhouse_driver import Client
from google.cloud import bigquery

config_path = os.path.expanduser("~/Desktop/rgwml.config")

class DBPreset:
    def __init__(self, name, db_type, host, username, password, database):
        self.name = name
        self.db_type = db_type
        self.host = host
        self.username = username
        self.password = password
        self.database = database


class Config:
    def __init__(self, config_path):
        with open(config_path, "r") as file:
            config_data = json.load(file)
        self.db_presets = [DBPreset(**preset) for preset in config_data["db_presets"]]
        self.google_big_query_presets = config_data.get("google_big_query_presets", [])
        self.open_ai_key = config_data.get("open_ai_key", "")

    def get_preset(self, name):
        for preset in self.db_presets:
            if preset.name == name:
                return preset
        raise ValueError(f"No preset found with name {name}")


class DatabaseManager:
    def __init__(self, preset):
        self.preset = preset
        self.connection = None
        self.client = None

    def connect(self):
        if self.preset.db_type == "mssql":
            self.connection = pyodbc.connect(
                f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.preset.host};'
                f'DATABASE={self.preset.database};UID={self.preset.username};PWD={self.preset.password}'
            )
        elif self.preset.db_type == "mysql":
            self.connection = pymysql.connect(
                host=self.preset.host,
                user=self.preset.username,
                password=self.preset.password,
                database=self.preset.database
            )
        elif self.preset.db_type == "clickhouse":
            self.client = Client(
                host=self.preset.host,
                user=self.preset.username,
                password=self.preset.password,
                database=self.preset.database
            )
        else:
            raise ValueError(f"Unsupported database type: {self.preset.db_type}")
        return self

    def execute_query(self, query):
        if self.preset.db_type in ["mssql", "mysql"]:
            df = pd.read_sql(query, self.connection)
            self.connection.close()
        elif self.preset.db_type == "clickhouse":
            df = pd.DataFrame(self.client.execute(query))
        else:
            raise ValueError(f"Unsupported database type: {self.preset.db_type}")
        return df


class BigQueryManager:
    def __init__(self, preset):
        self.client = bigquery.Client.from_service_account_json(preset["json_file_path"])

    def execute_query(self, query, project_id):
        query_job = self.client.query(query, project=project_id)
        results = query_job.result()
        return results.to_dataframe()


class DataManager:
    def __init__(self, config):
        self.config = Config(config)

    def mssql_to_df(self, preset_name, query_str):
        preset = self.config.get_preset(preset_name)
        db_manager = DatabaseManager(preset).connect()
        return db_manager.execute_query(query_str)

    def bigquery_to_df(self, preset_name, query_str):
        preset = next((p for p in self.config.google_big_query_presets if p["name"] == preset_name), None)
        if not preset:
            raise ValueError(f"No preset found with name {preset_name}")
        bq_manager = BigQueryManager(preset)
        return bq_manager.execute_query(query_str, preset["project_id"])



