import pandas as pd
import numpy as np
import pymssql
import os
import glob
import json
from datetime import datetime
import collections
import time
import argparse
import copy
import gc
import mysql.connector
import tempfile
from clickhouse_driver import Client as ClickHouseClient
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
import xgboost as xgb
import matplotlib.pyplot as plt
from PIL import Image
import scipy.stats as stats
import seaborn as sns
from sklearn.cluster import KMeans, DBSCAN, AgglomerativeClustering, MeanShift, SpectralClustering, Birch
from sklearn.mixture import GaussianMixture
from sklearn.metrics import silhouette_score
import dask.dataframe as dd
from openai import OpenAI
from pprint import pprint

class p:
    def __init__(self, df=None):
        """Initialize the EP class with an empty DataFrame."""
        self.df = df

    def cl(self):
        """UTILS::[d.clo()] Clone."""
        gc.collect()
        return p(df=copy.deepcopy(self.df))

    def gdf(self):
        """UTILS::[d.gdf()] Get DataFrame."""
        return self.df


    def frd(self, headers, data):
        """LOAD::[d.frd(['col1','col2'],[[1,2,3],[4,5,6]])] From raw data."""
        if isinstance(data, list) and all(isinstance(row, list) for row in data):
            self.df = pd.DataFrame(data, columns=headers)
        else:
            raise ValueError("Data should be an array of arrays.")
        return self

    def fq(self, db_preset_name, query):
        """LOAD::[d.fq('preset_name','SELECT * FROM your_table')] From query."""

        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [
                os.path.join(home_dir, "Desktop"),
                os.path.join(home_dir, "Documents"),
                os.path.join(home_dir, "Downloads"),
            ]

            for path in search_paths:
                for root, dirs, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")


        # Read the rgwml.config file from the Desktop
        config_path = locate_config_file()
        #config_path = os.path.expanduser("~/Desktop/rgwml.config")
        with open(config_path, 'r') as f:
            config = json.load(f)

        # Find the matching db_preset
        db_presets = config.get('db_presets', [])
        db_preset = next((preset for preset in db_presets if preset['name'] == db_preset_name), None)
        if not db_preset:
            raise ValueError(f"No matching db_preset found for {db_preset_name}")

        db_type = db_preset['db_type']

        if db_type == 'mssql':
            host = db_preset['host']
            username = db_preset['username']
            password = db_preset['password']
            database = db_preset.get('database', '')

            with pymssql.connect(server=host, user=username, password=password, database=database) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
        elif db_type == 'mysql':
            host = db_preset['host']
            username = db_preset['username']
            password = db_preset['password']
            database = db_preset.get('database', '')

            with mysql.connector.connect(host=host, user=username, password=password, database=database) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    if cursor.description is not None:
                        rows = cursor.fetchall()
                        columns = [desc[0] for desc in cursor.description]
        elif db_type == 'clickhouse':
            host = db_preset['host']
            username = db_preset['username']
            password = db_preset['password']
            database = db_preset.get('database', '')

            client = ClickHouseClient(host=host, user=username, password=password, database=database)
            rows = client.execute(query)
            columns_query = f"DESCRIBE TABLE {query.split('FROM')[1].strip()}"
            columns = [row[0] for row in client.execute(columns_query)]
        elif db_type == 'google_big_query':
            json_file_path = db_preset['json_file_path']
            project_id = db_preset['project_id']

            bigquery_presets = config.get('google_big_query_presets', [])
            if not db_preset:
                raise ValueError(f"No matching Google BigQuery preset found for {db_preset_name}")

            credentials = service_account.Credentials.from_service_account_file(json_file_path)
            client = bigquery.Client(credentials=credentials, project=project_id)

            # Run the query and manually fetch the results
            query_job = client.query(query)
            results = query_job.result()

            # Extract rows and columns
            rows = [list(row.values()) for row in results]
            columns = results.schema

            # Convert to DataFrame
            df = pd.DataFrame(rows, columns=[field.name for field in columns])
            self.df = df
            self.pr()
            gc.collect()
            return self
        else:
            raise ValueError(f"Unsupported db_type: {db_type}")

        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=columns)
        self.df = df
        self.pr()
        gc.collect()
        return self

    def dbq(self, preset_name, db_or_dataset_name, query):
        """DATABASE::[d.dbq('preset_name', 'db_or_dataset_name', 'YOUR QUERY')] Execute a query on the specified database and print the results if it returns data."""
        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [os.path.join(home_dir, folder) for folder in ["Desktop", "Documents", "Downloads"]]
            for path in search_paths:
                for root, _, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        config_path = locate_config_file()
        with open(config_path, 'r') as f:
            config = json.load(f)

        db_presets = config.get('db_presets', [])
        db_preset = next((preset for preset in db_presets if preset['name'] == preset_name), None)

        if not db_preset:
            raise ValueError(f"No matching db_preset found for {preset_name}")

        # Override the database name
        db_preset['database'] = db_or_dataset_name

        db_type = db_preset['db_type']
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None) 

        if db_type == 'mssql':
            host = db_preset['host']
            username = db_preset['username']
            password = db_preset['password']
            database = db_preset.get('database', '')

            with pymssql.connect(server=host, user=username, password=password, database=database) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    if cursor.description:
                        rows = cursor.fetchall()
                        columns = [desc[0] for desc in cursor.description]
                        df = pd.DataFrame(rows, columns=columns)
                        print(df)
                    else:
                        conn.commit()

        elif db_type == 'mysql':
            host = db_preset['host']
            username = db_preset['username']
            password = db_preset['password']
            database = db_preset.get('database', '')

            with mysql.connector.connect(host=host, user=username, password=password, database=database) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    if cursor.description:
                        rows = cursor.fetchall()
                        columns = [desc[0] for desc in cursor.description]
                        df = pd.DataFrame(rows, columns=columns)
                        print(df)
                    else:
                        conn.commit()
                    # Fetch all remaining results to clear the cursor
                    while cursor.nextset():
                        pass

        elif db_type == 'clickhouse':
            host = db_preset['host']
            username = db_preset['username']
            password = db_preset['password']
            database = db_preset.get('database', '')

            client = ClickHouseClient(host=host, user=username, password=password, database=database)
            rows = client.execute(query)
            if rows:
                columns_query = f"DESCRIBE TABLE {query.split('FROM')[1].strip()}"
                columns = [row[0] for row in client.execute(columns_query)]
                df = pd.DataFrame(rows, columns=columns)
                print(df)

        elif db_type == 'google_big_query':
            json_file_path = db_preset['json_file_path']
            project_id = db_preset['project_id']

            credentials = service_account.Credentials.from_service_account_file(json_file_path)
            client = bigquery.Client(credentials=credentials, project=project_id)

            query_job = client.query(query)
            result = query_job.result()
            if query.strip().lower().startswith("select"):
                df = result.to_dataframe()
                print(df)
            else:
                client.query(query).result()  # Executes the query without returning a DataFrame

        else:
            raise ValueError(f"Unsupported db_type: {db_type}")

        pd.reset_option('all')
        gc.collect()
        return self

    def dbct(self, db_preset_name, db_name, table_name, columns_str, print_query=False):
        """DATABASE::[d.dbct('preset_name', 'db_name', 'your_table', 'Column1, Column2, Column3[VARCHAR(1000)]', print_query=False)] Create table."""
        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [
                os.path.join(home_dir, "Desktop"),
                os.path.join(home_dir, "Documents"),
                os.path.join(home_dir, "Downloads"),
            ]

            for path in search_paths:
                for root, dirs, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        # Read the rgwml.config file from the Desktop
        config_path = locate_config_file()
        with open(config_path, 'r') as f:
            config = json.load(f)

        # Find the matching db_preset
        db_presets = config.get('db_presets', [])
        db_preset = next((preset for preset in db_presets if preset['name'] == db_preset_name), None)
        if not db_preset:
            raise ValueError(f"No matching db_preset found for {db_preset_name}")

        db_type = db_preset['db_type']
        if db_type != 'mysql':
            raise ValueError(f"Unsupported db_type for this method: {db_type}")

        host = db_preset['host']
        username = db_preset['username']
        password = db_preset['password']

        # Process columns
        columns = columns_str.split(',')
        columns = [col.strip() for col in columns]
        processed_columns = []
        for col in columns:
            if '[' in col and ']' in col:
                col_name, col_type = col.split('[')
                col_type = col_type.strip(']')
                processed_columns.append(f"`{col_name.strip()}` {col_type}")
            else:
                processed_columns.append(f"`{col}` VARCHAR(255)")

        # Add id, created_at, and updated_at columns
        processed_columns.insert(0, "`id` INT AUTO_INCREMENT PRIMARY KEY")
        processed_columns.append("`created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        processed_columns.append("`updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")

        create_query = f"CREATE TABLE `{table_name}` ({', '.join(processed_columns)})"

        # Connect to the database
        with mysql.connector.connect(host=host, user=username, password=password, database=db_name) as conn:
            cursor = conn.cursor()

            try:
                if print_query:
                    print(create_query)
                cursor.execute(create_query)
                conn.commit()

            finally:
                cursor.close()
                gc.collect()

        return self

    def dbrct(self, db_preset_name, db_name, table_name, columns_str, print_query=False):
        """DATABASE::[d.dbrct('preset_name', 'db_name', 'your_table', 'Column1, Column2, Column3[VARCHAR(1000)]', print_query=False)] Recreate table. Deletes existing table and recreates it."""
        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [
                os.path.join(home_dir, "Desktop"),
                os.path.join(home_dir, "Documents"),
                os.path.join(home_dir, "Downloads"),
            ]

            for path in search_paths:
                for root, dirs, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        # Read the rgwml.config file from the Desktop
        config_path = locate_config_file()
        with open(config_path, 'r') as f:
            config = json.load(f)

        # Find the matching db_preset
        db_presets = config.get('db_presets', [])
        db_preset = next((preset for preset in db_presets if preset['name'] == db_preset_name), None)
        if not db_preset:
            raise ValueError(f"No matching db_preset found for {db_preset_name}")

        db_type = db_preset['db_type']
        if db_type != 'mysql':
            raise ValueError(f"Unsupported db_type for this method: {db_type}")

        host = db_preset['host']
        username = db_preset['username']
        password = db_preset['password']

        # Process columns
        columns = columns_str.split(',')
        columns = [col.strip() for col in columns]
        processed_columns = []
        for col in columns:
            if '[' in col and ']' in col:
                col_name, col_type = col.split('[')
                col_type = col_type.strip(']')
                processed_columns.append(f"`{col_name.strip()}` {col_type}")
            else:
                processed_columns.append(f"`{col}` VARCHAR(255)")

        # Add id, created_at, and updated_at columns
        processed_columns.insert(0, "`id` INT AUTO_INCREMENT PRIMARY KEY")
        processed_columns.append("`created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        processed_columns.append("`updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")

        create_query = f"CREATE TABLE `{table_name}` ({', '.join(processed_columns)})"

        # Connect to the database
        with mysql.connector.connect(host=host, user=username, password=password, database=db_name) as conn:
            cursor = conn.cursor()

            try:
                # Drop the existing table
                drop_query = f"DROP TABLE IF EXISTS `{table_name}`"
                if print_query:
                    print(drop_query)
                cursor.execute(drop_query)
                conn.commit()

                # Create the table
                if print_query:
                    print(create_query)
                cursor.execute(create_query)
                conn.commit()

            finally:
                cursor.close()
                gc.collect()

        return self

    def dbh(self, db_type):
        """DATABASE::[d.dbh('mysql')] Database help. Prints the syntax to show all tables, describe tables, show column data types, and select the most recent 100 records of any table."""
        if db_type == 'mssql':
            help_text = """
-- Show all databases
SELECT name FROM sys.databases;

-- Show all tables
SELECT * FROM INFORMATION_SCHEMA.TABLES;

-- Describe a table
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'your_table_name';

-- Show column data types
SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'your_table_name';

-- Select the most recent 100 records
SELECT TOP 100 * FROM your_table_name ORDER BY your_date_column DESC;
            """
        elif db_type == 'mysql':
            help_text = """
-- DATABASE COMMANDS

    -- Show all databases
    SHOW DATABASES;

    -- Create a new database
    CREATE DATABASE new_database_name;

    -- Delete a database
    DROP DATABASE database_name;

    -- Rename a database (MySQL does not support renaming databases directly. This workaround can be used)
    CREATE DATABASE new_database_name; 
    RENAME TABLE old_database_name.table1 TO new_database_name.table1; 
    RENAME TABLE old_database_name.table2 TO new_database_name.table2; 
    DROP DATABASE old_database_name;

-- TABLE COMMANDS

    -- Show all tables
    SHOW TABLES;

    -- Describe a table
    DESCRIBE your_table_name;

    -- Show column data types
    SELECT COLUMN_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'your_table_name';

    -- Select the most recent 100 records
    SELECT * FROM your_table_name ORDER BY your_date_column DESC LIMIT 100;

    -- Rename a table
    ALTER TABLE old_table_name RENAME TO new_table_name;

    -- Remove a column from a table
    ALTER TABLE your_table_name DROP COLUMN column_name;

    -- Change the order of columns (requires recreating the table)
    CREATE TABLE new_table_name AS SELECT column1, column2, column3 FROM old_table_name;

    -- Change the data type of a column
    ALTER TABLE your_table_name MODIFY COLUMN column_name new_data_type;

    -- Create a new table
    CREATE TABLE new_table_name (id INT AUTO_INCREMENT PRIMARY KEY, column1 VARCHAR(255), column2 VARCHAR(255), column3 VARCHAR(255), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);

    -- Update a record in a table
    UPDATE your_table_name SET column1 = value1, column2 = value2 WHERE condition;

    -- Delete a record from a table
    DELETE FROM your_table_name WHERE condition;

    -- Delete a table
    DROP TABLE your_table_name;
            """
        elif db_type == 'clickhouse':
            help_text = """
-- Show all databases
SHOW DATABASES;

-- Show all tables
SHOW TABLES;

-- Describe a table
DESCRIBE TABLE your_table_name;

-- Show column data types
SELECT name, type FROM system.columns WHERE table = 'your_table_name';

-- Select the most recent 100 records
SELECT * FROM your_table_name ORDER BY your_date_column DESC LIMIT 100;
            """
        elif db_type == 'google_big_query':
            help_text = """
-- Show all datasets
SHOW SCHEMAS;

-- Show all tables
SELECT * FROM `project_id.dataset_id.INFORMATION_SCHEMA.TABLES`;

-- Describe a table
SELECT column_name, data_type, is_nullable FROM `project_id.dataset_id.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'your_table_name';

-- Show column data types
SELECT column_name, data_type FROM `project_id.dataset_id.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = 'your_table_name';

-- Select the most recent 100 records
SELECT * FROM `project_id.dataset_id.your_table_name` ORDER BY your_date_column DESC LIMIT 100;
            """
        else:
            raise ValueError(f"Unsupported db_type: {db_type}")

        print(help_text)
        return self

    def dbi(self, db_preset_name, db_name, table_name, insert_columns=None, print_query=False):
        """DATABASE::[d.dbi('preset_name', 'db_name', 'your_table', insert_columns=['Column7', 'Column9', 'Column3']] Simply insert of all rows in the DataFrame into the specified table."""

        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [
                os.path.join(home_dir, "Desktop"),
                os.path.join(home_dir, "Documents"),
                os.path.join(home_dir, "Downloads"),
            ]

            for path in search_paths:
                for root, dirs, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        # Read the rgwml.config file from the Desktop
        config_path = locate_config_file()
        with open(config_path, 'r') as f:
            config = json.load(f)

        # Find the matching db_preset
        db_presets = config.get('db_presets', [])
        db_preset = next((preset for preset in db_presets if preset['name'] == db_preset_name), None)
        if not db_preset:
            raise ValueError(f"No matching db_preset found for {db_preset_name}")

        db_type = db_preset['db_type']
        if db_type != 'mysql':
            raise ValueError(f"Unsupported db_type for this method: {db_type}")

        host = db_preset['host']
        username = db_preset['username']
        password = db_preset['password']

        # Determine columns to insert
        if insert_columns is None:
            insert_columns = self.df.columns.tolist()
        else:
            insert_columns = [col.strip() for col in insert_columns]

        # Connect to the database
        with mysql.connector.connect(host=host, user=username, password=password, database=db_name) as conn:
            cursor = conn.cursor()

            try:
                # Insert data into the table
                cols = ", ".join(insert_columns)
                insert_query = f"INSERT INTO {table_name} ({cols}) VALUES ({', '.join(['%s'] * len(insert_columns))})"
                if print_query:
                    print(insert_query)
                data = self.df[insert_columns].values.tolist()
                cursor.executemany(insert_query, data)
                conn.commit()

            finally:
                cursor.close()
                gc.collect()

        return self


    def dbiu(self, db_preset_name, db_name, table_name, unique_columns, insert_columns=None, print_query=False):
        """DATABASE::[d.dbiu('preset_name', 'db_name', 'your_table', unique_columns=['Column1', 'Column2'], insert_columns=['Column7', 'Column9', 'Column3'], print_query=False)] Insert only unique rows based on specified unique_columns."""

        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [
                os.path.join(home_dir, "Desktop"),
                os.path.join(home_dir, "Documents"),
                os.path.join(home_dir, "Downloads"),
            ]

            for path in search_paths:
                for root, dirs, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        # Read the rgwml.config file from the Desktop
        config_path = locate_config_file()
        with open(config_path, 'r') as f:
            config = json.load(f)

        # Find the matching db_preset
        db_presets = config.get('db_presets', [])
        db_preset = next((preset for preset in db_presets if preset['name'] == db_preset_name), None)
        if not db_preset:
            raise ValueError(f"No matching db_preset found for {db_preset_name}")

        db_type = db_preset['db_type']
        if db_type != 'mysql':
            raise ValueError(f"Unsupported db_type for this method: {db_type}")

        host = db_preset['host']
        username = db_preset['username']
        password = db_preset['password']

        # Determine columns to insert
        if insert_columns is None:
            insert_columns = self.df.columns.tolist()
        else:
            insert_columns = [col.strip() for col in insert_columns]

        # Connect to the database
        with mysql.connector.connect(host=host, user=username, password=password, database=db_name) as conn:
            cursor = conn.cursor()

            try:
                # Fetch existing data from the table
                select_query = f"SELECT {', '.join(unique_columns)} FROM {table_name}"
                if print_query:
                    print(select_query)
                cursor.execute(select_query)
                existing_data = cursor.fetchall()
                existing_df = pd.DataFrame(existing_data, columns=unique_columns)

                # Ensure column types match for merging
                for col in unique_columns:
                    self.df[col] = self.df[col].astype(str)
                    existing_df[col] = existing_df[col].astype(str)

                # Determine the unique rows to insert
                new_data_df = self.df[insert_columns]
                merged_df = pd.merge(new_data_df, existing_df, on=unique_columns, how='left', indicator=True)
                unique_new_data_df = merged_df[merged_df['_merge'] == 'left_only'][insert_columns]

                # Handle NaN values in the unique new data
                unique_new_data_df = unique_new_data_df.fillna('')

                # Insert unique new data into the table
                if not unique_new_data_df.empty:
                    cols = ", ".join(insert_columns)
                    insert_query = f"INSERT INTO {table_name} ({cols}) VALUES ({', '.join(['%s'] * len(insert_columns))})"
                    if print_query:
                        print(insert_query)
                    data = unique_new_data_df.values.tolist()
                    cursor.executemany(insert_query, data)
                    conn.commit()

            finally:
                cursor.close()
                gc.collect()

        return self

    def dbtai(self, db_preset_name, db_name, table_name, insert_columns=None, print_query=False):
        """DATABASE::[d.dbtai('preset_name', 'db_name', 'your_table', insert_columns=['Column7', 'Column9', 'Column3'], print_query=False)] Truncate and insert. Truncates the table and inserts the DataFrame."""

        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [
                os.path.join(home_dir, "Desktop"),
                os.path.join(home_dir, "Documents"),
                os.path.join(home_dir, "Downloads"),
            ]

            for path in search_paths:
                for root, dirs, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        # Read the rgwml.config file from the Desktop
        config_path = locate_config_file()
        with open(config_path, 'r') as f:
            config = json.load(f)

        # Find the matching db_preset
        db_presets = config.get('db_presets', [])
        db_preset = next((preset for preset in db_presets if preset['name'] == db_preset_name), None)
        if not db_preset:
            raise ValueError(f"No matching db_preset found for {db_preset_name}")

        db_type = db_preset['db_type']
        if db_type != 'mysql':
            raise ValueError(f"Unsupported db_type for this method: {db_type}")

        host = db_preset['host']
        username = db_preset['username']
        password = db_preset['password']

        # Determine columns to insert
        if insert_columns is None:
            insert_columns = self.df.columns.tolist()
        else:
            insert_columns = [col.strip() for col in insert_columns]

        # Handle NaN values by replacing them with None
        data_to_insert = self.df[insert_columns].replace({np.nan: None}).values.tolist()

        # Connect to the database
        with mysql.connector.connect(host=host, user=username, password=password, database=db_name) as conn:
            cursor = conn.cursor()

            try:
                # Truncate the table
                truncate_query = f"TRUNCATE TABLE {table_name}"
                if print_query:
                    print(truncate_query)
                cursor.execute(truncate_query)
                conn.commit()

                # Insert data into the table
                cols = ", ".join(insert_columns)
                insert_query = f"INSERT INTO {table_name} ({cols}) VALUES ({', '.join(['%s'] * len(insert_columns))})"
                if print_query:
                    print(insert_query)
                cursor.executemany(insert_query, data_to_insert)
                conn.commit()

            finally:
                cursor.close()
                gc.collect()

        return self

    def dbuoi(self, db_preset_name, db_name, table_name, update_where_columns, update_at_column_names, print_query=False):
        """DATABASE::[d.dbuoi('preset_name', 'db_name', 'your_table', ['where_column1', 'where_column2'], ['update_column1', 'update_column2'], print_query=False)] Update or insert. Updates rows based on columns, inserts if no update occurs."""

        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [
                os.path.join(home_dir, "Desktop"),
                os.path.join(home_dir, "Documents"),
                os.path.join(home_dir, "Downloads"),
            ]

            for path in search_paths:
                for root, dirs, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        # Read the rgwml.config file from the Desktop
        config_path = locate_config_file()
        with open(config_path, 'r') as f:
            config = json.load(f)

        # Find the matching db_preset
        db_presets = config.get('db_presets', [])
        db_preset = next((preset for preset in db_presets if preset['name'] == db_preset_name), None)
        if not db_preset:
            raise ValueError(f"No matching db_preset found for {db_preset_name}")

        db_type = db_preset['db_type']
        if db_type != 'mysql':
            raise ValueError(f"Unsupported db_type for this method: {db_type}")

        host = db_preset['host']
        username = db_preset['username']
        password = db_preset['password']

        # Connect to the database
        with mysql.connector.connect(host=host, user=username, password=password, database=db_name) as conn:
            cursor = conn.cursor()

            try:
                # Fetch existing data from the table
                select_query = f"SELECT * FROM {table_name}"
                if print_query:
                    print(select_query)
                cursor.execute(select_query)
                existing_data = cursor.fetchall()
                existing_columns = [desc[0] for desc in cursor.description]
                existing_df = pd.DataFrame(existing_data, columns=existing_columns)

                # Ensure column types match for merging
                for col in update_where_columns:
                    self.df[col] = self.df[col].astype(str)
                    existing_df[col] = existing_df[col].astype(str)

                # Merge new data with existing data
                merge_df = pd.merge(self.df, existing_df, on=update_where_columns, how='left', suffixes=('', '_existing'), indicator=True)

                # Determine rows to update and insert
                rows_to_update = merge_df[merge_df['_merge'] == 'both']
                rows_to_insert = merge_df[merge_df['_merge'] == 'left_only']

                #print("Rows to update:", rows_to_update)
                #print("Rows to insert:", rows_to_insert)

                # Prepare update queries
                for _, row in rows_to_update.iterrows():
                    set_clause = ", ".join([f"{col} = %s" for col in update_at_column_names])
                    where_clause = " AND ".join([f"{col} = %s" for col in update_where_columns])
                    update_query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
                    update_values = [row[f"{col}"] for col in update_at_column_names] + [row[col] for col in update_where_columns]
                    if print_query:
                        print(update_query, update_values)
                    cursor.execute(update_query, update_values)

                # Prepare insert queries
                if not rows_to_insert.empty:
                    cols = ", ".join(self.df.columns)
                    insert_query = f"INSERT INTO {table_name} ({cols}) VALUES ({', '.join(['%s'] * len(self.df.columns))})"
                    if print_query:
                        print(insert_query)
                    insert_values = rows_to_insert[self.df.columns].values.tolist()
                    cursor.executemany(insert_query, insert_values)

                conn.commit()

            finally:
                cursor.close()
                gc.collect()

        return self

    def fcq(self, db_preset_name, query, chunk_size):
        """LOAD::[d.fcq('preset_name', 'SELECT * FROM your_table', chunk_size)] From chunkable query."""

        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [
                os.path.join(home_dir, "Desktop"),
                os.path.join(home_dir, "Documents"),
                os.path.join(home_dir, "Downloads"),
            ]
                    
            for path in search_paths:
                for root, dirs, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        config_path = locate_config_file()
        with open(config_path, 'r') as f:
            config = json.load(f)

        db_presets = config.get('db_presets', [])
        db_preset = next((preset for preset in db_presets if preset['name'] == db_preset_name), None)
        if not db_preset:
            raise ValueError(f"No matching db_preset found for {db_preset_name}")

        db_type = db_preset['db_type']
        total_rows = 0
        temp_files = []

        if db_type == 'mssql':
            host = db_preset['host']
            username = db_preset['username']
            password = db_preset['password']
            database = db_preset.get('database', '')

            with pymssql.connect(server=host, user=username, password=password, database=database) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) FROM ({query}) AS total_query")
                    total_rows = cursor.fetchone()[0]

                    offset = 0
                    while offset < total_rows:
                        chunk_query = f"{query} ORDER BY (SELECT NULL) OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY"
                        cursor.execute(chunk_query)
                        rows = cursor.fetchall()
                        columns = [desc[0] for desc in cursor.description]

                        df_chunk = pd.DataFrame(rows, columns=columns)
                        print(df_chunk)
                        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
                        df_chunk.to_csv(temp_file.name, index=False)
                        temp_files.append(temp_file.name)

                        offset += chunk_size

        elif db_type == 'mysql':
            host = db_preset['host']
            username = db_preset['username']
            password = db_preset['password']
            database = db_preset.get('database', '')

            with mysql.connector.connect(host=host, user=username, password=password, database=database) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) FROM ({query}) AS total_query")
                    total_rows = cursor.fetchone()[0]

                    offset = 0
                    while offset < total_rows:
                        chunk_query = f"{query} LIMIT {chunk_size} OFFSET {offset}"
                        cursor.execute(chunk_query)
                        rows = cursor.fetchall()
                        columns = [desc[0] for desc in cursor.description]

                        df_chunk = pd.DataFrame(rows, columns=columns)
                        print(df_chunk)
                        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
                        df_chunk.to_csv(temp_file.name, index=False)
                        temp_files.append(temp_file.name)

                        offset += chunk_size

        elif db_type == 'clickhouse':
            host = db_preset['host']
            username = db_preset['username']
            password = db_preset['password']
            database = db_preset.get('database', '')

            client = ClickHouseClient(host=host, user=username, password=password, database=database)
            total_rows = client.execute(f"SELECT COUNT(*) FROM ({query}) AS total_query")[0][0]

            offset = 0
            while offset < total_rows:
                chunk_query = f"{query} LIMIT {chunk_size} OFFSET {offset}"
                rows = client.execute(chunk_query)
                columns_query = f"DESCRIBE TABLE {query.split('FROM')[1].strip()}"
                columns = [row[0] for row in client.execute(columns_query)]

                df_chunk = pd.DataFrame(rows, columns=columns)
                print(df_chunk)
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
                df_chunk.to_csv(temp_file.name, index=False)
                temp_files.append(temp_file.name)

                offset += chunk_size

        elif db_type == 'google_big_query':
            json_file_path = db_preset['json_file_path']
            project_id = db_preset['project_id']

            bigquery_presets = config.get('google_big_query_presets', [])
            if not db_preset:
                raise ValueError(f"No matching Google BigQuery preset found for {db_preset_name}")

            credentials = service_account.Credentials.from_service_account_file(json_file_path)
            query_job = client.query(f"SELECT COUNT(*) FROM ({query}) AS total_query", project=project_id, credentials=credentials)
            total_rows = [dict(row) for row in query_job.result()][0]['f0_']

            offset = 0
            while offset < total_rows:
                chunk_query = f"{query} LIMIT {chunk_size} OFFSET {offset}"
                query_job = pandas_gbq.read_gbq(chunk_query, project_id=project_id, credentials=credentials, chunksize=chunk_size)

                for chunk in query_job:
                    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
                    chunk.to_csv(temp_file.name, index=False)
                    temp_files.append(temp_file.name)

                offset += chunk_size

        else:
            raise ValueError(f"Unsupported db_type: {db_type}")

        # Combine all chunks into a single DataFrame
        all_chunks = [pd.read_csv(temp_file) for temp_file in temp_files]
        df = pd.concat(all_chunks, ignore_index=True)
        self.df = df

        # Clean up temporary files
        for temp_file in temp_files:
            os.remove(temp_file)

        self.pr()
        gc.collect()
        return self


    def fp(self, file_path):
        """LOAD::[d.fp('/absolute/path')] From path."""
        file_extension = file_path.split('.')[-1]
        
        if file_extension == 'csv':
            self.df = pd.read_csv(file_path)
        elif file_extension in ['xls', 'xlsx']:
            self.df = pd.read_excel(file_path)
        elif file_extension == 'json':
            self.df = pd.read_json(file_path)
        elif file_extension == 'parquet':
            self.df = pd.read_parquet(file_path)
        elif file_extension in ['h5', 'hdf5']:
            with pd.HDFStore(file_path, mode='r') as store:
                available_keys = store.keys()
                if len(available_keys) == 1:
                    # If there is only one key, load it directly
                    self.df = pd.read_hdf(file_path, key=available_keys[0])
                    print(f"Loaded key: {available_keys[0]}")
                else:
                    while True:
                        # List available keys
                        print("Available keys:", available_keys)

                        key = input("Enter the key for the HDF5 dataset: ").strip()
                        if key in available_keys:
                            self.df = pd.read_hdf(file_path, key=key)
                            break
                        else:
                            print(f"Key '{key}' is not in the available keys. Please try again.")
        elif file_extension == 'feather':
            self.df = pd.read_feather(file_path)
        elif file_extension == 'pkl':
            self.df = pd.read_pickle(file_path)
        else:
            raise ValueError(f"Unsupported file extension: {file_extension}")
        
        self.pr()
        gc.collect()
        return self


    def fd(self):
        """LOAD::[d.fd()] From directory."""
        directories = [os.path.expanduser(f"~/{folder}") for folder in ["Desktop", "Downloads", "Documents"]]
        parseable_extensions = ['csv', 'xls', 'xlsx', 'json', 'parquet', 'h5', 'hdf5', 'feather', 'pkl']

        files = []
        for directory in directories:
            for root, _, filenames in os.walk(directory):
                for ext in parseable_extensions:
                    files.extend(glob.glob(os.path.join(root, f'*.{ext}')))

        if not files:
            print("No parseable files found in the specified directories.")
            gc.collect()
            return self

        files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        recent_files = files[:7]

        file_info = []
        for i, file in enumerate(recent_files, start=1):
            last_modified = datetime.fromtimestamp(os.path.getmtime(file)).strftime('%Y-%m-%d %H:%M:%S')
            file_size = os.path.getsize(file) / (1024 * 1024)  # Convert bytes to MB
            file_info.append((i, file, last_modified, file_size))
            print(f"{i}: {file} (Last Modified: {last_modified}, Size: {file_size:.2f} MB)")

        while True:
            try:
                choice = input("Choose a number corresponding to the file you want to load: ").strip()
                choice = int(choice)

                if 1 <= choice <= len(recent_files):
                    file_path = recent_files[choice - 1]  # Correct the index
                    file_extension = file_path.split('.')[-1]

                    if file_extension == 'csv':
                        self.df = pd.read_csv(file_path)
                    elif file_extension in ['xls', 'xlsx']:
                        self.df = pd.read_excel(file_path)
                    elif file_extension == 'json':
                        self.df = pd.read_json(file_path)
                    elif file_extension == 'parquet':
                        self.df = pd.read_parquet(file_path)
                    elif file_extension in ['h5', 'hdf5']:
                        try:
                            with pd.HDFStore(file_path, mode='r') as store:
                                available_keys = store.keys()
                                if len(available_keys) == 1:
                                    # If there is only one key, load it directly
                                    self.df = pd.read_hdf(file_path, key=available_keys[0])
                                    print(f"Loaded key: {available_keys[0]}")
                                else:
                                    while True:
                                        # List available keys
                                        print("Available keys:", available_keys)

                                        key = input("Enter the key for the HDF5 dataset: ").strip()
                                        if key in available_keys:
                                            self.df = pd.read_hdf(file_path, key=key)
                                            break
                                        else:
                                            print(f"Key '{key}' is not in the available keys. Please try again.")
                        except Exception as e:
                            print(f"Error opening HDF5 file: {e}")
                            gc.collect()
                            return self
                    elif file_extension == 'feather':
                        self.df = pd.read_feather(file_path)
                    elif file_extension == 'pkl':
                        self.df = pd.read_pickle(file_path)
                    else:
                        raise ValueError(f"Unsupported file extension: {file_extension}")

                    # Debug statement to check if DataFrame is assigned correctly
                    if self.df is not None:
                        print("DataFrame loaded successfully.")
                    else:
                        print("Failed to load DataFrame.")

                    self.pr()
                    break
                else:
                    print("Invalid choice. Please choose a valid number between 1 and 7.")
            except ValueError as e:
                print(f"ValueError: {e}. Invalid input. Please enter a number.")

        gc.collect()
        return self

    def des(self):
        """INSPECT::[d.d()]Describe."""
        if self.df is not None:
            description = self.df.describe()
            print(description)
            self.df = description
        else:
            raise ValueError("No DataFrame to describe. Please load a file first using the frm or frml method.")
        
        gc.collect()
        return self

    def fnr(self, n):
        """INSPECT::[d.fnr('n')] First n rows."""
        if self.df is not None:
            first_n_rows = self.df.head(n).to_dict(orient="records")
            for row in first_n_rows:
                pprint(row, indent=4)
                print()
        else:
            raise ValueError("No DataFrame to display. Please load a file first using the frm or frml method.")

        gc.collect()
        return self

    def lnr(self, n):
        """INSPECT::[d.lnr('n')] Last n rows."""
        if self.df is not None:
            last_n_rows = self.df.tail(n).to_dict(orient="records")
            for row in last_n_rows:
                pprint(row, indent=4)
                print()
        else:
            raise ValueError("No DataFrame to display. Please load a file first using the frm or frml method.")

        gc.collect()
        return self

    def tnuv(self, n, columns):
        """INSPECT::[d.tnuv(n, ['col1', 'col2'])] Top n unique values for specified columns."""
        if self.df is not None:
            for column in columns:
                if column in self.df.columns:
                    top_n = self.df[column].value_counts().nlargest(n)
                    print(f"Top {n} unique values for column '{column}':\n{top_n}\n")
                else:
                    print(f"Column '{column}' does not exist in the DataFrame.")
        else:
            raise ValueError("No DataFrame to display. Please load a file first using the frm or frml method.")
        
        gc.collect()
        return self

    def bnuv(self, n, columns):
        """INSPECT::[d.bnuv(n, ['col1', 'col2'])] Bottom n unique values for specified columns."""
        if self.df is not None:
            for column in columns:
                if column in self.df.columns:
                    bottom_n = self.df[column].value_counts().nsmallest(n)
                    print(f"Bottom {n} unique values for column '{column}':\n{bottom_n}\n")
                else:
                    print(f"Column '{column}' does not exist in the DataFrame.")
        else:
            raise ValueError("No DataFrame to display. Please load a file first using the frm or frml method.")
        
        gc.collect()
        return self

    def pr(self):
        """INSPECT::[d.pr()] Print."""
        if self.df is not None:
            print(self.df)
            columns_with_types = [f"{col} ({self.df[col].dtypes})" for col in self.df.columns]
            #columns_with_types = [f"{col} ({self.df[col].dtype})" for col in self.df.columns]
            print("Columns:", columns_with_types)
        else:
            raise ValueError("No DataFrame to print. Please load a file first using the frm or frml method.")

        gc.collect()
        return self

    def prc(self, column_pairs):
        """INSPECT::[d.prc([('column1','column2'), ('column3','column4')])] Print correlation for multiple pairs."""
        if self.df is not None:
            for col1, col2 in column_pairs:
                if col1 in self.df.columns and col2 in self.df.columns:
                    correlation = self.df[col1].corr(self.df[col2])
                    print(f"The correlation between '{col1}' and '{col2}' is {correlation}.")
                else:
                    print(f"One or both of the specified columns ('{col1}', '{col2}') do not exist in the DataFrame.")
        else:
            print("The DataFrame is empty.")

        gc.collect()
        return self

    def mem(self):
        """INSPECT::[d.mem()] Memory usage print."""
        if self.df is not None:

            # Print the size of the DataFrame in memory
            memory_usage = self.df.memory_usage(deep=True).sum() / (1024 * 1024)  # Convert bytes to MB
            print(f"Memory usage of DataFrame: {memory_usage:.2f} MB")

        else:
            raise ValueError("No DataFrame to print. Please load a file first using the frm or frml method.")

        gc.collect()
        return self


    def f(self, filter_expr):
        """TINKER::[d.f("col1 > 100 and Col1 == Col3 and Col5 == 'XYZ'")] Filter."""
        if self.df is not None:
            try:
                # Attempt to use query method for simple expressions
                self.df = self.df.query(filter_expr)
            except:
                # Fallback to eval for more complex expressions
                self.df = self.df[self.df.eval(filter_expr)]
            self.pr()
        else:
            raise ValueError("No DataFrame to filter. Please load a file first using the frm or frml method.")
        gc.collect()
        return self


    def fim(self, mobile_col):
        """TINKER::[d.fim('mobile')] Filter Indian mobiles."""
        if self.df is not None:
            self.df = self.df[self.df[mobile_col].apply(
                lambda x: str(x).isdigit() and 
                          str(x).startswith(('6', '7', '8', '9')) and 
                          len(set(str(x))) >= 4)]
            self.pr()
        else:
            raise ValueError("No DataFrame to filter. Please load a file first using the frm or frml method.")
        gc.collect()
        return self

    def fimc(self, mobile_col):
        """TINKER::[d.fimc('mobile')] Filter Indian mobiles (complement)."""
        if self.df is not None:
            self.df = self.df[~self.df[mobile_col].apply(
                lambda x: str(x).isdigit() and 
                          str(x).startswith(('6', '7', '8', '9')) and 
                          len(set(str(x))) >= 4)]
            self.pr()
        else:
            raise ValueError("No DataFrame to filter. Please load a file first using the frm or frml method.")
        gc.collect()
        return self

    def g(self, target_cols, agg_funcs):
        """TRANSFORM::[d.(['group_by_columns'], ['column1::sum', 'column1::count', 'column3::sum'])] Group. Permits multiple aggregations on the same column. Available agg options: sum, mean, min, max, count, size, std, var, median, css (comma-separated strings), etc."""

        def css(series):
            """Comma-separated strings aggregation function."""
            return ','.join(series.astype(str))

        def css_unique(series):
            """Comma-separated strings aggregation function with unique values."""
            return ','.join(pd.Series(series.astype(str)).drop_duplicates().tolist())

        def css_granular_unique(series):
            """Comma-separated strings aggregation function with unique values after splitting."""
            all_values = ','.join(series.astype(str)).split(',')
            unique_values = pd.Series(all_values).drop_duplicates().tolist()
            return ','.join(unique_values)

        def count_granular_unique(series):
            """Count unique values after splitting comma-separated strings."""
            all_values = ','.join(series.astype(str)).split(',')
            unique_values = pd.Series(all_values).drop_duplicates()
            return unique_values.count()

        if self.df is not None:
            # Create a copy of the DataFrame to avoid modifying the original
            df_copy = self.df.copy()

            # Step 1: Create new columns by duplicating the specified columns
            new_cols = []
            numeric_funcs = {'sum', 'mean', 'min', 'max', 'std', 'var', 'median'}
            for agg_func in agg_funcs:
                col, func = agg_func.split('::')
                new_col_name = f'{col}_{func}'
                df_copy[new_col_name] = self.df[col]
                new_cols.append(new_col_name)

                # Convert columns to numeric if the aggregation function requires it
                if func in numeric_funcs:
                    if df_copy[new_col_name].dtype == 'object':
                        df_copy[new_col_name] = pd.to_numeric(df_copy[new_col_name].str.replace(' ', ''), errors='coerce')

            # Step 2: Perform group-by and aggregations
            agg_dict = {}
            for agg_func in agg_funcs:
                col, func = agg_func.split('::')
                new_col_name = f'{col}_{func}'
                if func == 'css':
                    agg_dict[new_col_name] = css
                elif func == 'css_unique':
                    agg_dict[new_col_name] = css_unique
                elif func == 'css_granular_unique':
                    agg_dict[new_col_name] = css_granular_unique
                elif func == 'count_granular_unique':
                    agg_dict[new_col_name] = count_granular_unique
                else:
                    agg_dict[new_col_name] = func

            try:
                grouped_df = df_copy.groupby(target_cols).agg(agg_dict).reset_index()
                self.df = grouped_df
                self.pr()
            except Exception as e:
                print(f"Aggregation error: {e}")
        else:
            raise ValueError("No DataFrame to transform. Please load a file first using the frm or frml method.")

        gc.collect()
        return self

    def p(self, index, values, aggfunc='sum', seg_columns=None):
        """TRANSFORM::[d.p(['group_by_cols'], 'values_to_agg_col', 'sum', ['seg_columns'])] Pivot. Optional param: seg_columns. Available agg options: sum, mean, min, max, count, size, std, var, median, etc."""
        if self.df is not None:
            if seg_columns is not None:
                self.df = self.df.pivot_table(index=index, columns=seg_columns, values=values, aggfunc=aggfunc).reset_index()
            else:
                self.df = self.df.pivot_table(index=index, values=values, aggfunc=aggfunc).reset_index()
            self.pr()
        else:
            raise ValueError("No DataFrame to pivot. Please load a file first using the frm or frml method.")
        gc.collect()
        return self

    def doc(self, method_type_filter=None):
        """DOCUMENTATION::[d.doc()] Prints docs. Optional parameter: method_type_filter (str) egs. 'APPEND, PLOT'"""
            
        # Dictionary to hold methods grouped by their type
        method_types = collections.defaultdict(list)

        # Get all callable methods of the class that do not start with '__'
        methods = [method for method in dir(self) if callable(getattr(self, method)) and not method.startswith("__")]

        for method in methods:
            method_func = getattr(self, method)
            docstring = method_func.__doc__
            if docstring: 
                # Extract only the first line of the docstring
                first_line = docstring.split('\n')[0]
                if "::" in first_line:
                    # Find the first occurrence of "::" and split there
                    split_index = first_line.find("::")
                    method_type = first_line[:split_index].strip()
                    method_description = first_line[split_index + 2:].strip()
                    method_types[method_type].append((method, method_description))

            # Convert the filter to a list of method types if provided
        method_type_list = []
        if method_type_filter:
            method_type_list = [mt.strip() for mt in method_type_filter.split(',')]

        # Sort the method types alphabetically
        method_types_list = sorted(method_types.items())

        # Print the methods grouped by type with counts
        for i, (method_type, method_list) in enumerate(method_types_list):
            if i == len(method_types_list) - 1:
                branch = "└──"
                sub_branch = "    "
            else:
                branch = "├──"
                sub_branch = "│   "
            print(f"{branch} {method_type} [{len(method_list)}]")

            if method_type_filter and method_type in method_type_list:
                for j, (method, description) in enumerate(method_list):
                    if j == len(method_list) - 1:
                        sub_branch_end = "└──"
                    else:
                        sub_branch_end = "├──"
                    print(f"{sub_branch}{sub_branch_end} {method}: {description}")

        gc.collect()
        return self

    def s(self, name_or_path):
        """PERSIST::[d.s('/filename/or/path')] Save the DataFrame as a CSV or HDF5 file on the desktop."""
        if self.df is None:
            raise ValueError("No DataFrame to save. Please load or create a DataFrame first.")

        # Ensure the file has the correct extension
        if not name_or_path.lower().endswith(('.csv', '.h5')):
            name_or_path += '.csv'

        # Determine the desktop path
        desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")

        # Handle the input to create the full path
        if os.path.isabs(name_or_path):
            full_path = name_or_path
        else:
            full_path = os.path.join(desktop_path, name_or_path)

        # Ensure the directory exists
        os.makedirs(os.path.dirname(full_path), exist_ok=True)

        # Convert nullable integer columns to regular integers
        for col in self.df.select_dtypes(include=["integer"]).columns:
            self.df[col] = self.df[col].astype('int64')

        # Convert datetime columns to strings for HDF5 serialization
        for col in self.df.columns:
            if pd.api.types.is_datetime64_any_dtype(self.df[col]) or pd.api.types.is_timedelta64_dtype(self.df[col]):
                print(f"Converting datetime column {col} to string")
                self.df[col] = self.df[col].astype(str)

        # Convert date object columns to strings for HDF5 serialization
        for col in self.df.columns:
            if pd.api.types.is_object_dtype(self.df[col]):
                try:
                    # Attempt to convert the entire column to string
                    self.df[col] = self.df[col].astype(str)
                except Exception as e:
                    # Fallback: Convert each element to string individually
                    self.df[col] = self.df[col].apply(lambda x: str(x) if isinstance(x, (pd.Timestamp, pd.Period, pd.NaTType)) else x)
                print(f"Converting object column {col} to string")

        # Convert other extension arrays to NumPy arrays
        for col in self.df.columns:
            if pd.api.types.is_extension_array_dtype(self.df[col]):
                self.df[col] = self.df[col].astype(object).astype(str)

        # Save the DataFrame as a CSV or HDF5 file
        if full_path.lower().endswith('.csv'):
            self.df.to_csv(full_path, index=False)
            print(f"DataFrame saved to {full_path}")
        elif full_path.lower().endswith('.h5'):
            self.df.to_hdf(full_path, key='df', mode='w', format='table')
            print(f"DataFrame saved to {full_path}")

        gc.collect()
        return self


    def ds(self, path):
        """PERSIST::[d.ds('/filename/or/path')] Dask Save the DataFrame as a HDF5 or CSV file."""
        if self.df is None:
            raise ValueError("No DataFrame to save. Please load or create a DataFrame first.")

        # Determine the format based on the file extension
        if path.lower().endswith('.h5'):
            format = 'h5'
        else:
            format = 'csv'

        # Convert the Pandas DataFrame to a Dask DataFrame
        dask_df = dd.from_pandas(self.df, npartitions=10)

        # Convert nullable integer columns to regular integers
        for col in dask_df.select_dtypes(include=["integer"]).columns:
            dask_df[col] = dask_df[col].astype('int64')

        # Convert datetime columns to timezone-naive (remove timezone information)
        for col in dask_df.select_dtypes(include=["datetimetz", "datetime64"]).columns:
            dask_df[col] = dask_df[col].dt.tz_localize(None)

        # Convert date object columns to strings for HDF5 serialization
        for col in dask_df.columns:
            if pd.api.types.is_object_dtype(dask_df[col]):
                try:
                    # Attempt to convert the entire column to string
                    dask_df[col] = dask_df[col].astype(str)
                except Exception as e:
                    # Fallback: Convert each element to string individually
                    dask_df[col] = dask_df[col].apply(lambda x: str(x) if isinstance(x, (pd.Timestamp, pd.Period, pd.NaTType)) else x)
                print(f"Converting object column {col} to string")

        # Convert other extension arrays to strings
        for col in dask_df.columns:
            if pd.api.types.is_extension_array_dtype(dask_df[col]):
                dask_df[col] = dask_df[col].astype(str)

        # Save the DataFrame as a CSV or HDF5 file
        if path.lower().endswith('.csv'):
            dask_df.to_csv(path, single_file=True, index=False)
            print(f"DataFrame saved to {path}")
        elif path.lower().endswith('.h5'):
            dask_df.compute().to_hdf(path, key='df', mode='w', format='table')
            print(f"DataFrame saved to {path}")

        gc.collect()
        return self

    def oc(self, column_order_str):
        """TINKER::[d.oc('Column1, ..., Column2')] Order columns based on a string input."""
        
        if self.df is None:
            raise ValueError("No DataFrame to reorder. Please load or create a DataFrame first.")
        
        columns = self.df.columns.tolist()
        parts = [part.strip() for part in column_order_str.split(',')]
        
        new_order = []
        seen = set()
        
        for part in parts:
            if part == '...':
                continue
            elif part in columns:
                new_order.append(part)
                seen.add(part)
            else:
                raise ValueError(f"Column {part} not found in DataFrame.")

        remaining = [col for col in columns if col not in seen]

        if parts[0] == '...':
            new_order = remaining + new_order
        elif parts[-1] == '...':
            new_order = new_order + remaining
        else:
            pos = parts.index('...')
            new_order = new_order[:pos] + remaining + new_order[pos:]

        self.df = self.df[new_order]
        return self








    def abc(self, condition, new_col_name):
        """APPEND::[d.abc('column1 > 30 and column2 < 50', 'new_column_name')] Append boolean classification column."""
        if self.df is not None:
            # Replace column names in the condition with DataFrame access syntax
            condition = condition.replace(' and ', ' & ').replace(' or ', ' | ')
            self.df[new_col_name] = self.df.eval(condition)
            self.pr()
        else:
            raise ValueError("No DataFrame to append a boolean column. Please load a file first using the frm or frml method.")
        gc.collect()
        return self


    def arc(self, ranges, target_col, new_col_name):
        """APPEND::[d.arc('0,500,1000,2000,5000,10000,100000,1000000', 'column_to_be_analyzed', 'new_column_name')] Append ranged classification column."""
        if self.df is not None:
            range_list = [int(r) for r in ranges.split(',')]
            max_length = len(str(max(range_list)))
            labels = [f"{str(range_list[i]).zfill(max_length)} to {str(range_list[i+1]).zfill(max_length)}" for i in range(len(range_list) - 1)]
            self.df[new_col_name] = pd.cut(self.df[target_col], bins=range_list, labels=labels, right=False)
            self.pr()
        else:
            raise ValueError("No DataFrame to append a ranged classification column. Please load a file first using the frm or frml method.")
        gc.collect()
        return self

    def apc(self, percentiles, target_col, new_col_name):
        """APPEND::[d.apc('0,25,50,75,100', 'column_to_be_analyzed', 'new_column_name')] Append percentile classification column."""
        if self.df is not None:
            percentiles_list = [int(p) for p in percentiles.split(',')]
            labels = [f"{percentiles_list[i]} to {percentiles_list[i+1]}" for i in range(len(percentiles_list) - 1)]
            quantiles = [self.df[target_col].quantile(p / 100) for p in percentiles_list]
            self.df[new_col_name] = pd.cut(self.df[target_col], bins=quantiles, labels=labels, include_lowest=True)
            self.pr()
        else:
            raise ValueError("No DataFrame to append a percentile classification column. Please load a file first using the frm or frml method.")
        gc.collect()
        return self

    def ardc(self, date_ranges, target_col, new_col_name):
        """APPEND::[d.ardc('2024-01-01,2024-02-01,2024-03-01', 'date_column', 'new_date_classification')] Append ranged date classification column."""
        if self.df is not None:
            date_list = [pd.to_datetime(date) for date in date_ranges.split(',')]
            labels = [f"{date_list[i].strftime('%Y-%m-%d')} to {date_list[i+1].strftime('%Y-%m-%d')}" for i in range(len(date_list) - 1)]
            self.df[new_col_name] = pd.cut(pd.to_datetime(self.df[target_col]), bins=date_list, labels=labels, right=False)
            self.pr()
        else:
            raise ValueError("No DataFrame to append a ranged date classification column. Please load a file first using the frm or frml method.")
        gc.collect()
        return self

    def atcar(self, timestamps_col, reference_col, new_col_name):
        """APPEND::[d.atcar('comma_separated_timestamps_column', 'reference_date_or_timestamps_column', 'new_column_count_after_reference')] Append count of timestamps after reference time. Requires values in YYYY-MM-DD or YYYY-MM-DD HH:MM:SS format."""
        if self.df is not None:
            # Ensure the reference_col is in datetime format
            self.df[reference_col] = pd.to_datetime(self.df[reference_col])

            # Function to count timestamps after reference_time
            def count_timestamps_after(row):
                if pd.isna(row[timestamps_col]):
                    return 0
                timestamps = row[timestamps_col].split(',')
                reference_time = row[reference_col]
                count = 0
                for ts in timestamps:
                    ts = ts.strip()
                    try:
                        ts_datetime = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        try:
                            ts_datetime = datetime.strptime(ts, '%Y-%m-%d')
                        except ValueError:
                            continue
                    if ts_datetime > reference_time:
                        count += 1
                return count

            # Apply the function to each row
            self.df[new_col_name] = self.df.apply(lambda row: count_timestamps_after(row), axis=1)
            self.pr()
        else:
            raise ValueError("No DataFrame to append a timestamp count column. Please load a file first using the frm or frml method.")
        gc.collect()
        return self

    def atcbr(self, timestamps_col, reference_col, new_col_name):
        """APPEND::[d.atcbr('comma_separated_timestamps_column', 'reference_date_or_timestamp_column', 'new_column_count_before_reference')] Append count of timestamps before reference time. Requires values in YYYY-MM-DD or YYYY-MM-DD HH:MM:SS format."""
        if self.df is not None:
            # Ensure the reference_col is in datetime format
            self.df[reference_col] = pd.to_datetime(self.df[reference_col])

            # Function to count timestamps before reference_time
            def count_timestamps_before(row):
                if pd.isna(row[timestamps_col]):
                    return 0
                timestamps = row[timestamps_col].split(',')
                reference_time = row[reference_col]
                count = 0
                for ts in timestamps:
                    ts = ts.strip()
                    try:
                        ts_datetime = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        try:
                            ts_datetime = datetime.strptime(ts, '%Y-%m-%d')
                        except ValueError:
                            continue
                    if ts_datetime < reference_time:
                        count += 1
                return count

            # Apply the function to each row
            self.df[new_col_name] = self.df.apply(lambda row: count_timestamps_before(row), axis=1)
            self.pr()
        else:
            raise ValueError("No DataFrame to append a timestamp count column. Please load a file first using the frm or frml method.")
        gc.collect()
        return self

    def uj(self, other):
        """JOINS::[d.uj(d2)] Union join."""
        if not isinstance(other, p):
            raise TypeError("The 'other' parameter must be an instance of the p class")

        if self.df is None and other.df is None:
            raise ValueError("Both instances must contain a DataFrame or be empty")

        if self.df is None or self.df.empty:
            self.df = other.df
        elif other.df is None or other.df.empty:
            pass  # self.df remains unchanged
        else:
            # Ensure both DataFrames have the same columns
            if set(self.df.columns) != set(other.df.columns):
                raise ValueError("Both DataFrames must have the same columns for a union join")

            # Perform the union join using an in-place operation
            self.df = pd.concat([self.df, other.df], ignore_index=True)
            self.df.drop_duplicates(inplace=True)

        self.pr()
        gc.collect()
        return self

    def buj(self, other):
        """JOINS::[d.buj(d2)] Bag union join (does not drop duplicates)."""
        if not isinstance(other, p):
            raise TypeError("The 'other' parameter must be an instance of the p class")

        if self.df is None and other.df is None:
            raise ValueError("Both instances must contain a DataFrame or be empty")

        if self.df is None or self.df.empty:
            self.df = other.df
        elif other.df is None or other.df.empty:
            pass  # self.df remains unchanged
        else:
            # Ensure both DataFrames have the same columns
            if set(self.df.columns) != set(other.df.columns):
                raise ValueError("Both DataFrames must have the same columns for a bag union join")

            # Perform the bag union join without dropping duplicates
            self.df = pd.concat([self.df, other.df], ignore_index=True)

        self.pr()
        return self

    def lj(self, other, left_on, right_on):
        """JOINS::[d.lj(d2,'table_a_id','table_b_id')] Left join."""
        if not isinstance(other, p):
            raise TypeError("The 'other' parameter must be an instance of the p class")

        if self.df is None or other.df is None:
            raise ValueError("Both instances must contain a DataFrame")

        # Perform the left join directly assigning to self.df
        self.df = self.df.merge(other.df, how='left', left_on=left_on, right_on=right_on)
        self.pr()
        return self

    def rj(self, other, left_on, right_on):
        """JOINS::[d.rj(d2,'table_a_id','table_b_id')] Right join."""
        if not isinstance(other, p):
            raise TypeError("The 'other' parameter must be an instance of the p class")

        if self.df is None or other.df is None:
            raise ValueError("Both instances must contain a DataFrame")

        # Perform the right join directly assigning to self.df
        self.df = self.df.merge(other.df, how='right', left_on=left_on, right_on=right_on)
        self.pr()
        return self


    def rnc(self, rename_pairs):
        """TINKER::[d.rnc({'old_col1': 'new_col1', 'old_col2': 'new_col2'})] Rename columns."""
        print("aaa")
        self.pr()
        if self.df is not None:
            self.df = self.df.rename(columns=rename_pairs)
            self.pr()
        else:
            raise ValueError("No DataFrame to rename columns. Please load a file first using the frm or frml method.")
        gc.collect()
        return self


    def ie(self):
        """INSPECT::[d.ie()] Is empty. Returns boolean, not chainable."""
        if self.df is not None:
            return self.df.empty
        else:
            raise ValueError("No DataFrame to check. Please load a file first using the frm or frml method.")
        gc.collect()

    def mnpdz(self, columns):
        """TINKER::[d.mnpdz(['Column1', Column2])] Make numerically parseable by defaulting to zero for specified columns."""
        if self.df is not None:
            for column in columns:
                self.df[column] = pd.to_numeric(self.df[column], errors='coerce').fillna(0)
            self.pr()
        else:
            print("DataFrame is not initialized.")
        return self

    def cs(self, columns):
        """TINKER::[d.cs(['Column1', 'Column2'])] Cascade sort by specified columns."""
        if self.df is not None:
            self.df = self.df.sort_values(by=columns)
            self.pr()
        else:
            print("DataFrame is not initialized.")
        return self

    def axl(self, ratio_str):
        """PREDICT::[d.axl('70:20:10')] Append XGB training labels based on a ratio string."""
        if self.df is not None:
            # Parse the ratio string
            ratios = list(map(int, ratio_str.split(':')))
            total_ratio = sum(ratios)

            # Calculate the number of rows for each label
            total_rows = len(self.df)
            
            if len(ratios) == 2:
                # TRAIN:TEST scenario
                train_rows = (ratios[0] * total_rows) // total_ratio
                test_rows = total_rows - train_rows
                
                # Assign labels
                labels = ['TRAIN'] * train_rows + ['TEST'] * test_rows
                
            elif len(ratios) == 3:
                # TRAIN:VALIDATE:TEST scenario
                train_rows = (ratios[0] * total_rows) // total_ratio
                validate_rows = (ratios[1] * total_rows) // total_ratio
                test_rows = total_rows - train_rows - validate_rows
                
                # Assign labels
                labels = ['TRAIN'] * train_rows + ['VALIDATE'] * validate_rows + ['TEST'] * test_rows
                
            else:
                raise ValueError("Invalid ratio string format. Use 'TRAIN:TEST' or 'TRAIN:VALIDATE:TEST'.")

            # Append the labels to the DataFrame
            self.df['XGB_TYPE'] = labels
            self.pr()
        else:
            print("DataFrame is not initialized.")
        return self

    def axlinr(self, target_col, feature_cols, pred_col, boosting_rounds=100, model_path=None):
        """PREDICT::[d.axlinr('target_column','feature1, feature2, feature3','prediction_column_name')] Append XGB regression predictions. Assumes labelling by the .axl() method. Optional params: boosting_rounds (int), model_path (str)"""
        if self.df is not None and 'XGB_TYPE' in self.df.columns:
            # Convert feature_cols from string to list
            features = feature_cols.replace(' ', '').split(',')

            # Convert categorical columns to 'category' dtype
            for col in features:
                if self.df[col].dtype == 'object':
                    self.df[col] = self.df[col].astype('category')

            # Separate data into TRAIN, VALIDATE (if present), and TEST sets
            train_data = self.df[self.df['XGB_TYPE'] == 'TRAIN']
            validate_data = self.df[self.df['XGB_TYPE'] == 'VALIDATE'] if 'VALIDATE' in self.df['XGB_TYPE'].values else None
            test_data = self.df[self.df['XGB_TYPE'] == 'TEST']

            # Prepare DMatrix for XGBoost with enable_categorical parameter
            dtrain = xgb.DMatrix(train_data[features], label=train_data[target_col], enable_categorical=True)
            evals = [(dtrain, 'train')]

            if validate_data is not None:
                dvalidate = xgb.DMatrix(validate_data[features], label=validate_data[target_col], enable_categorical=True)
                evals.append((dvalidate, 'validate'))
            
            dtest = xgb.DMatrix(test_data[features], enable_categorical=True)

            # Train the XGBoost model
            params = {
                'objective': 'reg:squarederror',
                'eval_metric': 'rmse'
            }
            
            if validate_data is not None:
                model = xgb.train(params, dtrain, num_boost_round=boosting_rounds, evals=evals, early_stopping_rounds=10)
            else:
                model = xgb.train(params, dtrain, num_boost_round=boosting_rounds, evals=evals)

            # Make predictions for all data
            all_data = self.df[features]
            dall = xgb.DMatrix(all_data, enable_categorical=True)
            self.df[pred_col] = model.predict(dall)

            # Save the model if a path is provided
            if model_path:
                model.save_model(model_path)

            # Reorder columns
            columns_order = [col for col in self.df.columns if col not in ['XGB_TYPE', target_col, pred_col]] + ['XGB_TYPE', target_col, pred_col]
            self.df = self.df[columns_order]
            print("RMSE to accuracy: 50% (around 1.0); 60% (around 0.8); 70% (around 0.6); 80% (around 0.4); 90% (around 0.2)")
            self.pr()
        else:
            print("DataFrame is not initialized or 'XGB_TYPE' column is missing.")
        return self

    def axlogr(self, target_col, feature_cols, pred_col, boosting_rounds=100, model_path=None):
        """PREDICT::[d.axlogr('target_column','feature1, feature2, feature3','prediction_column_name')] Append XGB logistic regression predictions. Assumes labeling by the .axl() method. Optional params: boosting_rounds (int), model_path (str)"""
        if self.df is not None and 'XGB_TYPE' in self.df.columns:
            # Convert feature_cols from string to list
            features = feature_cols.replace(' ', '').split(',')

            # Convert categorical columns to 'category' dtype
            for col in features:
                if self.df[col].dtype == 'object':
                    self.df[col] = self.df[col].astype('category')

            # Separate data into TRAIN, VALIDATE (if present), and TEST sets
            train_data = self.df[self.df['XGB_TYPE'] == 'TRAIN']
            validate_data = self.df[self.df['XGB_TYPE'] == 'VALIDATE'] if 'VALIDATE' in self.df['XGB_TYPE'].values else None
            test_data = self.df[self.df['XGB_TYPE'] == 'TEST']

            # Prepare DMatrix for XGBoost with enable_categorical parameter
            dtrain = xgb.DMatrix(train_data[features], label=train_data[target_col], enable_categorical=True)
            evals = [(dtrain, 'train')]

            if validate_data is not None:
                dvalidate = xgb.DMatrix(validate_data[features], label=validate_data[target_col], enable_categorical=True)
                evals.append((dvalidate, 'validate'))

            dtest = xgb.DMatrix(test_data[features], enable_categorical=True)

            # Train the XGBoost model
            params = {
                'objective': 'binary:logistic',
                'eval_metric': 'auc'
            }

            if validate_data is not None:
                model = xgb.train(params, dtrain, num_boost_round=boosting_rounds, evals=evals, early_stopping_rounds=10)
            else:
                model = xgb.train(params, dtrain, num_boost_round=boosting_rounds, evals=evals)

            # Make predictions for all data
            all_data = self.df[features]
            dall = xgb.DMatrix(all_data, enable_categorical=True)
            self.df[pred_col] = model.predict(dall)

            # Save the model if a path is provided
            if model_path:
                model.save_model(model_path)

            # Reorder columns
            columns_order = [col for col in self.df.columns if col not in ['XGB_TYPE', target_col, pred_col]] + ['XGB_TYPE', target_col, pred_col]
            self.df = self.df[columns_order]
            print("AUC to accuracy: 50% (around 0.50); 60% (around 0.65); 70% (around 0.75); 80% (around 0.95); 90% (around 0.95)")
            self.pr()
        else:
            print("DataFrame is not initialized or 'XGB_TYPE' column is missing.")
        return self

    def plc(self, y, x=None, save_path=None):
        """PLOT::[d.plc(y='Column1, Column2, Column3')] Plot line chart. Optional param: x (str), i.e. a single column name for the x axis eg. 'Column5', image_save_path (str)"""
        y = y.replace(' ', '').split(',')
        plt.figure(figsize=(10, 6))

        if x is None:
            x_data = self.df.index
            x_label = 'index'
        else:
            x_data = self.df[x]
            x_label = x

        for y_column in y:
            plt.plot(x_data, self.df[y_column], label=y_column)

        plt.xlabel(x_label)
        plt.ylabel('y_axis_columns')
        plt.title('Line Chart')
        plt.legend()

        if save_path is None:
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
            save_path = temp_file.name
            plt.savefig(save_path)
            print(f"Plot saved to temporary file: {save_path}")
        else:
            plt.savefig(save_path)
            print(f"Plot saved to {save_path}")

        # Attempt to open and display the saved image
        try:
            img = Image.open(save_path)
            img.show()
        except Exception as e:
            print(f"Failed to open image: {e}")
        
        return self

    def pdist(self, y, save_path=None):
        """PLOT::[d.pdist(y='Column1, Column2, Column3')] Plot distribution histograms for the specified columns. Optional param: image_save_path (str)"""
        if isinstance(y, str):
            y = y.replace(' ', '').split(',')
        elif isinstance(y, list):
            y = [str(col).replace(' ', '') for col in y]
        else:
            raise ValueError("Parameter 'y' must be a string or a list of strings.")

        if self.df is None:
            raise ValueError("Data frame is not set. Use set_data() to initialize the data frame.")

        num_columns = len(y)
        plt.figure(figsize=(10, 6 * num_columns))

        for i, y_column in enumerate(y, 1):
            if y_column not in self.df.columns:
                raise ValueError(f"Column '{y_column}' not found in data frame.")
            
            plt.subplot(num_columns, 1, i)
            self.df[y_column].hist(bins=30, alpha=0.7)
            plt.xlabel(y_column)
            plt.ylabel('Frequency')
            plt.title(f'Distribution of {y_column}')

        plt.tight_layout()

        if save_path is None:
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
            save_path = temp_file.name
            plt.savefig(save_path)
            print(f"Plot saved to temporary file: {save_path}")
        else:
            plt.savefig(save_path)
            print(f"Plot saved to {save_path}")

        # Attempt to open and display the saved image
        try:
            img = Image.open(save_path)
            img.show()
        except Exception as e:
            print(f"Failed to open image: {e}")

        return self


    def pqq(self, y, save_path=None):
        """PLOT::[d.pqq(y='Column1, Column2, Column3')] Plot Q-Q plots for the specified columns. Optional param: image_save_path (str)"""
        if isinstance(y, str):
            y = y.replace(' ', '').split(',')
        elif isinstance(y, list):
            y = [str(col).replace(' ', '') for col in y]
        else:
            raise ValueError("Parameter 'y' must be a string or a list of strings.")

        if self.df is None:
            raise ValueError("Data frame is not set. Use set_data() to initialize the data frame.")

        num_columns = len(y)
        plt.figure(figsize=(10, 6 * num_columns))

        for i, y_column in enumerate(y, 1):
            if y_column not in self.df.columns:
                raise ValueError(f"Column '{y_column}' not found in data frame.")
            
            plt.subplot(num_columns, 1, i)
            stats.probplot(self.df[y_column], dist="norm", plot=plt)
            plt.title(f'Q-Q Plot of {y_column}')

        plt.tight_layout()

        if save_path is None:
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
            save_path = temp_file.name
            plt.savefig(save_path)
            print(f"Plot saved to temporary file: {save_path}")
        else:
            plt.savefig(save_path)
            print(f"Plot saved to {save_path}")

        # Attempt to open and display the saved image
        try:
            img = Image.open(save_path)
            img.show()
        except Exception as e:
            print(f"Failed to open image: {e}")

        return self

    def pcr(self, y, save_path=None):
        """PLOT::[d.pcr(y='Column1, Column2, Column3')] Plot correlation heatmap for the specified columns. Optional param: image_save_path (str)"""
        if isinstance(y, str):
            y = y.replace(' ', '').split(',')
        elif isinstance(y, list):
            y = [str(col).replace(' ', '') for col in y]
        else:
            raise ValueError("Parameter 'y' must be a string or a list of strings.")

        if self.df is None:
            raise ValueError("Data frame is not set. Use set_data() to initialize the data frame.")

        selected_columns = [col for col in y if col in self.df.columns]
        if len(selected_columns) == 0:
            raise ValueError("None of the specified columns are found in the data frame.")

        correlation_matrix = self.df[selected_columns].corr()
        plt.figure(figsize=(10, 8))
        sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', linewidths=0.5)
        plt.title('Correlation Matrix')

        if save_path is None:
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
            save_path = temp_file.name
            plt.savefig(save_path)
            print(f"Plot saved to temporary file: {save_path}")
        else:
            plt.savefig(save_path)
            print(f"Plot saved to {save_path}")

        plt.close()

        
        # Attempt to open and display the saved image
        
        try:
            img = Image.open(save_path)
            img.show()
        except Exception as e:
            print(f"Failed to open image: {e}")
        

        return self

    def ancc(self, features, operation, cluster_column_name, n_clusters_finding_method=None, visualize=True):
        """APPEND::[d.ancc('Column1,Column2', 'KMEANS', 'new_cluster_column_name', n_clusters_finding_method='FIXED:5', visualize=True)] Append n-cluster column. Available operations: KMEANS/ AGGLOMERATIVE/ MEAN_SHIFT/ GMM/ SPECTRAL/ BIRCH. Optional: visualize (boolean), n_cluster_finding_method (str) i.e. ELBOW/ SILHOUETTE/ FIXED:n (specify a number of n clusters)."""
        def perform_kmeans(X, n_clusters):
            kmeans = KMeans(n_clusters=n_clusters, random_state=0)
            return kmeans.fit_predict(X)

        def perform_dbscan(X, eps, min_samples):
            dbscan = DBSCAN(eps=eps, min_samples=min_samples)
            return dbscan.fit_predict(X)

        def perform_agglomerative(X, n_clusters):
            agglomerative = AgglomerativeClustering(n_clusters=n_clusters)
            return agglomerative.fit_predict(X)

        def perform_mean_shift(X):
            mean_shift = MeanShift()
            return mean_shift.fit_predict(X)

        def perform_gmm(X, n_clusters):
            gmm = GaussianMixture(n_components=n_clusters, random_state=0)
            gmm.fit(X)
            return gmm.predict(X)

        def perform_spectral(X, n_clusters):
            spectral = SpectralClustering(n_clusters=n_clusters, random_state=0)
            return spectral.fit_predict(X)

        def perform_birch(X, n_clusters):
            birch = Birch(n_clusters=n_clusters)
            return birch.fit_predict(X)

        def find_optimal_clusters_elbow(X):
            wcss = []
            for i in range(1, 11):
                kmeans = KMeans(n_clusters=i, random_state=0)
                kmeans.fit(X)
                wcss.append(kmeans.inertia_)

            # Automatically determine the "elbow" point
            deltas = np.diff(wcss)
            second_deltas = np.diff(deltas)
            optimal_clusters = np.argmax(second_deltas) + 2  # +2 to offset the second difference
            return optimal_clusters

        def find_optimal_clusters_silhouette(X):
            silhouette_scores = []
            for i in range(2, min(11, len(X))):
                kmeans = KMeans(n_clusters=i, random_state=0)
                labels = kmeans.fit_predict(X)
                if len(np.unique(labels)) > 1:
                    silhouette_scores.append(silhouette_score(X, labels))
                else:
                    silhouette_scores.append(-1)  # Invalid score if only one cluster

            # Automatically determine the optimal number of clusters
            optimal_clusters = np.argmax(silhouette_scores) + 2  # +2 because range starts from 2
            return optimal_clusters

        def parse_optimal_clustering_method(optimal_clustering_method):
            if optimal_clustering_method.startswith('FIXED:'):
                return int(optimal_clustering_method.split(':')[1])
            elif optimal_clustering_method in ['ELBOW', 'SILHOUETTE']:
                return optimal_clustering_method
            else:
                raise ValueError("Unsupported optimal clustering method. Choose from 'FIXED:n', 'ELBOW' or 'SILHOUETTE'.")

        def save_and_show_plot(plots):
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
            plt.savefig(temp_file.name, bbox_inches='tight')
            plt.close()
            plots.append(temp_file.name)


        def combine_and_show_plots(plot_files):
            images = [Image.open(file) for file in plot_files]
            widths, heights = zip(*(img.size for img in images))

            total_width = max(widths)
            total_height = sum(heights) + (len(images) - 1) * 10

            combined_image = Image.new('RGB', (total_width, total_height))

            y_offset = 0
            for img in images:
                combined_image.paste(img, (0, y_offset))
                y_offset += img.height + 10

            combined_image.show()

        def plot_clusters_3d(x_col, y_col, z_col, cluster_col, plots):
            fig = plt.figure(figsize=(10, 6))
            ax = fig.add_subplot(111, projection='3d')
            for cluster in self.df[cluster_col].unique():
                cluster_data = self.df[self.df[cluster_col] == cluster]
                ax.scatter(cluster_data[x_col], cluster_data[y_col], cluster_data[z_col], label=f'Cluster {cluster}')
            ax.set_xlabel(x_col)
            ax.set_ylabel(y_col)
            ax.set_zlabel(z_col)
            ax.legend()
            plt.title('3D Cluster Plot')
            save_and_show_plot(plots)

        def plot_heatmap_of_centers(X, cluster_col, plots):
            kmeans = KMeans(n_clusters=self.df[cluster_col].nunique(), random_state=0).fit(X)
            cluster_centers = pd.DataFrame(kmeans.cluster_centers_, columns=X.columns)
            plt.figure(figsize=(10, 10))
            sns.heatmap(cluster_centers, annot=True, cmap='coolwarm')
            plt.title('Heatmap of Cluster Centers')
            save_and_show_plot(plots)

        def plot_cluster_analysis(X, feature_list, cluster_column_name, operation):
            """Visualize cluster analysis results."""
            plots = []

            # Pair plot
            pair_plot = sns.pairplot(self.df[feature_list + [cluster_column_name]], hue=cluster_column_name)
            pair_plot.fig.set_size_inches(10, 8)
            pair_plot.fig.suptitle('Pair Plot', y=1.02)
            save_and_show_plot(plots)

            # 3D scatter plot if there are at least 3 features
            if len(feature_list) == 3:
                plot_clusters_3d(feature_list[0], feature_list[1], feature_list[2], cluster_column_name, plots)

            # Heatmap of cluster centers (if applicable)
            if operation in ['KMEANS', 'GMM']:
                plot_heatmap_of_centers(X, cluster_column_name, plots)

            combine_and_show_plots(plots)

        plots = []

        if self.df is not None:
            # Select the features for clustering
            feature_list = [feature.strip() for feature in features.split(',')]
            X = self.df[feature_list]

            # Determine the optimal number of clusters if required
            n_clusters = None
            if n_clusters_finding_method == None:
                n_clusters_finding_method = 'ELBOW'
            optimal_method = parse_optimal_clustering_method(n_clusters_finding_method)
            if isinstance(optimal_method, int):
                n_clusters = optimal_method
            elif optimal_method == 'ELBOW':
                n_clusters = find_optimal_clusters_elbow(X)
            elif optimal_method == 'SILHOUETTE':
                n_clusters = find_optimal_clusters_silhouette(X)

            # Perform the chosen clustering operation
            if operation == 'KMEANS':
                self.df[cluster_column_name] = perform_kmeans(X, n_clusters)
            elif operation == 'AGGLOMERATIVE':
                self.df[cluster_column_name] = perform_agglomerative(X, n_clusters)
            elif operation == 'MEAN_SHIFT':
                self.df[cluster_column_name] = perform_mean_shift(X)
            elif operation == 'GMM':
                self.df[cluster_column_name] = perform_gmm(X, n_clusters)
            elif operation == 'SPECTRAL':
                self.df[cluster_column_name] = perform_spectral(X, n_clusters)
            elif operation == 'BIRCH':
                self.df[cluster_column_name] = perform_birch(X, n_clusters)
            else:
                raise ValueError("Operation must be one of 'KMEANS', 'AGGLOMERATIVE', 'MEAN_SHIFT', 'GMM', 'SPECTRAL', or 'BIRCH'")
            
            if visualize:
                plot_cluster_analysis(X, feature_list, cluster_column_name, operation)

            self.pr()
        else:
            raise ValueError("No DataFrame to append a cluster analysis column. Please load a file first using the frm or frml method.")
        gc.collect()
        return self



    def adbscancc(self, features, cluster_column_name, eps, min_samples, visualize=True):
        """APPEND::[d.adbscancc('Column1,Column2', 'new_cluster_column_name', eps=0.5, min_samples=5, visualize=True)] Append DBSCAN cluster column. Optional: visualize (boolean)."""
        def perform_dbscan(X, eps, min_samples):
            dbscan = DBSCAN(eps=eps, min_samples=min_samples)
            return dbscan.fit_predict(X)

        def save_and_show_plot(plots):
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.png')
            plt.savefig(temp_file.name, bbox_inches='tight')
            plt.close()
            plots.append(temp_file.name)


        def combine_and_show_plots(plot_files):
            images = [Image.open(file) for file in plot_files]
            widths, heights = zip(*(img.size for img in images))

            total_width = max(widths)
            total_height = sum(heights) + (len(images) - 1) * 10

            combined_image = Image.new('RGB', (total_width, total_height))

            y_offset = 0
            for img in images:
                combined_image.paste(img, (0, y_offset))
                y_offset += img.height + 10

            combined_image.show()

        def plot_clusters_3d(x_col, y_col, z_col, cluster_col, plots):
            fig = plt.figure(figsize=(10, 6))
            ax = fig.add_subplot(111, projection='3d')
            for cluster in self.df[cluster_col].unique():
                cluster_data = self.df[self.df[cluster_col] == cluster]
                ax.scatter(cluster_data[x_col], cluster_data[y_col], cluster_data[z_col], label=f'Cluster {cluster}')
            ax.set_xlabel(x_col)
            ax.set_ylabel(y_col)
            ax.set_zlabel(z_col)
            ax.legend()
            plt.title('3D Cluster Plot')
            save_and_show_plot(plots)

        def plot_heatmap_of_centers(X, cluster_col, plots):
            kmeans = KMeans(n_clusters=self.df[cluster_col].nunique(), random_state=0).fit(X)
            cluster_centers = pd.DataFrame(kmeans.cluster_centers_, columns=X.columns)
            plt.figure(figsize=(10, 10))
            sns.heatmap(cluster_centers, annot=True, cmap='coolwarm')
            plt.title('Heatmap of Cluster Centers')
            save_and_show_plot(plots)

        def plot_cluster_analysis(X, feature_list, cluster_column_name):
            """Visualize cluster analysis results."""
            plots = []

            # Pair plot
            pair_plot = sns.pairplot(self.df[feature_list + [cluster_column_name]], hue=cluster_column_name)
            pair_plot.fig.set_size_inches(10, 8)
            pair_plot.fig.suptitle('Pair Plot', y=1.02)
            save_and_show_plot(plots)

            # 3D scatter plot if there are at least 3 features
            if len(feature_list) == 3:
                plot_clusters_3d(feature_list[0], feature_list[1], feature_list[2], cluster_column_name, plots)

            combine_and_show_plots(plots)

        plots = []

        if self.df is not None:
            # Select the features for clustering
            feature_list = [feature.strip() for feature in features.split(',')]
            X = self.df[feature_list]

            self.df[cluster_column_name] = perform_dbscan(X, eps, min_samples)
           
            if visualize:
                plot_cluster_analysis(X, feature_list, cluster_column_name)

            self.pr()
        else:
            raise ValueError("No DataFrame to append a cluster analysis column. Please load a file first using the frm or frml method.")
        gc.collect()
        return self

    def pnfc(self, n, columns, order_by="FREQ_DESC"):
        """INSPECT::[d.pnfc(5,'Column1,Columns')] Print n frequency cascading. Optional: order_by (str), which has options: ASC, DESC, FREQ_ASC, FREQ_DESC (default)"""
        
        # Split columns string into a list
        columns = [col.strip() for col in columns.split(",")]

        # Generate cascading frequency report
        def generate_cascade_report(df, columns, limit, order_by):
            if not columns:
                return None

            current_col = columns[0]
            if current_col not in df.columns:
                return None

            # Handle NaN, NaT, and None values correctly based on the column type
            df = df.copy()

            if pd.api.types.is_numeric_dtype(df[current_col]):
                missing_value_str = 'NaN'
            elif pd.api.types.is_datetime64_any_dtype(df[current_col]):
                missing_value_str = 'NaT'
            else:
                missing_value_str = 'None'

            frequency = df[current_col].value_counts(dropna=False)
            frequency = frequency.rename(index={pd.NaT: 'NaT', None: 'None', float('nan'): 'NaN'})

            if limit is not None:
                frequency = frequency.nlargest(limit)
            sorted_frequency = sort_frequency(frequency, order_by)

            report = {}
            for value, count in sorted_frequency.items():
                if pd.isna(value) or value in ['NaN', 'NaT', 'None']:
                    filtered_df = df[df[current_col].isna()]
                    value_str = missing_value_str
                else:
                    filtered_df = df[df[current_col] == value]
                    value_str = str(value)

                if len(columns) > 1:
                    sub_report = generate_cascade_report(filtered_df, columns[1:], limit, order_by)
                    report[value_str] = {
                        "count": str(count),
                        f"sub_distribution({columns[1]})": sub_report if sub_report else {}
                    }
                else:
                    report[value_str] = {
                        "count": str(count)
                    }

            return report



        def sort_frequency(frequency, order_by):
            if order_by == "ASC":
                return dict(sorted(frequency.items(), key=lambda item: item[0]))
            elif order_by == "DESC":
                return dict(sorted(frequency.items(), key=lambda item: item[0], reverse=True))
            elif order_by == "FREQ_ASC":
                return dict(sorted(frequency.items(), key=lambda item: item[1]))
            else:  # Default to "FREQ_DESC"
                return dict(sorted(frequency.items(), key=lambda item: item[1], reverse=True))

        # Generate report
        report = generate_cascade_report(self.df, columns, n, order_by)
        print(json.dumps(report, indent=2))

        return self

    def pnfl(self, n, columns, order_by="FREQ_DESC"):
        """INSPECT::[d.pnfl(5,'Column1,Columns')] Print n frequency linear. Optional: order_by (str), which has options: ASC, DESC, FREQ_ASC, FREQ_DESC (default)"""

        # Split columns string into a list
        columns = [col.strip() for col in columns.split(",")]

        def generate_linear_report(df, columns, limit, order_by):
            report = {}

            for current_col in columns:
                if current_col not in df.columns:
                    continue

                # Handle NaN, NaT, and None values correctly based on the column type
                if pd.api.types.is_numeric_dtype(df[current_col]):
                    missing_value_str = 'NaN'
                elif pd.api.types.is_datetime64_any_dtype(df[current_col]):
                    missing_value_str = 'NaT'
                else:
                    missing_value_str = 'None'

                frequency = df[current_col].value_counts(dropna=False)
                frequency = frequency.rename(index={pd.NaT: 'NaT', None: 'None', float('nan'): 'NaN'})

                if limit is not None:
                    frequency = frequency.nlargest(limit)
                sorted_frequency = sort_frequency(frequency, order_by)

                col_report = {}
                for value, count in sorted_frequency.items():
                    value_str = str(value) if not pd.isna(value) else missing_value_str
                    col_report[value_str] = str(count)

                report[current_col] = col_report

            return report

        def sort_frequency(frequency, order_by):
            if order_by == "ASC":
                return dict(sorted(frequency.items(), key=lambda item: item[0]))
            elif order_by == "DESC":
                return dict(sorted(frequency.items(), key=lambda item: item[0], reverse=True))
            elif order_by == "FREQ_ASC":
                return dict(sorted(frequency.items(), key=lambda item: item[1]))
            else:  # Default to "FREQ_DESC"
                return dict(sorted(frequency.items(), key=lambda item: item[1], reverse=True))

        # Generate report
        report = generate_linear_report(self.df, columns, n, order_by)
        print(json.dumps(report, indent=2))

        return self

    def rtc(self, columns_to_retain):
        """TINKER::[d.rtc(['column1','column2'])] Retain columns specified and drop the rest."""
        if not isinstance(columns_to_retain, list):
            raise ValueError("columns_to_retain should be a list of column names.")
        self.df = self.df[columns_to_retain]
        return self

    def mad(self, other_p, column_name):
        """MASK::[d.mad(mask_p_instance, 'column7')] Mask against DataFrame, retaining only rows with common column values. Only rows where for the mutual column 7, values exist in mask_p_instance, will be retained"""
        if not isinstance(other_p, p):
            raise ValueError("other_p should be an instance of the class p.")
        if column_name not in self.df.columns or column_name not in other_p.df.columns:
            raise ValueError("The specified column must exist in both DataFrames.")
        self.df = self.df[self.df[column_name].isin(other_p.df[column_name])]
        return self

    def goaibc(self, job_name, model, columns_to_analyse, classification_options):
        """OPENAI::[batch_id = d.oaibc('fruit_or_vegetable_classification','gpt-3.5-turbo','Column1, Column3','fruit, vegetable, other')] Get OpenAI Batch Classification. Returns a batch id"""

        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [os.path.join(home_dir, folder) for folder in ["Desktop", "Documents", "Downloads"]]

            for path in search_paths:
                for root, _, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        def load_key(key_name):
            config_path = locate_config_file()
            with open(config_path, 'r') as f:
                config = json.load(f)

            return config.get(key_name)

        open_ai_key = load_key('open_ai_key')
        client = OpenAI(api_key=open_ai_key)

        # Prepare the batch input file
        batch_input_data = []
        for idx, row in self.df.iterrows():
            row_data = {col: row[col] for col in columns_to_analyse.split(', ')}
            prompt = f"Return a single classification response considering all the data provided below, labelling the entire dataset with one of the following categories: {classification_options}. Data: {row_data}"
            request_data = {
                "custom_id": f"request-{idx}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": model,
                    "response_format": {"type": "json_object"},
                    "messages": [
                        {"role": "system", "content": """You are a helpful assistant that returns a json classifying the user's input in this format: {"classification":"your_response"}."""},
                        {"role": "user", "content": prompt}
                    ],
                    "max_tokens": 1000
                }
            }
            batch_input_data.append(request_data)

        #print(batch_input_data)

        # Write the batch input data to a .jsonl file
        batch_input_file_path = tempfile.mktemp(suffix=".jsonl")
        with open(batch_input_file_path, 'w') as batch_input_file:
            for request in batch_input_data:
                batch_input_file.write(json.dumps(request) + '\n')

        # Upload the batch input file
        batch_input_file = client.files.create(
            file=open(batch_input_file_path, "rb"),
            purpose="batch"
        )

        # Create the batch
        batch_input_file_id = batch_input_file.id  # Corrected from ['id'] to .id
        batch = client.batches.create(
            input_file_id=batch_input_file_id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
            metadata={
                "description": job_name
            }
        )
        print(f"Batch ID for {job_name}: {batch.id}")

        return batch.id

    def oaibl(self):
        """OPENAI::[d.oaibl()] OpenAI Batch list. Lists OpenAI batch jobs."""

        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [os.path.join(home_dir, folder) for folder in ["Desktop", "Documents", "Downloads"]]

            for path in search_paths:
                for root, _, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        def load_key(key_name):
            config_path = locate_config_file()
            with open(config_path, 'r') as f:
                config = json.load(f)
            return config.get(key_name)

        open_ai_key = load_key('open_ai_key')
        client = OpenAI(api_key=open_ai_key)

        # Collect batch statuses
        batch_statuses = []
        response = client.batches.list(limit=10)  # Adjust the limit as needed

        for batch_info in response:
            batch_statuses.append({
                "batch_id": batch_info.id,
                "status": batch_info.status,
                "created_at": batch_info.created_at,
                "completed_at": getattr(batch_info, 'completed_at', None),
                "failed_at": getattr(batch_info, 'failed_at', None),
                "expired_at": getattr(batch_info, 'expired_at', None)
            })

        # Create a DataFrame to display batch statuses
        status_df = pd.DataFrame(batch_statuses)
        print(status_df)

        # Return self for method chaining
        return self


    def oaibc(self, batch_id):
        """OPENAI::[d.oaibc('batch_id_to_cancel')] OpenAI batch cancel. Cancel a batch and print its status."""
        
        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [os.path.join(home_dir, folder) for folder in ["Desktop", "Documents", "Downloads"]]

            for path in search_paths:
                for root, _, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        def load_key(key_name):
            config_path = locate_config_file()
            with open(config_path, 'r') as f:
                config = json.load(f)
            return config.get(key_name)

        open_ai_key = load_key('open_ai_key')
        client = OpenAI(api_key=open_ai_key)

        # Cancel the batch
        client.batches.cancel(batch_id)
        time.sleep(3)

        # Retrieve and print the batch info
        batch_info = client.batches.retrieve(batch_id)
        batch_status = {
            "batch_id": batch_info.id,
            "status": batch_info.status,
            "created_at": batch_info.created_at,
            "completed_at": getattr(batch_info, 'completed_at', None),
            "failed_at": getattr(batch_info, 'failed_at', None),
            "expired_at": getattr(batch_info, 'expired_at', None)
        }

        # Create a DataFrame to display batch status
        status_df = pd.DataFrame([batch_status])
        print(status_df)

        # Return self for method chaining
        return self

    def oaibs(self, batch_id):
        """OPENAI::[d.oaibs('batch_id')] OpenAI Batch status. Checks the status of a specific OpenAI batch job."""

        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [os.path.join(home_dir, folder) for folder in ["Desktop", "Documents", "Downloads"]]

            for path in search_paths:
                for root, _, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        def load_key(key_name):
            config_path = locate_config_file()
            with open(config_path, 'r') as f:
                config = json.load(f)
            return config.get(key_name)

        open_ai_key = load_key('open_ai_key')
        client = OpenAI(api_key=open_ai_key)

        # Retrieve the batch info
        batch_info = client.batches.retrieve(batch_id)

        # Extract the status
        status = batch_info.status

        # Print the status
        print(f"Batch Status: {status}")

        # Return the status for method chaining or other use
        return self

    def goaibs(self, batch_id):
        """OPENAI::[status, output_file_id = d.goaibs('batch_id')] OpenAI Batch get status. Returns the status of a specific OpenAI batch job."""

        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [os.path.join(home_dir, folder) for folder in ["Desktop", "Documents", "Downloads"]]

            for path in search_paths:
                for root, _, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        def load_key(key_name):
            config_path = locate_config_file()
            with open(config_path, 'r') as f:
                config = json.load(f)
            return config.get(key_name)

        open_ai_key = load_key('open_ai_key')
        client = OpenAI(api_key=open_ai_key)

        # Retrieve the batch info
        batch_info = client.batches.retrieve(batch_id)

        # Extract the status
        status = batch_info.status
        output_file_id = batch_info.output_file_id

        # Print the status
        print(f"Batch Status: {status}")
        print(f"Output file id: {output_file_id}")

        # Return the status for method chaining or other use
        return status, output_file_id

    def oais(self, file_path, model, columns_to_analyse, classification_options, batch_column_name):
        """OPENAI::[d.oais('path/to/save/file','gpt-3.5-turbo','Column7, Column9','Fruits, Vegetables','NewColumn')] OpenAI Sow. Create an OpenAI batch job and save the DataFrame as an H5 file with batch ID in metadata."""
        batch_id = self.goaibc('sow', model, columns_to_analyse, classification_options)

        h5_file_path = f"{file_path}.h5" if not file_path.endswith(".h5") else file_path
        self.df.to_hdf(h5_file_path, key='df', mode='w', format='table', complib='blosc', complevel=9)

        with pd.HDFStore(h5_file_path, mode='a') as store:
            store.get_storer('df').attrs.metadata = {'batch_id': batch_id, 'batch_column_name': batch_column_name}

        print(f"DataFrame with batch ID saved to {h5_file_path}")
        return batch_id

    def oaih(self, file_path):
        """OPENAI::[d.oaih('path/to/your/sowed/saved/file')] OpenAI harvest. Open the H5 file, check the status of the batch job, and open the file accordingly."""

        def download_output_file(output_file_id, open_ai_key):
            client = OpenAI(api_key=open_ai_key)
            
            # Retrieve the file metadata
            file_metadata = client.files.retrieve(output_file_id)
            print(file_metadata)

            # Retrieve the file content
            response = client.files.content(output_file_id)
            file_content = response.read()  # Read the content of the response

            # Write the content to a temporary file
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                tmp_file.write(file_content)
                tmp_file_path = tmp_file.name

            return tmp_file_path

        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [os.path.join(home_dir, folder) for folder in ["Desktop", "Documents", "Downloads"]]

            for path in search_paths:
                for root, _, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        def load_key(key_name):
            config_path = locate_config_file()
            with open(config_path, 'r') as f:
                config = json.load(f)
            return config.get(key_name)

        open_ai_key = load_key('open_ai_key')

        with pd.HDFStore(file_path, mode='r') as store:
            try:
                storer = store.get_storer('df')
                if not hasattr(storer.attrs, 'metadata'):
                    print("No harvest meta data found")
                    self.pr()
                    return self

                metadata = storer.attrs.metadata
            except AttributeError:
                print("No harvest meta data found")
                return self

            batch_id = metadata.get('batch_id')
            batch_column_name = metadata.get('batch_column_name')

            if not batch_id or not batch_column_name:
                print("No harvest meta data found")
                self.pr()
                return self

            self.df = store['df']

        if not batch_id:
            raise ValueError("No batch ID found in the file metadata.")

        status, output_file_id = self.goaibs(batch_id)

        if status == 'completed':
            print(f"Batch job {batch_id} completed")
            if output_file_id:
                tmp_file_path = download_output_file(output_file_id, open_ai_key)
                new_column_data = []
                with open(tmp_file_path, 'r') as f:
                    for line in f:
                        result = json.loads(line)
                        if 'response' in result and 'body' in result['response'] and 'choices' in result['response']['body'] and len(result['response']['body']['choices']) > 0:
                            message_content = result['response']['body']['choices'][0]['message']['content']
                            classification = json.loads(message_content)["classification"]
                            new_column_data.append(classification)
                        else:
                            print(f"Skipping invalid result: {result}")
                self.df[batch_column_name] = new_column_data

                # Save the DataFrame
                self.s(file_path)
            else:
                raise ValueError("No output file ID found for the completed batch.")
        else:
            print(f"Batch job {batch_id} status: {status}")

        self.pr()
        return self
