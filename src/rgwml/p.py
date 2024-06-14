import pandas as pd
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

class p:
    def __init__(self, df=None):
        """Initialize the EP class with an empty DataFrame."""
        self.df = df

    def cl(self):
        """[d.clo()] Clone."""
        gc.collect()
        return p(df=copy.deepcopy(self.df))

    def frd(self, headers, data):
        """INSTANTIATE::[d.frd(['col1','col2'],[[1,2,3],[4,5,6]])] From raw data."""
        if isinstance(data, list) and all(isinstance(col, list) for col in data):
            self.df = pd.DataFrame(data={header: col for header, col in zip(headers, data)})
        else:
            raise ValueError("Data should be an array of arrays.")
        return self

    def fq(self, db_preset_name, query):
        """INSTANTIATE::[d.fq('preset_name','SELECT * FROM your_table')] From query."""

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

            # Run the query and get the results as a DataFrame
            query_job = client.query(query)
            result = query_job.result()

            # Convert the result to a pandas DataFrame
            df = result.to_dataframe()
            #columns = df.columns
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


    def fcq(self, db_preset_name, query, chunk_size):
        """INSTANTIATE::[d.fcq('preset_name', 'SELECT * FROM your_table', chunk_size)] From chunkable query."""

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
        """INSTANTIATE::[d.fp('/absolute/path')] From path."""
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
        """INSTANTIATE::[d.fd()] From directory."""
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
            first_n_rows = self.df.head(n).to_json(orient="records", indent=4)
            print(first_n_rows)
        else:
            raise ValueError("No DataFrame to display. Please load a file first using the frm or frml method.")
        
        gc.collect()
        return self

    def lnr(self, n):
        """INSPECT::[d.lnr('n')] Last n rows."""
        if self.df is not None:
            last_n_rows = self.df.tail(n).to_json(orient="records", indent=4)
            print(last_n_rows)
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
            columns_with_types = [f"{col} ({self.df[col].dtype})" for col in self.df.columns]
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

        if self.df is not None:
            # Create a copy of the DataFrame to avoid modifying the original
            df_copy = self.df.copy()

            # Step 1: Create new columns by duplicating the specified columns
            new_cols = []
            for agg_func in agg_funcs:
                col, func = agg_func.split('::')
                new_col_name = f'{col}_{func}'
                df_copy[new_col_name] = self.df[col]
                new_cols.append(new_col_name)

            # Step 2: Perform group-by and aggregations
            agg_dict = {}
            for agg_func in agg_funcs:
                col, func = agg_func.split('::')
                new_col_name = f'{col}_{func}'
                if func == 'css':
                    agg_dict[new_col_name] = css
                else:
                    agg_dict[new_col_name] = func

            grouped_df = df_copy.groupby(target_cols).agg(agg_dict).reset_index()

            self.df = grouped_df
            self.pr()
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

    def doc(self):
        """INSPECT::[d.doc()] Print the names and docstrings of all methods in the EP class."""

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
                    # Extract the method type from the first line
                    method_type = first_line.split("::")[0].strip()
                    method_description = first_line.split("::")[1].strip()
                    method_types[method_type].append((method, method_description))

        # Print the methods grouped by type in a tree-like format
        method_types_list = list(method_types.items())
        for i, (method_type, method_list) in enumerate(method_types_list):
            if i == len(method_types_list) - 1:
                branch = "└──"
                sub_branch = "    "
            else:
                branch = "├──"
                sub_branch = "│   "
            print(f"{branch} {method_type}")
            for j, (method, description) in enumerate(method_list):
                if j == len(method_list) - 1:
                    sub_branch_end = "└──"
                else:
                    sub_branch_end = "├──"
                print(f"{sub_branch}{sub_branch_end} {method}: {description}")
        
        gc.collect()
        return self

    def s(self, name_or_path):
        """TINKER::[d.s('/filename/or/path')] Save the DataFrame as a CSV or HDF5 file on the desktop."""
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

        # Convert datetime columns to datetime64[ns] without timezone
        for col in self.df.select_dtypes(include=["datetimetz", "datetime64"]).columns:
            self.df[col] = self.df[col].dt.tz_localize(None)

        # Convert other extension arrays to NumPy arrays
        for col in self.df.columns:
            if pd.api.types.is_extension_array_dtype(self.df[col]):
                self.df[col] = self.df[col].astype(object).astype(str)

        # Save the DataFrame as a CSV or HDF5 file
        if full_path.lower().endswith('.csv'):
            self.df.to_csv(full_path, index=False)
            print(f"DataFrame saved to {full_path}")
        elif full_path.lower().endswith('.h5'):
            self.df.to_hdf(full_path, key='df', mode='w')
            print(f"DataFrame saved to {full_path}")

        gc.collect()
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
        if self.df is not None:
            self.df = self.df.rename(columns=rename_pairs, inplace=True)
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
        """TINKER::[d.cs(['Column1', 'Column2'])]Cascade sort by specified columns."""
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
            train_rows = (ratios[0] * total_rows) // total_ratio
            validate_rows = (ratios[1] * total_rows) // total_ratio
            test_rows = total_rows - train_rows - validate_rows
            
            # Assign labels
            labels = ['TRAIN'] * train_rows + ['VALIDATE'] * validate_rows + ['TEST'] * test_rows
            
            # Append the labels to the DataFrame
            self.df['XGB_TYPE'] = labels
            self.pr()
        else:
            print("DataFrame is not initialized.")
        return self


    def axr(self, target_col, feature_cols, pred_col, boosting_rounds=100, model_path=None):
        """PREDICT::[d.axr('target_column','feature1, feature2, feature3','prediction_column_name')] Append XGB regression predictions. Assumes labelling by the .axl() method. Optional params: boosting_rounds (int), model_path (str)"""
        if self.df is not None and 'XGB_TYPE' in self.df.columns:
            # Convert feature_cols from string to list
            features = feature_cols.replace(' ', '').split(',')

            # Convert categorical columns to 'category' dtype
            for col in features:
                if self.df[col].dtype == 'object':
                    self.df[col] = self.df[col].astype('category')

            # Separate data into TRAIN, VALIDATE, and TEST sets
            train_data = self.df[self.df['XGB_TYPE'] == 'TRAIN']
            validate_data = self.df[self.df['XGB_TYPE'] == 'VALIDATE']
            test_data = self.df[self.df['XGB_TYPE'] == 'TEST']

            # Prepare DMatrix for XGBoost with enable_categorical parameter
            dtrain = xgb.DMatrix(train_data[features], label=train_data[target_col], enable_categorical=True)
            dvalidate = xgb.DMatrix(validate_data[features], label=validate_data[target_col], enable_categorical=True)
            dtest = xgb.DMatrix(test_data[features], enable_categorical=True)

            # Train the XGBoost model
            params = {
                'objective': 'reg:squarederror',
                'eval_metric': 'rmse'
            }
            evals = [(dtrain, 'train'), (dvalidate, 'validate')]
            model = xgb.train(params, dtrain, num_boost_round=boosting_rounds, evals=evals, early_stopping_rounds=10)

            # Make predictions
            self.df.loc[self.df['XGB_TYPE'] == 'TRAIN', pred_col] = model.predict(dtrain)
            self.df.loc[self.df['XGB_TYPE'] == 'VALIDATE', pred_col] = model.predict(dvalidate)
            self.df.loc[self.df['XGB_TYPE'] == 'TEST', pred_col] = model.predict(dtest)

            # Save the model if a path is provided
            if model_path:
                model.save_model(model_path)

            # Reorder columns
            columns_order = [col for col in self.df.columns if col not in ['XGB_TYPE', target_col, pred_col]] + ['XGB_TYPE', target_col, pred_col]
            self.df = self.df[columns_order]

            self.pr()
        else:
            print("DataFrame is not initialized or 'XGB_TYPE' column is missing.")
        return self


    def plc(self, y_axis_columns, x_axis_column=None, save_path=None):
        """INSPECT::[d.('Column1, Column2, Column3')] Plot line chart. Optional param: x_axis_column_name (str), image_save_path (str)"""
        y_axis_columns = y_axis_columns.replace(' ', '').split(',')
        plt.figure(figsize=(10, 6))

        if x_axis_column is None:
            x_data = self.df.index
            x_label = 'index'
        else:
            x_data = self.df[x_axis_column]
            x_label = x_axis_column

        for y_column in y_axis_columns:
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

        # Open the saved image
        img = Image.open(save_path)
        img.show()
        return self

