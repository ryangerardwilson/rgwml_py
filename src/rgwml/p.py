import pandas as pd
import os
import glob
import json
from datetime import datetime
import collections
import time
import argparse
import copy
import gc

class p:
    def __init__(self, df=None):
        """Initialize the EP class with an empty DataFrame."""
        self.df = df

    def cl(self):
        """[d.clo()] Clone."""
        gc.collect()
        return p(df=copy.deepcopy(self.df))

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
            for ext in parseable_extensions:
                files.extend(glob.glob(os.path.join(directory, f'*.{ext}')))

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

    def pr(self):
        """INSPECT::[d.pr()] Print."""
        if self.df is not None:
            # Print the DataFrame
            print(self.df)
        
            # Print all the column names with their data types in brackets
            columns_with_types = [f"{col} ({self.df[col].dtype})" for col in self.df.columns]
            print("Columns:", columns_with_types)
        else:
            raise ValueError("No DataFrame to print. Please load a file first using the frm or frml method.")

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
        """TINKER::[d.f('column1 > 100')] Filter."""
        if self.df is not None:
            self.df = self.df.query(filter_expr)
            self.pr()
        else:
            raise ValueError("No DataFrame to filter. Please load a file first using the frm or frml method.")

        gc.collect()
        return self


    def fim(self, mobile_col):
        """TINKER::[d.fim('mobile')] Filter Indian mobiles."""
        if self.df is not None:
            self.df = self.df[self.df[mobile_col].apply(lambda x: str(x).isdigit() and str(x).startswith(('6', '7', '8', '9')))]
            self.pr()
        else:
            raise ValueError("No DataFrame to filter. Please load a file first using the frm or frml method.")
        gc.collect()
        return self

    def fimc(self, mobile_col):
        """TINKER::[d.fimc('mobile')] Filter Indian mobiles (complement)."""
        if self.df is not None:
            self.df = self.df[~self.df[mobile_col].apply(lambda x: str(x).isdigit() and str(x).startswith(('6', '7', '8', '9')))]
            self.pr()
        else:
            raise ValueError("No DataFrame to filter. Please load a file first using the frm or frml method.")
        gc.collect()
        return self


    def g(self, groupby_cols, agg_func):
        """TRANSFORM::[d.g(['group_by_cols'], {'col7': 'sum'})] Group. Available agg options: sum, mean, min, max, count, size, std, var, median, etc."""
        if self.df is not None:
            self.df = self.df.groupby(groupby_cols).agg(agg_func)
            self.pr()
        else:
            raise ValueError("No DataFrame to group. Please load a file first using the frm or frml method.")
        gc.collect()
        return self


    def p(self, index, values, aggfunc='sum', seg_columns=None):
        """TRANSFORM::[d.p(['group_by_cols'], 'values_to_agg_col', 'sum', ['seg_columns'])] Pivot. Optional param: seg_columns. Available agg options: sum, mean, min, max, count, size, std, var, median, etc."""
        if self.df is not None:
            if seg_columns is not None:
                self.df = self.df.pivot_table(index=index, columns=seg_columns, values=values, aggfunc=aggfunc)
            else:
                self.df = self.df.pivot_table(index=index, values=values, aggfunc=aggfunc)
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
        """TINKER::[d.s('/filename/or/path')] Save the DataFrame as a CSV file on the desktop."""
        if self.df is None:
            raise ValueError("No DataFrame to save. Please load or create a DataFrame first.")

        # Ensure the file has a .csv extension
        if not name_or_path.lower().endswith('.csv'):
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

        # Save the DataFrame as a CSV file
        self.df.to_csv(full_path, index=False)
        print(f"DataFrame saved to {full_path}")
        gc.collect()
        return self


    def abc(self, condition, new_col_name):
        """APPEND::[d.abc('column1 > 30 and column2 < 50', 'age_above_30_and_below_50')] Append boolean classification column."""
        if self.df is not None:
            # Replace column names in the condition with DataFrame access syntax
            condition = condition.replace(' and ', ' & ').replace(' or ', ' | ')
            self.df[new_col_name] = self.df.eval(condition)
            self.pr()
        else:
            raise ValueError("No DataFrame to append a boolean column. Please load a file first using the frm or frml method.")
        return self


    def arc(self, ranges, target_col, new_col_name):
        """APPEND::[d.arc('0,500,1000,2000,5000,10000,100000,1000000', 'data_used', 'data_range')] Append ranged classification column."""
        if self.df is not None:
            range_list = [int(r) for r in ranges.split(',')]
            max_length = len(str(max(range_list)))
            labels = [f"{str(range_list[i]).zfill(max_length)} to {str(range_list[i+1]).zfill(max_length)}" for i in range(len(range_list) - 1)]
            self.df[new_col_name] = pd.cut(self.df[target_col], bins=range_list, labels=labels, right=False)
            self.pr()
        else:
            raise ValueError("No DataFrame to append a ranged classification column. Please load a file first using the frm or frml method.")
        return self

