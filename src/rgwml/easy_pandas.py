import pandas as pd
import os
import glob
import json
from datetime import datetime
import collections
import time
import argparse

class EP:
    def __init__(self):
        """Initialize the EP class with an empty DataFrame."""
        self.df = None

    def frm(self, file_path):
        """INSTANTIATE::Load a DataFrame from a file."""
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
            while True:
                key = input("Enter the key for the HDF5 dataset (or leave blank to list available keys): ")
                try:
                    if key:
                        self.df = pd.read_hdf(file_path, key=key)
                    else:
                        # List available keys if no key is provided
                        with pd.HDFStore(file_path) as store:
                            print("Available keys:", store.keys())
                            continue
                    break
                except KeyError as e:
                    print(f"KeyError: {e}. Please try again.")
        elif file_extension == 'feather':
            self.df = pd.read_feather(file_path)
        elif file_extension == 'pkl':
            self.df = pd.read_pickle(file_path)
        else:
            raise ValueError(f"Unsupported file extension: {file_extension}")
        
        self.p()
        return self

    def frml(self):
        """INSTANTIATE::List and select files to load a DataFrame from the Desktop, Downloads, and Documents directories."""
        directories = [os.path.expanduser(f"~/{folder}") for folder in ["Desktop", "Downloads", "Documents"]]
        parseable_extensions = ['csv', 'xls', 'xlsx', 'json', 'parquet', 'h5', 'hdf5', 'feather', 'pkl']

        files = []
        for directory in directories:
            for ext in parseable_extensions:
                files.extend(glob.glob(os.path.join(directory, f'*.{ext}')))

        if not files:
            print("No parseable files found in the specified directories.")
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
                choice = int(input("Choose a number corresponding to the file you want to load: "))
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
                        while True:
                            key = input("Enter the key for the HDF5 dataset (or leave blank to list available keys): ")
                            try:
                                if key:
                                    self.df = pd.read_hdf(file_path, key=key)
                                else:
                                    # List available keys if no key is provided
                                    with pd.HDFStore(file_path) as store:
                                        print("Available keys:", store.keys())
                                        continue
                                break
                            except KeyError as e:
                                print(f"KeyError: {e}. Please try again.")
                    elif file_extension == 'feather':
                        self.df = pd.read_feather(file_path)
                    elif file_extension == 'pkl':
                        self.df = pd.read_pickle(file_path)
                    else:
                        raise ValueError(f"Unsupported file extension: {file_extension}")
                    
                    self.p()
                    break
                else:
                    print("Invalid choice. Please choose a valid number between 1 and 7.")
            except ValueError:
                print("Invalid input. Please enter a number.")

        return self

    def d(self):
        """INSPECT::Print the descriptive statistics of the DataFrame."""
        if self.df is not None:
            description = self.df.describe()
            print(description)
            self.df = description
        else:
            raise ValueError("No DataFrame to describe. Please load a file first using the frm or frml method.")
        
        return self

    def fnr(self, n):
        """INSPECT::Print the first n rows of the DataFrame in pretty JSON format."""
        if self.df is not None:
            first_n_rows = self.df.head(n).to_json(orient="records", indent=4)
            print(first_n_rows)
        else:
            raise ValueError("No DataFrame to display. Please load a file first using the frm or frml method.")
        
        return self

    def lnr(self, n):
        """INSPECT::Print the last n rows of the DataFrame in pretty JSON format."""
        if self.df is not None:
            last_n_rows = self.df.tail(n).to_json(orient="records", indent=4)
            print(last_n_rows)
        else:
            raise ValueError("No DataFrame to display. Please load a file first using the frm or frml method.")
        
        return self

    def p(self):
        """
        INSPECT::Print the DataFrame, its memory usage, and its column names.
        
        Example:
            >>> ep = EP()
            >>> data = {'A': [1, 2], 'B': [3, 4]}
            >>> ep.df = pd.DataFrame(data)
            >>> ep.p()
               A  B
            0  1  3
            1  2  4
            Memory usage of DataFrame: 0.00 MB
            Columns: ['A', 'B']
            <easy_pandas.EP object at ...>
        """
        if self.df is not None:
            # Print the DataFrame
            print(self.df)
            
            # Print the size of the DataFrame in memory
            memory_usage = self.df.memory_usage(deep=True).sum() / (1024 * 1024)  # Convert bytes to MB
            print(f"Memory usage of DataFrame: {memory_usage:.2f} MB")
            
            # Print all the column names
            print("Columns:", self.df.columns.tolist())
        else:
            raise ValueError("No DataFrame to print. Please load a file first using the frm or frml method.")

        return self

    def ft(self, filter_expr):
        """
        TINKER::Filter the DataFrame using a pandas-like query expression and print the result.
        Example:
            >>> ep = EP()
            >>> data = {'A': [1, 2, 3, 4], 'B': [5, 6, 7, 8], 'C': ['foo', 'bar', 'baz', 'qux']}
            >>> ep.df = pd.DataFrame(data)
            >>> ep.ft("A > 2")
               A  B    C
            2  3  7  baz
            3  4  8  qux
            Memory usage of DataFrame: 0.00 MB
            Columns: ['A', 'B', 'C']
            <easy_pandas.EP object at ...>
        """
        if self.df is not None:
            self.df = self.df.query(filter_expr)
            self.p()
        else:
            raise ValueError("No DataFrame to filter. Please load a file first using the frm or frml method.")

        return self

    def fi(self, filter_expr):
        """
        INSPECT::Filter the DataFrame using a pandas-like query expression, print the result, and return the original object.
        
        Example:
            >>> ep = EP()
            >>> data = {'A': [1, 2, 3, 4], 'B': [5, 6, 7, 8], 'C': ['foo', 'bar', 'baz', 'qux']}
            >>> ep.df = pd.DataFrame(data)
            >>> ep.fi("A > 2")
               A  B    C
            2  3  7  baz
            3  4  8  qux
            Memory usage of DataFrame: 0.00 MB
            Columns: ['A', 'B', 'C']
            <easy_pandas.EP object at ...>
        """
        if self.df is not None:
            # Create a copy of the original DataFrame
            original_df = self.df.copy()
            
            # Filter the DataFrame
            self.df = self.df.query(filter_expr)
            
            # Print the filtered DataFrame
            self.p()
            
            # Restore the original DataFrame
            self.df = original_df
        else:
            raise ValueError("No DataFrame to filter. Please load a file first using the frm or frml method.")

        return self


    def doc(self):
        """Print the names and docstrings of all methods in the EP class."""

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

        return self



