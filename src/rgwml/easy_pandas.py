import pandas as pd
import os
import glob
import json
from datetime import datetime

class EP:
    def __init__(self):
        """Initialize the EP class with an empty DataFrame."""
        self.df = None

    def frm(self, file_path):
        """
        Load a DataFrame from a file.
        
        Parameters:
        file_path (str): Path to the file to load the DataFrame from.
        
        Returns:
        self: EP instance with the loaded DataFrame.
        """
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
        """
        List and select files to load a DataFrame from the Desktop, Downloads, and Documents directories.
        
        Returns:
        self: EP instance with the loaded DataFrame.
        """
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
        """
        Print the descriptive statistics of the DataFrame.
        
        Returns:
        self: EP instance with the updated DataFrame containing the descriptive statistics.
        """
        if self.df is not None:
            description = self.df.describe()
            print(description)
            self.df = description
        else:
            raise ValueError("No DataFrame to describe. Please load a file first using the frm or frml method.")
        
        return self

    def fnr(self, n):
        """
        Print the first n rows of the DataFrame in pretty JSON format.
        
        Parameters:
        n (int): Number of rows to display.
        
        Returns:
        self: EP instance.
        """
        if self.df is not None:
            first_n_rows = self.df.head(n).to_json(orient="records", indent=4)
            print(first_n_rows)
        else:
            raise ValueError("No DataFrame to display. Please load a file first using the frm or frml method.")
        
        return self

    def lnr(self, n):
        """
        Print the last n rows of the DataFrame in pretty JSON format.
        
        Parameters:
        n (int): Number of rows to display.
        
        Returns:
        self: EP instance.
        """
        if self.df is not None:
            last_n_rows = self.df.tail(n).to_json(orient="records", indent=4)
            print(last_n_rows)
        else:
            raise ValueError("No DataFrame to display. Please load a file first using the frm or frml method.")
        
        return self

    def p(self):
        """
        Print the DataFrame, its memory usage, and its column names.
        
        Returns:
        self: EP instance.
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

    def f(self, filter_expr):
        """
        Filter the DataFrame using a pandas-like query expression and print the result.
        
        Parameters:
        filter_expr (str): Pandas query expression.
        
        Returns:
        self: EP instance with the filtered DataFrame.
        """
        if self.df is not None:
            self.df = self.df.query(filter_expr)
            self.p()
        else:
            raise ValueError("No DataFrame to filter. Please load a file first using the frm or frml method.")

        return self

    def doc(self):
        """
        Print the names and docstrings of all methods in the EP class.
        
        Returns:
        self: EP instance.
        """
        methods = [method for method in dir(self) if callable(getattr(self, method)) and not method.startswith("__")]
        for method in methods:
            method_func = getattr(self, method)
            docstring = method_func.__doc__
            print(f"{method}:\n{docstring}\n")
        return self



# Example usage:
# ep = EP().frml().d().fnr(5).lnr(5)
# print(ep.df)

