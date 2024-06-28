import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd  # Importing pandas for HDF5 support
import gc
import os
import copy
import collections

class d:
    def __init__(self, df=None):
        """Initialize the class with an empty Dask DataFrame."""
        self.df = df

    def cl(self):
        """[d.cl()] Clone."""
        gc.collect()
        return d(df=copy.deepcopy(self.df))

    def frd(self, headers, data):
        """LOAD::[d.frd(['col1','col2'],[[1,2,3],[4,5,6]])] From raw data."""
        if isinstance(data, list) and all(isinstance(row, list) for row in data):
            self.df = dd.from_pandas(pd.DataFrame(data, columns=headers), npartitions=1)
        else:
            raise ValueError("Data should be an array of arrays.")
        return self

    def fp(self, file_path):
        """LOAD::[d.fp('/absolute/path')] From path."""
        file_extension = file_path.split('.')[-1]

        if file_extension == 'csv':
            self.df = dd.read_csv(file_path)
        elif file_extension in ['xls', 'xlsx']:
            self.df = dd.read_excel(file_path)
        elif file_extension == 'json':
            self.df = dd.read_json(file_path, orient='records', lines=True)
        elif file_extension == 'parquet':
            self.df = dd.read_parquet(file_path)
        elif file_extension in ['h5', 'hdf5']:
            self.df = dd.read_hdf(file_path, key='/*')
        elif file_extension == 'feather':
            self.df = dd.read_feather(file_path)
        elif file_extension == 'pkl':
            self.df = dd.from_pandas(pd.read_pickle(file_path), npartitions=1)
        else:
            raise ValueError(f"Unsupported file extension: {file_extension}")

        self.pr()
        gc.collect()
        return self

    def rnc(self, rename_pairs):
        """TINKER::[d.rnc({'old_col1': 'new_col1', 'old_col2': 'new_col2'})] Rename columns."""
        if self.df is not None:
            self.df = self.df.rename(columns=rename_pairs)
            self.pr()
        else:
            raise ValueError("No DataFrame to rename columns. Please load a file first using the frm or frml method.")
        gc.collect()
        return self

    def pr(self):
        """INSPECT::[d.pr()] Print."""
        if self.df is not None:
            print(self.df.compute())
            columns_with_types = [f"{col} ({self.df[col].dtype})" for col in self.df.columns]
            print("Columns:", columns_with_types)
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
            except Exception as e:
                print(f"Query method failed: {e}, falling back to eval method.")
                # Fallback to eval for more complex expressions
                self.df = self.df[self.df.eval(filter_expr)]
            self.pr()
        else:
            raise ValueError("No DataFrame to filter. Please load a file first using the frm or frml method.")
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

        # Convert datetime columns to timezone-naive (remove timezone information)
        for col in self.df.select_dtypes(include=["datetimetz", "datetime64"]).columns:
            self.df[col] = self.df[col].dt.tz_localize(None)

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

        # Convert other extension arrays to strings
        for col in self.df.columns:
            if pd.api.types.is_extension_array_dtype(self.df[col]):
                self.df[col] = self.df[col].astype(str)

        # Save the DataFrame as a CSV or HDF5 file
        if full_path.lower().endswith('.csv'):
            self.df.to_csv(full_path, single_file=True, index=False)
            print(f"DataFrame saved to {full_path}")
        elif full_path.lower().endswith('.h5'):
            # For Dask, converting to Pandas DataFrame for HDF5 saving
            dask_df.compute().to_hdf(full_path, key='df', mode='w', format='table')
            #pandas_df = self.df.compute()
            #pandas_df.to_hdf(full_path, key='df', mode='w', format='table')
            print(f"DataFrame saved to {full_path}")

        gc.collect()
        return self

    def uj(self, other):
        """JOINS::[d.uj(d2)] Union join."""
        if not isinstance(other, d):
            raise TypeError("The 'other' parameter must be an instance of the d class")

        if self.df is None and other.df is None:
            raise ValueError("Both instances must contain a DataFrame or be empty")

        if self.df is None or self.df.compute().empty:
            self.df = other.df
        elif other.df is None or other.df.compute().empty:
            pass
        else:
            if set(self.df.columns) != set(other.df.columns):
                raise ValueError("Both DataFrames must have the same columns for a union join")

            self.df = dd.concat([self.df, other.df]).drop_duplicates()

        self.pr()
        gc.collect()
        return self

    def buj(self, other):
        """JOINS::[d.buj(d2)] Bag union join without dropping duplicates."""
        if not isinstance(other, d):
            raise TypeError("The 'other' parameter must be an instance of the d class")

        if self.df is None and other.df is None:
            raise ValueError("Both instances must contain a DataFrame or be empty")

        if self.df is None or self.df.compute().empty:
            self.df = other.df
        elif other.df is None or other.df.compute().empty:
            pass
        else:
            if set(self.df.columns) != set(other.df.columns):
                raise ValueError("Both DataFrames must have the same columns for a union join")

            self.df = dd.concat([self.df, other.df])

        self.pr()
        gc.collect()
        return self

    def fim(self, mobile_col):
        """TINKER::[d.fim('mobile')] Filter Indian mobiles."""
        if self.df is not None:
            self.df = self.df[self.df[mobile_col].apply(
                lambda x: str(x).isdigit() and
                          str(x).startswith(('6', '7', '8', '9')) and
                          len(set(str(x))) >= 4,
                meta=(mobile_col, 'bool'))]
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
                          len(set(str(x))) >= 4,
                meta=(mobile_col, 'bool'))]
            self.pr()
        else:
            raise ValueError("No DataFrame to filter. Please load a file first using the frm or frml method.")
        gc.collect()
        return self

    def g(self, target_cols, agg_funcs):
        """TRANSFORM::[d.(['group_by_columns'], ['column1::sum', 'column1::count', 'column3::sum'])] Group. Permits multiple aggregations on the same column. Available agg options: sum, mean, min, max, count, size, std, var, median, css (comma-separated strings), etc. Warning: When using MEAN, its essential that the column values are such that they avoid division by zero errors"""
        
        def css(series):
            if series.empty or series.isnull().all():
                return ''
            return ','.join(series.dropna().astype(str))

        def css_unique(series):
            if series.empty or series.isnull().all():
                return ''
            return ','.join(pd.Series(series.dropna().astype(str)).drop_duplicates().tolist())

        def css_granular_unique(series):
            if series.empty or series.isnull().all():
                return ''
            all_values = ','.join(series.dropna().astype(str)).split(',')
            unique_values = pd.Series(all_values).drop_duplicates().tolist()
            return ','.join(unique_values)

        def count_granular_unique(series):
            if series.empty or series.isnull().all():
                return 0
            all_values = ','.join(series.dropna().astype(str)).split(',')
            unique_values = pd.Series(all_values).drop_duplicates()
            return unique_values.count()

        if self.df is not None:
            df_copy = self.df.copy()

            # Internal preprocessing: Convert columns for numerical aggregation (excluding 'css' and 'count') to floats
            for agg_func in agg_funcs:
                col, func = agg_func.split('::')
                if func not in ['css', 'count', 'css_unique', 'count_unique', 'css_granular_unique', 'count_granular_unique']:
                    # Check if column exists and can be converted to float
                    if col in df_copy.columns:
                        try:
                            # Convert to numeric, setting errors='coerce' to handle non-numeric values
                            df_copy[col] = df_copy[col].map_partitions(pd.to_numeric, errors='coerce')
                        except Exception as e:
                            raise ValueError(f"Error converting column '{col}' to numeric: {e}")

            agg_results = []
            css_results = []
            for agg_func in agg_funcs:
                col, func = agg_func.split('::')
                new_col_name = f'{col}_{func}'
                df_copy[new_col_name] = self.df[col]

                if func in ['css', 'css_unique', 'css_granular_unique']:
                    if func == 'css':
                        css_func = css
                    elif func == 'css_unique':
                        css_func = css_unique
                    else:
                        css_func = css_granular_unique

                    df_pandas_copy = df_copy.compute()
                    css_grouped_pandas_step = df_pandas_copy.groupby(target_cols)[new_col_name].apply(css_func).reset_index()
                    css_results.append(css_grouped_pandas_step)
                    continue

                if func == 'count_unique':
                    result = df_copy.groupby(target_cols)[new_col_name].nunique().reset_index()
                elif func == 'count_granular_unique':
                    df_pandas_copy = df_copy.compute()
                    result = df_pandas_copy.groupby(target_cols)[new_col_name].apply(count_granular_unique).reset_index()
                    result.columns = target_cols + [new_col_name]
                else:
                    # Use .agg() method instead of direct aggregation functions
                    result = df_copy.groupby(target_cols)[new_col_name].agg({new_col_name: func}).reset_index()
                agg_results.append(result)

            if agg_results:
                combined_df = agg_results[0]
                for res in agg_results[1:]:
                    combined_df = combined_df.merge(res, on=target_cols, how='outer')
            else:
                combined_df = None

            if css_results:
                css_combined = pd.concat(css_results, axis=1).loc[:, ~pd.concat(css_results, axis=1).columns.duplicated()]
                css_combined = dd.from_pandas(css_combined, npartitions=1)

                if combined_df is not None:
                    combined_df = combined_df.merge(css_combined, on=target_cols, how='outer')
                else:
                    combined_df = css_combined

            self.df = combined_df
            self.pr()
        else:
            raise ValueError("No DataFrame to transform. Please load a file first.")

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

    def rtc(self, columns_to_retain):
        """TINKER::[d.rtc(['column1','column2'])] Retain columns specified and drop the rest."""
        if not isinstance(columns_to_retain, list):
            raise ValueError("columns_to_retain should be a list of column names.")
        self.df = self.df[columns_to_retain]
        return self

    def mad(self, other_d, column_name):
        """MASK::[d.mad(mask_d_instance, 'column7')] Mask against DataFrame, retaining only rows with common column values. Only rows where for the mutual column 7, values exist in mask_d_instance, will be retained"""
        if not isinstance(other_d, d):
            raise ValueError("other_d should be an instance of the class d.")
        if column_name not in self.df.columns or column_name not in other_d.df.columns:
            raise ValueError("The specified column must exist in both DataFrames.")
        self.df = self.df.merge(other_d.df[[column_name]].drop_duplicates(), on=column_name, how='inner')
        return self

