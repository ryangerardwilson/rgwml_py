RGWML

***By Ryan Gerard Wilson (https://ryangerardwilson.com)***

***Manipulate data with code that is less a golden retriever, and more a Samurai's sword***

1. Install
----------
    
    sudo apt update
    sudo apt install ffmpeg
    pip3 install --upgrade rgwml

2. Import & Load Data
---------------------

    import rgwml as r

    # For 99% use cases a Pandas df is good enough
    d1 = r.p()
    d1.fp('/path/to/your/file')

    # For the remaining 1%
    d2 = r.d()
    d2.fp('/path/to/your/file')
    
3. Create a rgwml.config file
-----------------------------

An rgwml.config file is required for MSSQL, CLICKHOUSE, MYSQL, GOOGLE BIG QUERY, OPEN AI, NETLIFY and VERCEL integrations. It allows you to namespace your db connections, so you can query like this:

    import rgwml as r
    d = r.p()
    d.fq('mysql_db2','SELECT * FROM your_table')

Set out below is the format of a rgwml.config file. Place it anywhere in your Desktop, Downloads or Documents directories.

    {
      "db_presets" : [
        {
          "name": "mssql_db9",
          "db_type": "mssql",
          "host": "",
          "username": "",
          "password": "",
          "database": ""
        },
        {
          "name": "clickhouse_db7",
          "db_type": "clickhouse",
          "host": "",
          "username": "",
          "password": "",
          "database": ""
        },
        {
          "name": "mysql_db2",
          "db_type": "mysql",
          "host": "",
          "username": "",
          "password": "",
          "database": ""
        },
        {
          "name": "bq_db1",
          "db_type": "google_big_query",
          "json_file_path": "",
          "project_id": ""
        }
      ],
    "vm_presets": [
        {
          "name": "main_server",
          "host": "",
          "ssh_user": "",
          "ssh_key_path": ""
        }
      ],
    "cloud_storage_presets": [
        {
          "name": "gcs_bucket_name",
          "credential_path": "path/to/your/credentials.json"
        }
      ],
    "open_ai_key": "",
    "netlify_token": "",
    "vercel_token": ""
  }

4. `r.p()` Class Methods
------------------------

Instantiate this class by `d = r.p()`

### 4.1. LOAD

    # From raw data
    d.frd(['col1','col2'],[[1,2,3],[4,5,6]])

    # From path
    d.fp('/absolute/path')

    # From Directory (select from your last 7 recently modified files in your Desktop/Downloads/Documents directories)
    d.fd()

    # From query
    d.fq('rgwml_config_db_preset_name','SELECT * FROM your_table')

    # FROM chunkable query
    d.fcq('rgwml_config_db_preset_name', 'SELECT * FROM your_table', chunk_size)
    
### 4.2. INSPECT

    # Describe
    d.d()

    # Print
    d.pr()

    # First n rows
    d.fnr('n')

    # Last n rows
    d.lnr('n')

    # Top n unique values for specified columns
    d.tnuv(n, ['col1', 'col2'])

    # Bottom n unique values for specified columns
    d.bnuv(n, ['col1', 'col2'])

    # Is empty. Returns boolean, not chainable.
    d.ie()

    # Memory usage print.
    d.mem()

    # Print correlation
    d.prc([('column1','column2'), ('column3','column4')])

    # Print n frequency linear. Optional: order_by (str), which has options: ASC, DESC, FREQ_ASC, FREQ_DESC (default)
    d.pnfl(5,'Column1,Columns')

    # Print n frequency cascading. Optional: order_by (str), which has options: ASC, DESC, FREQ_ASC, FREQ_DESC (default)
    d.pnfc(5,'Column1,Columns')

### 4.3. APPEND

    # Append boolean classification column
    d.abc('column1 > 30 and column2 < 50', 'new_column_name')

    # Append DBSCAN cluster column. Optional: visualize (boolean)
    d.adbscancc('Column1,Column2', 'new_cluster_column_name', eps=0.5, min_samples=5, visualize=True)

    # Append n-cluster column. Available operations: KMEANS/ AGGLOMERATIVE/ MEAN_SHIFT/ GMM/ SPECTRAL/ BIRCH. Optional: visualize (boolean), n_cluster_finding_method (str) i.e. ELBOW/ SILHOUETTE/ FIXED:n (specify a number of n clusters).
    d.ancc('Column1,Column2', 'KMEANS', 'new_cluster_column_name', n_clusters_finding_method='FIXED:5', visualize=True)

    # Append percentile classification column
    d.apc('0,25,50,75,100', 'column_to_be_analyzed', 'new_column_name')

    # Append ranged classification column
    d.arc('0,500,1000,2000,5000,10000,100000,1000000', 'column_to_be_analyzed', 'new_column_name')

    # Append ranged date classification column
    d.ardc('2024-01-01,2024-02-01,2024-03-01', 'date_column', 'new_date_classification')    

    # Append count of timestamps after reference time. Requires values in YYYY-MM-DD or YYYY-MM-DD HH:MM:SS format.
    d.atcar('comma_separated_timestamps_column', 'reference_date_or_timestamps_column', 'new_column_count_after_reference')

    # Append count of timestamps before reference time. Requires values in YYYY-MM-DD or YYYY-MM-DD HH:MM:SS format.
    d.atcbr('comma_separated_timestamps_column', 'reference_date_or_timestamp_column', 'new_column_count_before_reference')

### 4.4. DOCUMENTATION

    # Prints docs. Optional parameter: method_type_filter (str) egs. 'APPEND, PLOT'
    d.doc()

### 4.5. JOINS

    # Union join
    d.uj(d2)

    # Bag union join
    d.buj(d2)

    # Left join
    d.lj(d2,'table_a_id','table_b_id')

    # Right join
    d.rj(d2,'table_a_id','table_b_id')

### 4.6. PERSIST

    # Save (saves as csv (default) or h5, to desktop (default) or path)
    d.s('/filename/or/path')
    d.s() #If the dataframe was loaded from a source with an absolute path, calling the s method without an argument will save at the same path

### 4.7. PLOT

    # Plot correlation heatmap for the specified columns. Optional param: image_save_path (str)
    d.pcr(y='Column1, Column2, Column3')

    # Plot distribution histograms for the specified columns. Optional param: image_save_path (str)
    d.pdist(y='Column1, Column2, Column3')

    # Plot line chart. Optional param: x (str), i.e. a single column name for the x axis eg. 'Column5', image_save_path (str)
    d.plc(y='Column1, Column2, Column3')

    # Plot Q-Q plots for the specified columns. Optional param: image_save_path (str)
    d.pqq(y='Column1, Column2, Column3')

### 4.8. PREDICT

    # Append XGB training labels based on a ratio string. Specify a ratio a:b:c to split into TRAIN, VALIDATE and TEST, or a:b to split into TRAIN and TEST.
    d.axl('70:20:10')

    # Append XGB regression predictions. Assumes labelling by the .axl() method. Optional params: boosting_rounds (int), model_path (str)
    d.axlinr('target_column','feature1, feature2, feature3','prediction_column_name')

    # Append XGB logistic regression predictions. Assumes labeling by the .axl() method. Optional params: boosting_rounds (int), model_path (str)
    d.axlogr('target_column','feature1, feature2, feature3','prediction_column_name')

### 4.9. TINKER

    # Cascade sort by specified columns.
    d.cs(['Column1', 'Column2'])

    # Filter
    d.f("col1 > 100 and Col1 == Col3 and Col5 == 'XYZ'")

    # Filter Indian Mobiles
    d.fim('mobile')

    # Filter Indian Mobiles (complement)
    d.fimc('mobile')

    # Make numerically parseable by defaulting to zero for specified column
    d.mnpdz(['Column1', Column2])

    # Rename columns
    d.rnc({'old_col1': 'new_col1', 'old_col2': 'new_col2'})

### 4.10. TRANSFORM

    # Group. Permits multiple aggregations on the same column. Available agg options: sum, mean, min, max, count, size, std, var, median, css (comma-separated strings), etc.
    d.(['group_by_columns'], ['column1::sum', 'column1::count', 'column3::sum'])

    # Pivot. Optional param: seg_columns. Available agg options: sum, mean, min, max, count, size, std, var, median, etc.
    d.p(['group_by_cols'], 'values_to_agg_col', 'sum', ['seg_columns'])

5. `r.d()` Methods
------------------

Instantiate this class by `d = r.d()`

### 5.1. LOAD

    # From raw data
    d.frd(['col1','col2'],[[1,2,3],[4,5,6]])

    # From path
    d.fp('/absolute/path')

### 5.2. INSPECT
    
    # Print
    d.pr()

### 5.3. DOCUMENTATION

    # Prints docs. Optional parameter: method_type_filter (str) egs. 'APPEND, PLOT'
    d.doc()

### 5.4. JOINS

    # Union join
    d.uj(d2)

### 5.5. PERSIST
    
    # Save (saves as csv (default) or h5, to desktop (default) or path)
    d.s('/filename/or/path')

### 5.6. TINKER
    
    # Filter Indian Mobiles
    d.fim('mobile')

    # Filter Indian Mobiles (complement)
    d.fimc('mobile')

### 5.7. TRANSFORM

    # Group. Permits multiple aggregations on the same column. Available agg options: sum, mean, min, max, count, size, std, var, median, css (comma-separated strings), etc.
    d.(['group_by_columns'], ['column1::sum', 'column1::count', 'column3::sum'])

    # Pivot. Optional param: seg_columns. Available agg options: sum, mean, min, max, count, size, std, var, median, etc.
    d.p(['group_by_cols'], 'values_to_agg_col', 'sum', ['seg_columns'])

