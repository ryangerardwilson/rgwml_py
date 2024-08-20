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

6. `r.f()` Methods
------------------

Serves a CRM with a Scaffolded MYSQL DB, Backend (i.e., a Bottle App on your GCS VM), a NextJS Web Frontend, and a Flutter Android App, on your local machine with highly customized options based on modal definitions. Note that all modal names, modal column names, and option values must NOT contain spaces and should be separated by underscores.

#### 6.1 Instantiation

    import rgwml as r
    crm = r.f()

#### 6.2 Envirnoment

Set up your environment with the necessary dependencies to use this feature:
 
- Install the latest version of NodeJS (https://nodejs.org/en/download/package-manager). Typical steps would include the below. However, make sure the restart your terminals before you verify the installations.

    # installs nvm (Node Version Manager)
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash

    # download and install Node.js (you may need to restart the terminal)
    nvm install 20

    # verifies the right Node.js version is in the environment
    node -v # should print `v20.16.0`

    # verifies the right npm version is in the environment
    npm -v # should print `10.8.1`

- Install Android Studio (via https://developer.android.com/studio) and Flutter (via the Snap Store)

- Ensure that your rgwml.config sets out details of the VM being used, as well as your VERCEL and NETLIFY tokens

#### 6.3 Backend Config Syntax (`modal_backend_config`)

- Ensure that queries in the read routes return columns in the same order as a simple SELECT * FROM `your_table` query
- Keep the `read_routes` queries simple by avoiding adding columns from other tables, and returning only the columns present in the corresponding modal. 

#### 6.4 Frontend Config Syntax (`modal_frontend_config`)

- Keep the order of the READ permissions the same as the order of a simple SELECT * FROM `your_table` query with respect to the concerned modal.
- Available validation rules: `REQUIRED, CHAR_LENGTH:X, IS_NUMERICALLY_PARSEABLE, IS_INDIAN_MOBILE_NUMBER, IS_YYYY-MM-DD, IS_AFTER_TODAY, IS_BEFORE_TODAY`

#### 6.5 Example

    project_name = "crm"
    db_name = "crm"

    modal_backend_config = {
        "sudo": {
            "username": "sudo",
            "password": "sudo"
            },
        "modals": {
            "social_media_escalations": {
                "columns": "url[VARCHAR(2048)],post_text,post_author,star_rating,forum,mobile,ai_sentiment,ai_category,ai_author_type,reach_out_method,action_taken,comment[VARCHAR(1000)],follow_up_date",
                "read_routes": [
                    {"a-most-recent-500": "SELECT id, user_id, url, post_text, post_author, star_rating, forum, mobile, ai_sentiment, ai_category, ai_author_type, reach_out_method, action_taken, comment, follow_up_date, CONVERT_TZ(created_at, '+00:00', '+05:30') AS created_at, CONVERT_TZ(updated_at, '+00:00', '+05:30') AS updated_at FROM cx_crm.social_media_escalations ORDER BY id DESC LIMIT 500""}
                    ]
                },
            "welcome_calls": {
                "columns": "mobile,name,city,nasid,device_id,plan_name,plan_amount,payment_mode,internet_state,customer_tenure_days,last_ping_time,device_type,data_usage_rng,priority,disposition,issue,sub_issue,alternate_number,comment[VARCHAR(1000)]",
                "read_routes": [
                    {"a-unprosecuted": "SELECT id, user_id, mobile, name, city, nasid, device_id, plan_name, plan_amount, payment_mode, internet_state, customer_tenure_days, last_ping_time, device_type, data_usage_rng, priority, disposition, issue, sub_issue, alternate_number, comment, CONVERT_TZ(created_at, '+00:00', '+05:30') AS created_at, CONVERT_TZ(updated_at, '+00:00', '+05:30') AS updated_at FROM cx_crm.welcome_calls WHERE disposition != 'wc_completed' OR disposition IS NULL ORDER BY priority ASC"},
                    {"b-prosecuted": "SELECT id, user_id, mobile, name, city, nasid, device_id, plan_name, plan_amount, payment_mode, internet_state, customer_tenure_days, last_ping_time, device_type, data_usage_rng, priority, disposition, issue, sub_issue, alternate_number, comment, CONVERT_TZ(created_at, '+00:00', '+05:30') AS created_at, CONVERT_TZ(updated_at, '+00:00', '+05:30') AS updated_at FROM cx_crm.welcome_calls WHERE disposition = 'wc_completed' ORDER BY updated_at DESC"}
                    ]
                },
            }
        }

    modal_frontend_config = {
        "social_media_escalations": {
            "options": {
                "forum[XOR]": ["playstore", "google_review", "freshdesk", "mail_to_management", "facebook", "instagram", "twitter","linkedin","other"],
                "reach_out_method[XOR]": ["post", "comment", "dm","other"],
                "action_taken[XOR]": ["no_customer_detail_found", "resolved", "post_removed_and_resolved", "other"]
            },
            "conditional_options": {},
            "scopes": {
                "create": True,
                "read": ["username","id","url","post_text","post_author","star_rating","forum","mobile","ai_sentiment","ai_category","ai_author_type","reach_out_method","action_taken","comment","follow_up_date","created_at","updated_at"],
                "update": ["url","post_text","post_author","star_rating","forum","mobile","reach_out_method","action_taken","comment","follow_up_date"],
                "delete": True
            },
            "validation_rules": {
                "post_text": ["REQUIRED"],
                "post_author": ["REQUIRED"],
                "forum": ["REQUIRED"],
                "reach_out_method": ["REQUIRED"],
                "action_taken": ["REQUIRED"],
                "mobile": ["IS_INDIAN_MOBILE_NUMBER"],
                "follow_up_date": ["IS_AFTER_TODAY"]
            },
            "ai_quality_checks": {},
        },
        "welcome_calls": {
            "options": {
                "disposition[XOR]": ["wc_completed", "dnp", "asked_to_call_back", "call_disconnected_in_between"],
                "issue[XOR]": ["no_issue", "internet_issue", "misbehave", "not_proper_install", "other_issue"]
            },
            "conditional_options": {
                "sub_issue": [
                    {
                        "condition": "issue == no_issue",
                        "options": ["happy","neutral","unhappy"]
                    },
                    {
                        "condition": "issue == internet_issue",
                        "options": ["slow_speed","range","frequent_disconnect","internet_down","slow_speed_and_frequent_disconnect","other"]
                    },
                    {
                        "condition": "issue == misbehave",
                        "options": ["rude_behaviour","fake_installation","disintermediation","false_promises","took_extra_cash","demanded_extra_cash","other"]
                    },
                    {
                        "condition": "issue == not_proper_install",
                        "options": ["untidy_wiring","wrong_positioning","other"]
                    },
                    {
                        "condition": "issue == other_issue",
                        "options": ["other_issue"]
                    }
                ]
            },
            "scopes": {
                "create": False,
                "read": ["id","user_id","mobile","name","device_id","plan_name","plan_amount","internet_state","customer_tenure_days","last_ping_time","device_type","data_usage_rng","priority","disposition","issue","sub_issue","alternate_number","comment","created_at","updated_at"],
                "update": ["disposition","issue","sub_issue","alternate_number","comment"],
                "delete": False
            },
            "validation_rules": {
                "disposition": ["REQUIRED"],
                "issue": ["REQUIRED"],
                "sub_issue": ["REQUIRED"],
                "alternate_number": ["IS_INDIAN_MOBILE_NUMBER"],
                "comment": ["REQUIRED"],
            },
            "ai_quality_checks": {
                "comment": ["the text should not be gibberish"]
            },
        }
    }

    crm = r.f()
    crm.ser(
        project_name= project_name,
        new_db_name= db_name,                                           # A new db will be created if this does not already exist
        db_preset_name='your_rgwml_mysql_db_preset_name',
        vm_preset_name='your_server_preset_name',
        cloud_storage_preset_name='your_cloud_storage_preset_name',
        cloud_storage_bucket_name='your_cloud_storage_bucket_name',     # A new bucket will be created if this does not already exist
        modal_backend_config=modal_backend_config,
        modal_frontend_config=modal_frontend_config,
        backend_vm_deploy_path='/path/to/deploy/on/your/vm',
        backend_domain='your.backend-domain.com',
        frontend_local_deploy_path='/path/on/your/local/machine/to/build/nextjs/web/app',
        frontend_flutter_app_path='/path/on/your/local/machine/to/build/flutter/web/app',
        frontend_domain='your.frontend-domain.com',
        open_ai_json_mode_model='gpt-4o-mini',
        version='0.0.1',
        deploy_backend=True,
        deploy_web=True,
        deploy_flutter=True
    )




