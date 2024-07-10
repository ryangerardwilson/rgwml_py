import rgwml as r
import pandas as pd
import numpy as np
import tempfile

def test_axlinr():
    # Create a mock dataset
    headers = ["feature1", "feature2", "feature3", "target_column"]
    data = [
        np.random.rand(100).tolist(),
        np.random.rand(100).tolist(),
        np.random.rand(100).tolist(),
        np.random.rand(100).tolist()
    ]

    d = r.p()
    d.frd(headers, data)

    # Apply axl method to split the data
    d.axl('70:20:10')

    # Apply axrp method to append XGB regression predictions
    d.axlinr('target_column', 'feature1, feature2, feature3','PREDICTION')

def test_axlinr_2():
    # Create a mock dataset with categorical data
    headers = ["feature1", "feature2", "feature3", "target_column"]
    data = [
        np.random.choice(['A', 'B', 'C'], 100).tolist(),  # Categorical data
        np.random.rand(100).tolist(),
        np.random.rand(100).tolist(),
        np.random.rand(100).tolist()
    ]

    d = r.p()
    d.frd(headers, data)

    # Apply axl method to split the data
    d.axl('70:20:10')

    d.axlinr('target_column', 'feature1, feature2, feature3', 'PREDICTION').plc('target_column, PREDICTION').f("XGB_TYPE == 'TEST'").plc('target_column, PREDICTION','feature1')

def test_axlinr_3():
    # Create a mock dataset with categorical data
    headers = ["feature1", "feature2", "feature3", "target_column"]
    data = [
        np.random.choice(['A', 'B', 'C'], 100).tolist(),  # Categorical data
        np.random.rand(100).tolist(),
        np.random.rand(100).tolist(),
        np.random.rand(100).tolist()
    ]

    d = r.p()
    d.frd(headers, data)

    # Apply axl method to split the data
    d.axl('70:20:10').axlinr('target_column', 'feature1, feature2, feature3', 'PREDICTION').plc('target_column, PREDICTION')

def test_axlinr_4():
    # Create a mock dataset with categorical data
    headers = ["feature1", "feature2", "feature3", "target_column"]
    data = [
        np.random.choice(['A', 'B', 'C'], 100).tolist(),  # Categorical data
        np.random.rand(100).tolist(),
        np.random.rand(100).tolist(),
        np.random.rand(100).tolist()
    ]

    d = r.p()
    d.frd(headers, data)

    # Apply axl method to split the data
    d.axl('70:30').axlinr('target_column', 'feature1, feature2, feature3', 'PREDICTION').plc('target_column, PREDICTION')

def test_axlogr_1():
    # Create a mock dataset with categorical data
    headers = ["feature1", "feature2", "feature3", "target_column"]
    data = [
        np.random.choice(['A', 'B', 'C'], 100).tolist(),  # Categorical data
        np.random.rand(100).tolist(),
        np.random.rand(100).tolist(),
        np.random.randint(0, 2, 100).tolist()  # Binary target column
    ]

    d = r.p()
    d.frd(headers, data)

    # Apply axl method to split the data
    d.axl('70:20:10').axlogr('target_column', 'feature1, feature2, feature3', 'PREDICTION').plc('target_column, PREDICTION')

def test_axlogr_2():
    # Create a mock dataset with categorical data
    headers = ["feature1", "feature2", "feature3", "target_column"]
    data = [
        np.random.choice(['A', 'B', 'C'], 100).tolist(),  # Categorical data
        np.random.rand(100).tolist(),
        np.random.rand(100).tolist(),
        np.random.randint(0, 2, 100).tolist()  # Binary target column
    ]

    d = r.p()
    d.frd(headers, data)

    # Apply axl method to split the data
    d.axl('70:30').axlogr('target_column', 'feature1, feature2, feature3', 'PREDICTION').plc('target_column, PREDICTION')


def test_pdist():

    headers = ["Column1","Column2","Column3"]
    data = [
        [1, 2, 2, 3, 3, 3, 4, 4, 4, 4],
        [5, 6, 6, 7, 7, 7, 8, 8, 8, 8],
        [9, 10, 10, 11, 11, 11, 12, 12, 12, 12]
    ]

    d = r.p()
    d.frd(headers,data)
    d.pdist(y='Column1,Column2,Column3')


def test_pqq():

    headers = ["Column1","Column2","Column3"]
    data = [
        [1, 2, 2, 3, 3, 3, 4, 4, 4, 4],
        [5, 6, 6, 7, 7, 7, 8, 8, 8, 8],
        [9, 10, 10, 11, 11, 11, 12, 12, 12, 12]
    ]
    
    d = r.p()
    d.frd(headers,data)
    d.pqq(y='Column1,Column2,Column3')

def test_pcr():

    np.random.seed(0)

    data = {
        'Column1': np.random.normal(0, 1, 100),
        'Column2': np.random.normal(0, 1, 100),
        'Column3': np.random.normal(0, 1, 100)
    }
    
    # Introduce some correlation
    data['Column2'] = data['Column1'] * 0.5 + np.random.normal(0, 0.5, 100)
    data['Column3'] = data['Column1'] * -0.7 + np.random.normal(0, 0.5, 100)

    # Convert to list format
    headers = ['Column1', 'Column2', 'Column3']
    data_list = [data[col].tolist() for col in headers]

    d = r.p()
    d.frd(headers,data_list)
    d.pcr(y='Column1,Column2,Column3')

def test_acc():
    np.random.seed(0)

    # Create a DataFrame with sample data
    data = {
        'Column1': np.random.normal(0, 1, 100),
        'Column2': np.random.normal(0, 1, 100),
        'Column3': np.random.normal(0, 1, 100)
    }

    # Introduce some correlation
    data['Column2'] = data['Column1'] * 0.5 + np.random.normal(0, 0.5, 100)
    data['Column3'] = data['Column1'] * -0.7 + np.random.normal(0, 0.5, 100)


    headers = ['Column1', 'Column2', 'Column3']
    data_list = [data[col].tolist() for col in headers]

    d = r.p()
    d.frd(headers,data_list)

    # Perform clustering using the acc method
    d.acc('Column1,Column2,Column3', 'KMEANS', 'cluster_column', visualize=True, n_clusters_finding_method='FIXED:5')


def test_ancc():
    np.random.seed(0)

    # Create a DataFrame with sample data
    data = {
        'Column1': np.random.normal(0, 1, 100),
        'Column2': np.random.normal(0, 1, 100),
        'Column3': np.random.normal(0, 1, 100)
    }

    # Introduce some correlation
    data['Column2'] = data['Column1'] * 0.5 + np.random.normal(0, 0.5, 100)
    data['Column3'] = data['Column1'] * -0.7 + np.random.normal(0, 0.5, 100)


    headers = ['Column1', 'Column2', 'Column3']
    data_list = [data[col].tolist() for col in headers]

    d = r.p()
    d.frd(headers,data_list)

    # Perform clustering using the acc method
    d.ancc('Column1,Column2,Column3', 'KMEANS', 'cluster_column', n_clusters_finding_method='FIXED:5',visualize=True)

def test_adbscancc():
    np.random.seed(0)

    # Create a DataFrame with sample data
    data = {
        'Column1': np.random.normal(0, 1, 100),
        'Column2': np.random.normal(0, 1, 100),
        'Column3': np.random.normal(0, 1, 100)
    }

    # Introduce some correlation
    data['Column2'] = data['Column1'] * 0.5 + np.random.normal(0, 0.5, 100)
    data['Column3'] = data['Column1'] * -0.7 + np.random.normal(0, 0.5, 100)


    headers = ['Column1', 'Column2', 'Column3']
    data_list = [data[col].tolist() for col in headers]

    d = r.p()
    d.frd(headers,data_list)

    # Perform clustering using the acc method
    d.adbscancc('Column1,Column2,Column3', 'cluster_column', eps=0.5, min_samples=5, visualize=True)

def test_pnfc():
    d = r.p()
    d.fq('i2e1','SELECT TOP 10000 * FROM t_customer_ticket')
    d.pnfc(10,'title, resolution_type, tags')

def test_pnfl():
    d = r.p()
    d.fq('i2e1','SELECT TOP 10000 * FROM t_customer_ticket')
    d.pnfl(10,'title, resolution_type, tags')


def test_ser():


    project_name = "sajal-ka-crm"


    modal_backend_config = {
        "sudo": {
            "username": "sudo",
            "password": "sudo"
            },
        "modals": {
            "social_media_escalations": {
                "columns": "url[VARCHAR(2048)],forum,mobile,issue,status,sub_status,action_taken,follow_up_date",
                "read_routes": [
                    {"most-recent-500": "SELECT id, url,forum,mobile,issue,status,sub_status,action_taken,follow_up_date, CONVERT_TZ(created_at, '+00:00', '+05:30') AS created_at, CONVERT_TZ(updated_at, '+00:00', '+05:30') AS updated_at FROM social_media_escalations ORDER BY id DESC LIMIT 500"},
                    {"todays-cases": "SELECT id, url,forum,mobile,issue,status,sub_status,action_taken,follow_up_date, CONVERT_TZ(created_at, '+00:00', '+05:30') AS created_at, CONVERT_TZ(updated_at, '+00:00', '+05:30') AS updated_at FROM social_media_escalations WHERE DATE(CONVERT_TZ(created_at, '+00:00', '+05:30')) = CURDATE() ORDER BY id ASC"},
                    {"yesterdays-cases": "SELECT id, url,forum,mobile,issue,status,sub_status,action_taken,follow_up_date, CONVERT_TZ(created_at, '+00:00', '+05:30') AS created_at, CONVERT_TZ(updated_at, '+00:00', '+05:30') AS updated_at FROM social_media_escalations WHERE DATE(CONVERT_TZ(created_at, '+00:00', '+05:30')) = DATE_SUB(CURDATE(), INTERVAL 1 DAY) ORDER BY id ASC"}
                    ]
                },
            }
        }



    modal_frontend_config = {
        "social_media_escalations": { 
            "options": { 
                "forum": ["Google_Reviews", "LinkedIn", "Twitter/X", "Facebook", "Instagram", "YouTube", "Other"],
                "status": ["Unresolved", "Resolved_but_post_not_removed", "Not_able_to_identify_poster"],
                "issue": ["Internet_supply_down", "Slow_speed", "Frequent_disconnect", "Rude_behaviour_of_Partner", "Booking_fee_refund", "Trust issue", "Other"]
            },
            "conditional_options": {
                "sub_status": [
                    {
                        "condition": "status == Unresolved",
                        "options": ["Did_not_pick_up", "Picked_up_yet_unresolved"]
                    },
                    {
                        "condition": "status == Resolved_but_post_not_removed",
                        "options": ["Was_very_angry", "Other"]
                    }
                ]
            },
            "scopes": {
                "create": True,
                "read": ["id", "url", "forum", "mobile", "issue", "status", "sub_status", "action_taken", "follow_up_date", "created_at"],
                "update": ["url", "forum", "mobile", "issue", "status", "sub_status", "action_taken", "follow_up_date"],
                "delete": True
            },
            "validation_rules": {
                "url": ["REQUIRED"],
                "forum": ["REQUIRED"],
                "issue": ["REQUIRED"],
                "status": ["REQUIRED"],
                "sub_status": ["REQUIRED"],
                "action_taken": ["REQUIRED"],
                "follow_up_date": ["REQUIRED","IS_AFTER_TODAY"]
            },
            "ai_quality_checks": {
                "action_taken": ["must describe a meaningful step taken to reach out to a customer and resolve a social media escalation"]
            },
        },
    }
    """
        "high_pain_customers": {
            "options": {
                "status": ["WIP_(Partner)", "WIP_(Wiom)"],
                "issue": ["Internet_supply_down", "Slow_speed", "Frequent_disconnect", "Rude_behaviour_of_Partner", "Booking_fee_refund", "Other"]
            },
            "scopes": {
                "create": True,
                "read": ["id", "mobile", "issue", "pain_level", "status", "action_taken", "follow_up_date", "created_at"],
                "update": ["issue", "status", "action_taken", "follow_up_date"],
                "delete": False
            },
            "validation_rules": {
                "mobile":["REQUIRED", "IS_INDIAN_MOBILE_NUMBER"],
                "issue": ["REQUIRED"],
                "status": ["REQUIRED"],
                "action_taken": ["REQUIRED"],
                "follow_up_date": ["REQUIRED", "IS_AFTER_TODAY"]
            },
            "ai_quality_checks": {
                "action_taken": ["must describe a meaningful step taken to reach out to a high pain customer and resolve a service ticket"]
            },
        },
        "welcome_calls": {
            "options": {
                "status": ["FONI_detected", "No_FONI_detected"],
                "issue": ["Internet supply down", "Slow speed", "Frequent disconnect", "Rude behaviour of Partner", "Booking fee refund", "Other"]
            },
            "scopes": {
                "create": True,
                "read": ["id", "mobile", "issue", "status", "action_taken", "created_at"],
                "update": ["issue", "status", "action_taken"],
                "delete": False
            },
            "validation_rules": {
                "issue": ["REQUIRED"],
                "status": ["REQUIRED"],
                "action_taken": ["REQUIRED"]
            },
            "ai_quality_checks": {
                "action_taken": ["must describe a meaningful welcome call conversation with a customer by a customer support agent after a router installation"]
            },
        },
    """



    crm = r.f()
    crm.ser(
        project_name= 'sajal-ka-crm',
        new_db_name= 'sajal_ka_crm',
        db_preset_name='happy_sudo',
        vm_preset_name='labs_main_server',
        modal_backend_config=modal_backend_config,
        modal_frontend_config=modal_frontend_config,
        backend_vm_deploy_path='/home/rgw/Apps/FORGE_sajal_ka_crm',
        backend_domain='sajal-ka-crm-api.10xlabs.in',
        frontend_local_deploy_path='/home/rgw/Apps/forge_sajal_ka_crm',
        frontend_domain='sajal-ka-crm.10xlabs.in',
        open_ai_json_mode_model='gpt-3.5-turbo'
    )

def test_dg():
    # Create an instance of the class
    d_instance = r.d()

    # Load a DataFrame using the frd method
    headers = ['group', 'column1', 'column2']
    data = [
        [1, 10, 'A'],
        [1, 20, 'A'],
        [2, 30, 'A'],
    ]
    d_instance.frd(headers, data)

    # Use the g method to perform the group-by and aggregations
    d_instance.g(['group'], ['column1::sum', 'column2::count'])


def test_dg_2():

    # Create an instance of the class
    d = r.d()

    # Load a DataFrame using the frd method
    headers = ['group', 'column1', 'column2']
    data = [
        [1, 10, 'A'],
        [1, 20, 'A'],
        [2, 30, 'A'],
    ]
    d.frd(headers, data)

    # Use the g method to perform the group-by and aggregations
    d.g(['group'], ['column1::sum', 'column2::css', 'column2::count'])

def test_dg_3():

    # Create an instance of the class
    d = r.d()

    # Load a DataFrame using the frd method
    headers = ['group', 'column1', 'column2']
    data = [
        [1, 10, 'A'],
        [1, 20, 'A'],
        [2, 30, 'A'],
    ]
    d.frd(headers, data)

    # Use the g method to perform the group-by and aggregations
    d.g(['group'], ['column1::sum', 'column2::css_unique', 'column2::count_unique'])


def test_dg_4():

    # Create an instance of the class
    d = r.d()

    # Load a DataFrame using the frd method
    headers = ['group', 'column1', 'column2']
    data = [
        [1, 10, 'A,B,C'],
        [1, 20, 'A,X,Y'],
        [2, 30, 'A'],
    ]
    d.frd(headers, data)

    # Use the g method to perform the group-by and aggregations
    d.g(['group'], ['column1::sum', 'column2::css_granular_unique', 'column2::count_granular_unique'])

def test_pg_4():

    # Create an instance of the class
    d = r.p()

    # Load a DataFrame using the frd method
    headers = ['group', 'column1', 'column2']
    data = [
        [1, 10, 'A,B,C'],
        [1, 20, 'A,X,Y'],
        [2, 30, 'A'],
    ]
    d.frd(headers, data)

    # Use the g method to perform the group-by and aggregations
    d.g(['group'], ['column1::sum', 'column2::css_granular_unique', 'column2::count_granular_unique'])

def test_dbq_1():
    d = r.p()
    d.dbq('happy_sudo','test','DROP DATABASE test')
    d.dbq('happy_sudo', 'happy_main', 'SHOW DATABASES')
    d.dbq('happy_sudo', 'happy_main', 'CREATE DATABASE test')
    d.dbq('happy_sudo', 'happy_main', 'SHOW DATABASES')

def test_dbct():
    d = r.p()
    d.dbct('happy_sudo','test','test_table','Column1, Column2, Column3[VARCHAR(1000)]')
    d.dbq('happy_sudo', 'test', 'SHOW TABLES')
    d.dbq('happy_sudo', 'test', 'DESCRIBE test.test_table')

def test_dbrct():
    d = r.p()
    d.dbrct('happy_sudo','test','test_table','Column7, Column9, Column3[VARCHAR(900)]')
    d.dbq('happy_sudo', 'test', 'SHOW TABLES')
    d.dbq('happy_sudo', 'test', 'DESCRIBE test.test_table')

def test_dbi():
    d = r.p()
    d.frd(['Column7','Column9','Column3'],[[1,2,3],[4,5,6],[7,8,9]])
    d.dbi('happy_sudo', 'test','test_table', insert_columns=['Column7','Column9','Column3'])
    d.frd(['Column7','Column9','Column3'],[[10,11,12],[13,14,15],[16,17,18]])
    d.dbi('happy_sudo', 'test','test_table', insert_columns=['Column7','Column9','Column3'])
    d.frd(['Column7','Column9','Column3'],[[1,2,3],[4,5,6],[7,8,9]])
    d.dbi('happy_sudo', 'test','test_table', insert_columns=['Column7','Column9','Column3'])
    d.fq('happy_sudo','SELECT * FROM test.test_table')

def test_dbiu():
    d = r.p()
    d.frd(['Column7','Column9','Column3'],[[1,2,3],[4,5,6],[7,8,9]])
    d.dbiu('happy_sudo', 'test','test_table', unique_columns=['Column7','Column9','Column3'], insert_columns=['Column7','Column9','Column3'])
    d.frd(['Column7','Column9','Column3'],[[10,11,12],[13,14,15],[16,17,18]])
    d.dbiu('happy_sudo', 'test','test_table', unique_columns=['Column7','Column9','Column3'], insert_columns=['Column7','Column9','Column3'])
    d.fq('happy_sudo','SELECT * FROM test.test_table')

def test_dbtai():
    d = r.p()
    d.frd(['Column7','Column9','Column3'],[[1,2,3],[4,5,6],[7,8,9]])
    d.dbtai('happy_sudo', 'test','test_table', ['Column7','Column9','Column3'])
    d.fq('happy_sudo','SELECT * FROM test.test_table')

def test_dbuoi():
    d = r.p()
    d.frd(['Column7','Column9','Column3'],[[1,2,3],[4,5,6],[7,8,9]])
    d.dbtai('happy_sudo', 'test','test_table', ['Column7','Column9','Column3'])
    d.fq('happy_sudo','SELECT * FROM test.test_table')

    d = r.p()
    d.frd(['Column7','Column9','Column3'],[[1,2,33333],[10,11,12],[13,14,15]])
    d.dbuoi('happy_sudo', 'test','test_table', update_where_columns=['Column7','Column9'], update_at_column_names=['Column3'])
    d.fq('happy_sudo','SELECT * FROM test.test_table')


def test_goaibc():
    d = r.p()
    d.frd(['item','value'],[['apple',100],['spinach',200],['vinegar',300]])
    batch_id = d.goaibc('fruit_or_vegetable_classification','gpt-3.5-turbo','item','fruit, vegetable, other')
    status = d.goaibs(batch_id)

    d.oaibs(batch_id)
    d.oaibc(batch_id)
    d.oaibs(batch_id)
    d.oaibl()

def test_oais():
    d = r.p()
    d.frd(['item','value'],[['apple',100],['spinach',200],['vinegar',300]])
    d.oais('/home/rgw/Desktop/test.h5','gpt-3.5-turbo','item','fruit, vegetable, other','ai_classification')

def test_oaih():
    d = r.p()
    d.oaih('/home/rgw/Desktop/test.h5')


def test_oaiatc():
    d = r.p()
    d.frd(['recording'],[
        ['https://cloudphone.tatateleservices.com/file/recording?callId=1720456064.234573&type=rec&token=SEhNVXoxc24rSEdRRDFEZVorV2ZYK2xkd0ptdVd3a25DUkZ2b05aOVJWMHBLWHZjRHhZbHJHZjlVNHdMVGFBSTo6YWIxMjM0Y2Q1NnJ0eXl1dQ%3D%3D'],
        ['https://cloudphone.tatateleservices.com/file/recording?callId=1720456143.234600&type=rec&token=dFFUQlN0MERtSXRsbmJRS3NiQ0hEcXNSYVFuU0FLRWt5WWk1c1llSzFGN0VHL3ArYUxxRThPaFhOd0ZkcUdZSzo6YWIxMjM0Y2Q1NnJ0eXl1dQ%3D%3D'],
        ['https://cloudphone.tatateleservices.com/file/recording?callId=1720455766.234443&type=rec&token=VGNaZGovdTE5UFZmcnRzL1NrMlEvQ3FmbFUvRVRJdlRjaXovRzJnUnZ1VDhUajZuSGZZVWVvbEozN0I1b29OWjo6YWIxMjM0Y2Q1NnJ0eXl1dQ%3D%3D']
    ])
    d.oaiatc('recording','transcription',participants='agent, customer', classify=[{'emotion':'very_happy, happy, neutral, unhappy, very_unhappy'}, {'issue':'internet_issue, payment_issue, other_issue'}], summary_word_length=30)
    d.fnr(3)

def test_oaiatc_2():
    d = r.p()
    d.fq('happy','SELECT recording_url FROM tata_sajal_events WHERE actual_speak_time > 60 ORDER BY id DESC LIMIT 10')
    d.oaiatc('recording_url','transcription',participants='agent, customer', classify=[{'emotion':'very_happy, happy, neutral, unhappy, very_unhappy'}, {'issue':'internet_issue, payment_issue, other_issue'}], summary_word_length=30, chunk_size=5)
    d.fnr(3)


# Call the test method
#test_axlinr()
#test_axlinr_2()
#test_axlinr_3()
#test_axlinr_4()
#test_axlogr_1()
#test_axlogr_2()
#test_pdist()
#test_pqq()
#test_pcr()
#test_ancc()
#test_adbscancc()
#test_pnfc()
#test_pnfl()
#test_ser()
#test_dg()
#test_dg_2()
#test_dg_3()
#test_dg_4()
#test_pg_4()

#test_dbq_1()
#test_dbct()
#test_dbrct()
#test_dbi()
#test_dbiu()
#test_dbtai()
#test_dbuoi()
#test_goaibc()
#test_oais()
#test_oaih()
#test_oaiatc()
test_oaiatc_2()
