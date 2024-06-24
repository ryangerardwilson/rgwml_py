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

    """
    modal_backend_config = {
        "sudo": {
            "username": "sudo",
            "password": "sudo"
            },
        "modals": {
            "social_media_esclataions": "url,forum,mobile,issue,status,sub_status,action_taken,follow_up_date",
            "high_pain_customers": "mobile,issue,pain_level,status,action_taken,follow_up_date",
            "welcome_calls": "mobile,status,issue,action_taken"
            }
        }
    """

    modal_backend_config = {
        "sudo": {
            "username": "sudo",
            "password": "sudo"
            },  
        "modals": {
            "social_media_escalations": {
                "columns": "url,forum,mobile,issue,status,sub_status,action_taken,follow_up_date",
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
test_ser()
