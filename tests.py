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
    crm = r.f()
    crm.ser(
        db_preset_name='happy_sudo',
        new_db_name='labsforge',
        vm_preset_name='labs_main_server',
        modal_map={'customers': 'mobile,issue,status', 'partners': 'mobile,issue,status'},
        backend_deploy_at='/home/rgw/Apps/labsforgeAPI',
        backend_deploy_port='8080',
        frontend_deploy_path='/home/rgw/Apps/forge_frontend'
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
