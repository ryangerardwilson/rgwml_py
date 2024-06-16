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



# Call the test method
#test_axlinr()
#test_axlinr_2()
#test_axlinr_3()
test_axlinr_4()
#test_axlogr_1()
test_axlogr_2()

