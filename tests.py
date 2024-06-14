import rgwml as r
import pandas as pd
import numpy as np
import tempfile

def test_axr():
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
    d.axr('target_column', 'feature1, feature2, feature3','PREDICTION')

def test_axr_2():
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

    d.axr('target_column', 'feature1, feature2, feature3', 'PREDICTION').plc('target_column, PREDICTION').f("XGB_TYPE == 'TEST'").plc('target_column, PREDICTION','feature1')

# Call the test method
test_axr()
test_axr_2()

