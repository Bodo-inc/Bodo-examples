import pandas as pd
import bodo
import time
import os
@bodo.jit
def mean_power_speed(filename):
    print("in py")
    df = pd.read_csv(filename)
    m = len(df)
    return m
cwd = os.getcwd()
res = mean_power_speed(f"{cwd}/data")
print(res)