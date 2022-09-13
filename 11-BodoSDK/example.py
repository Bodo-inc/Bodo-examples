import pandas as pd
#import bodo
import time
import os
@bodo.jit
def mean_power_speed():
    print("in py")
    cwd = os.getcwd()
    df = pd.read_csv(f"{cwd}/data/cycling_dataset.csv")
    m = df[["power", "speed"]].mean()
    return m

res = mean_power_speed()
print(res)