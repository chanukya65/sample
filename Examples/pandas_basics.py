import pandas as pd
import numpy as np

a = [19, 'hello', 100, np.nan, 'INF']
b = [100, 9,  'hi', 'INF', np.nan]
df = pd.DataFrame(a, b)
df = df.replace('INF', np.nan)
print(df)

