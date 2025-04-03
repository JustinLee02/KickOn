import pandas as pd

df = pd.read_csv("tottenham_balanced_data.csv")
df["Transfer"] = df["Transfer"].apply(lambda x: f"'{x}'")
df.to_csv("tottenham_balanced_data_v2.csv", index=False)
