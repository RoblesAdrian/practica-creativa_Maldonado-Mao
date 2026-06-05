#!/usr/bin/env python3
import pandas as pd

input_file = "data/origin_dest_distances.jsonl"
output_file = "data/origin_dest_distances.csv"

df = pd.read_json(input_file, lines=True)
df = df.rename(columns={"Origin": "origin", "Dest": "dest", "Distance": "distance"})
df["distance"] = df["distance"].astype(int)
df.to_csv(output_file, index=False)

print(f"Wrote {output_file} with {len(df)} rows")