#!/usr/bin/env python
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import json
import argparse

parser = argparse.ArgumentParser(description="Plots the crawling result on a map")
parser.add_argument("-i", "--input", help="The json or the crawling", required=True)
parser.add_argument("-o", "--output", help="The png output", default="kaspad_nodes.png")

args = parser.parse_args()

all_info = json.load(open(args.input,"r"))
valid_nodes = {k:v for k,v in all_info.items() if v["error"] != "'timeout'" and v["loc"] != ""}

if len(valid_nodes) == 0:
    parser.error("No public nodes found in json")

location_df = pd.DataFrame([{"address": k, "loc": v["loc"]} for k,v in valid_nodes.items()])
# From GeoPandas, our world map data
worldmap = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))

# Creating axes and plotting world map
fig, ax = plt.subplots(figsize=(12, 6))
worldmap.plot(color="lightgrey", ax=ax)

x = location_df["loc"].str.split(",").str[1].astype("float")
y = location_df["loc"].str.split(",").str[0].astype("float")

plt.scatter(x, y, s=5)

# Creating axis limits and title
plt.xlim([-180, 180])
plt.ylim([-90, 90])
print(args.output)
plt.savefig(args.output)
