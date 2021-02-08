# I could just use simple python to do the analysis in part 2. However, that seems tedious, and I want to showcase some other tools.
# Ideally i would have done this in spark. However, I don't have spark setup in windows (getting some weird errors...thanks windows...) and databricks community edition won't let me persist files in dbfs.
# If i get the time, I'll implement the entire thing (including part 1) in spark and demo it.
# The biggest advantage of the spark framework is how it handles everything in memory - no more disk i/o operations :)
import json
import os
import pandas as pd

if __name__ == '__main__':
    # Setting up some parameters. Ideally parameterized
    output_dir = "output_data/"
    valid_filename = "valid_ndcs.parquet"
    invalid_filename = "invalid_ndcs.parquet"

    # Reading in data from Part 1 and creating a dataframe using all the package NDCs found from there.
    input_df = pd.read_csv("Documents/ApolloMed Data Engineering Challenge_Dataset.csv")
    with open("output_data/formatted-drug-ndc-0001-of-0001.json", "r") as f:
        fda = json.load(f)["results"]

    valid_ndcs = list()
    for product in fda:
        for package in product["packaging"]:
            valid_ndcs.append(package["formatted_package_ndc"])

    valid_ndc_df = pd.DataFrame(valid_ndcs, columns=["NDC_PACKAGE_CODE"]) # Turn the list of valid ndcs generated from the fda list into a df
    valid_ndc_df["_c1"] = 0  # adding a dummy column here for use later

    # Find all valid inputs by taking the given input file and inner joining it against the valid ndcs
    valid_input = input_df.merge(valid_ndc_df, how='inner', on="NDC_PACKAGE_CODE") # Inner join because only codes that are on both lists can be considered valid
    num_valid = valid_input.groupby("NDC_PACKAGE_CODE").count().drop(columns=["_c1"])
    num_valid.columns = ["num_claims"] # This part is a little weird. It seems like the index is not considered an actual column

    # Find all invalid inputs by taking the given input file looking for all instances where those ndcs are not found from cross referencing the FDA list
    invalid_input = input_df.merge(valid_ndc_df, how="left", on="NDC_PACKAGE_CODE").query("_c1.isnull()")
    num_invalid = invalid_input.groupby("NDC_PACKAGE_CODE").count().drop(columns=["_c1"])
    num_invalid.columns = ["num_claims"]

    # Write the output files for analysis later
    num_valid.to_parquet(os.path.join(output_dir, valid_filename))
    num_invalid.to_parquet(os.path.join(output_dir, invalid_filename))