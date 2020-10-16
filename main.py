from ml_part import *
import os
import pandas as pd

input_file = "example/db_100.csv"
output_path = "example/db_100_3_anon.csv"

quasi = '"age" "zip_code"'

dgh_files = '"example/age_generalization.csv" "example/zip_code_generalization.csv"'

k = 3
print("Anonymizing data")
os.system(
    f"python algo.py -pt {input_file} -qi {quasi} -dgh {dgh_files} -k {k} -o {output_path}"
)

orig_data = pd.read_csv(input_file)

print("Running ML regression")
an_data = pd.read_csv(output_path, names=orig_data.columns)

main_ret(orig_data)
main_ret(an_data)
