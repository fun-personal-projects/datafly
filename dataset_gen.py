#!/usr/bin/env python3

import pandas as pd
import numpy as np
import os
import random
# Functions for each type of thing

def fix_range(x):
    x = str(x)
    x = x.replace('(', '')
    x = x.replace(')','')
    x = x.replace('[', '')
    x = x.replace(']', '')
    x = x.replace(',', '-')
    x = x.replace(' ', '')
    return x

def multipleFix(x):
    return [fix_range(y) for y in x]

def round10(x):
    return round(x, -1)

def age(data, col):
    # Get min and max vals rounded off to nearest 10s
    # col = col.values.flatten()
    col = data[col]
    # col2 = col - 10
    # col3 = col +10
    # print(col, col2, col3)
    tot = len(col)
    min_val, max_val = 0, round(col.max(),-1) +10
    quartile_val = int(round(max_val/4,-1))
    middle_val = int(round(max_val/2,-1))
    
    binned1 = multipleFix( [ (min_val, round10(x)) for x in col ] )
    binned2 = multipleFix( [ (min_val, max(round10(x), middle_val)) for x in col ] )
    binned3 = multipleFix( [ (min_val, max_val) for x in col ] )
    # binned1 = multipleFix([(int(round(x,-1)), int(round(x,-1))+10) for x in col])
    # # binned1 = multipleFix(pd.cut(x = col , bins =bins))
    # binned2 = multipleFix([(0, quartile_val) for _ in range(len(binned1))])
    # binned3 = multipleFix([(0, max_val) for _ in range(len(binned1))])
    # return binned1, binned2
    return "\n".join([f"{i+1}, {binned1[i]}, {binned2[i]}, {binned3[i]}" for i in range(len(binned1))])

    
    # return "".join([fix_range(x)+"\n" for x in binned])

def replacewithstar(string, no):
    siz = len(string)
    return string[:siz-no]+ no*"*"
# print(replacewithstar("12343", 3))

def genStars(row, no):
    return ",".join([replacewithstar(row, i) for i in range(no+1)])

# print(genStars("12083", 5))

def zip(data, col):
    col = data[col]
    max_val = len(str(col.max()))
    padded_vals = [x.zfill(max_val) for x in col.astype('str')]
    return "".join([genStars(x, max_val)+ "\n" for x in padded_vals])

def ret_val_between(ran):
    return str(np.random.randint(ran[0], ran[1]))

def diseases():
    list_dis =["Cancer", "AIDS", "Autism", "Alzheimer's disease", "Anorexia","Heart disease"]
    return random.choice(list_dis)

# temp = pd.DataFrame({'a':[23, 24, 50, 67]})
# print(age(temp, 'a'))

# Generating the dataset
# Set parameters

row_names = ["id","age","father_age","zip_code", "zip_code2", "disease"]
types = ["","age", "age", "zip", "zip","dis"]
no_rows = 20
ranges = ["", [0, 40], [60,90], [10000, 70000], [10000, 70000]]

main_db = open("example/db_100_gen.csv", "w+")
main_db.write(",".join(row_names) + "\n")

# Create
for i in range(no_rows):
    main_db.write(f"{str(i+1)}, {ret_val_between(ranges[1])},{ret_val_between(ranges[2])},{ret_val_between(ranges[3])}, {ret_val_between(ranges[4])}, {diseases()}\n")
    main_db.flush()
    
main_db.close()

# Create DGH files

data = pd.read_csv("example/db_100_gen.csv")
cols = data.columns.values
for i in range(len(types)):
    if types[i]== "age":
        with open(f"example/gen_{cols[i]}_generalization.csv", "w+") as f:
            # print(age(data, cols[i]))
            f.write(age(data, cols[i]).strip())
 
    elif types[i] == "zip":
        with open(f"example/gen_{cols[i]}_generalization.csv", "w+") as f:
            f.write(zip(data, cols[i]).strip())
     
