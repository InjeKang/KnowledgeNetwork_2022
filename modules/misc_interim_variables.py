import pandas as pd
import swifter
from multiprocessing import Pool
import multiprocessing
import numpy as np
import itertools
import math
from functools import partial

def split_(data, splitter):
    """to split string with the consideration of NoneType Error"""
    try:
        return data.split(splitter)
    except:
        return ""

def flatten_ipc(data):        
    flatten_list = []
    if isinstance(data, list):
        for i in range(len(data)):
            if isinstance(data[i], list): # lists in list
                data[i] = [strX for strX in data[i] if strX.strip()] # remove blank string
                data[i] = list((strX.replace(" ", "") for strX in data[i])) # remove whitespace
                data[i] = [split_(strX, ";") for strX in data[i]]
                data[i] = [strX for strX_list in data[i] for strX in strX_list] # to flatten a list of lists
                flatten_list.extend(data[i])                      
            elif isinstance(data[i], str):
                data[i] = data[i].replace(" ","")
                data[i] = split_(data[i], ";")
                flatten_list.extend(data[i])
            elif (data[i] == [] or data[i] == "") :
                flatten_list = data[i]
            elif ((data[i] is None) or math.isnan(data[i])):
                pass
            else:
                print(data[i])
    elif math.isnan(data):
        flatten_list = data
    else:
        print(data)
    return flatten_list

def ipc_list_firm(firm, year, patent):
    firm = firm.lower()
    firm_patent = patent[(patent["firm"].str.lower() == firm)
                    & (pd.to_numeric(patent[10]) <= year-1)
                    & (pd.to_numeric(patent[10]) >= year-5)].reset_index(drop=True, inplace=False)
    # flattening the dataframe to list
    ipcList = flatten_ipc(firm_patent[32].tolist())
    ipcList2 = [x[0:4] for x in ipcList]
    return ipcList2

def multi_process(df, target_func):
    n_cores = 8
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    output = pd.concat(pool.map(target_func, df_split))
    pool.close()
    pool.join()
    return output

def knowledge_depth(focal_ipc):
    """Zhu et al. (2021): the average depth of a firm's five deepest technology positions"""
    focal_ipc







ally_data = pd.read_excel("D:\\Analysis\\2023_Network_v1(alliance)\\data\\01.firm_alliance_v5(removed_na).xlsx", engine="openpyxl", sheet_name = "Sheet1")
# ally_data = pd.read_pickle("data\\01.firm_alliance_v6(with_patent).pkl")
# ally_data2 = ally_data[["merged_fpy", "focal"]]
patent_data = pd.read_pickle("D:\\Analysis\\2023_Network_v1(alliance)\\data\\00.patent_stock_v2(four_digitIPC).pkl")
patent_data2 = patent_data[["firm", 10, "four_digitIPC"]]


ally_data2 = ally_data[["merged_fpy", "focal", "year"]]

target_func = partial(knowledge_characteristics, patent_data2)
ally_data3 = multi_process(ally_data2, target_func)









def naics_code(firm):
    firm_data = pd.read_excel("data\\00.compustat_v1.xlsx", engine="openpyxl", sheet_name = "Sheet1")
    firm2 = firm_data[firm_data["Company Name"].str.contains(firm.lower())].reset_index(drop = True)
    naics_ = firm2["North American Industry Classification Code"][0]
    naics_4digit = str(int(naics_))
    naics_4digit = naics_4digit[:4]
    return int(naics_4digit)

def apply_naics_code(df):
    df["naics"] = df["focal"].apply(naics_code)
    return df



def split_dataframe(data, split_func, num_partitions):
    df_list = split_func(data)
    df_split = [df_list[i::num_partitions] for i in range(num_partitions)]
    return df_split

def parallelize_dataframe(df, func, split_func, num_partitions=8):
    df_split = split_dataframe(df, split_func, num_partitions)
    pool = Pool(num_partitions)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df

def split_by_focal(df):
    return [df[df["focal"] == f] for f in df["focal"].unique()]


result = parallelize_dataframe(ally_data, apply_naics_code, split_by_focal)
print(result)
