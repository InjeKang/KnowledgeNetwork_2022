import pandas as pd
import os
from os.path import join
import math
import multiprocessing
from multiprocessing import Pool, cpu_count
import numpy as np
from functools import partial
import itertools
import tqdm


def read_data(filename, sheet_, readData):
    if readData:
        default_path = os.getcwd()
        input_path = join(default_path, "data")        
        # change default directory to read data
        os.chdir(input_path)
        # read excel file
        if filename.endswith("xlsx"):
            data = pd.read_excel(filename, engine="openpyxl", sheet_name = sheet_)
        # read pickle file
        else:
            data = pd.read_pickle(filename)        
        # reset default directory
        os.chdir(default_path)
        return data


def cleanse_data(data, cleanseData):
    if cleanseData: # from 00.Robustness_v6.8(matched3).xlsx
        column_ = ["merged_fpy", "formation", "year", "focal", "partner", "focal_nation", "partner_nation",
        "tech_sim", "intern",
        "semi_ind", "employ", "RnD", "revenue", "ratio_size", "ratio_ipc",
        "hhi_focal", "hhi_partner", "patent_size_focal", "patent_size_partner"] # "type", "concurrent", "prior_exp", "port_size", 
        data2 = data[column_]
        data3 = data2.dropna(subset=["tech_sim"], how="all").reset_index(drop=True) # 698,819 >> 376,449
        # # descriptive
        # data3["tech_sim"].describe()
        # data3["formation"].value_counts()
        # data3["intern"].value_counts()
        # data3["semi_ind"].value_counts()
        data3.to_excel("data\\01.firm_alliance_v5(removed_na).xlsx", index=False)
        return data3


def run_on_subset(func, data_subset):
    return data_subset.apply(func, axis=1)

def parallelize_on_rows(data, func):
    return multi_process(data, partial(run_on_subset, func))


def multi_process(df, target_func):
    n_cores = multiprocessing.cpu_count()-2
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    # pool.map(target_func, tqdm.tqdm(range(len(df_split))))
    output = pd.concat(pool.map(target_func, df_split))

    """
    When multiprocessing dataframe, check if .iloc is properly used to prevent KeyError: 0
        
    pool.apply: the function call is performed in a seperate process / blocks until the function is completed / lack of reducing time
    pool.apply_async: returns immediately instead of waiting for the result / the orders are not the same as the order of the calls
    pool.map: list of jobs in one time (concurrence) / block / ordered-results
    pool.map_async: 
    http://blog.shenwei.me/python-multiprocessing-pool-difference-between-map-apply-map_async-apply_async/
    """
    pool.close()
    pool.join()
    return output

def split_dataframe(data, split_func):
    n_cores = multiprocessing.cpu_count()-2
    df_list = split_func(data)
    df_split = [df_list[i::n_cores] for i in range(n_cores)]
    return df_split

def split_by_year(data):
    if "year" in data.columns:
        subset_by_year = [data[data["year"] == year_] for year_ in data["year"].unique()]
    else:
        subset_by_year = [data[data[10] == year_] for year_ in data[10].unique()]
    return subset_by_year

def multi_process_split_designated(data, func, split_func):
    n_cores = multiprocessing.cpu_count()-2
    df_split = split_dataframe(data, split_func)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


def merge_patent(data, mergePatent):
    if mergePatent:
        # read patent data
        patent_stock = pd.read_pickle("data\\00.patent_stock")
        # select only relevant columns
        patent_stock2 = patent_stock[["firm", 10, 32]]
        # drop None rows
        patent_stock2 = patent_stock2.dropna(subset=[10, 32], how="all").reset_index() # 980764 >> 980754
        data2 = data.copy()
        # make a column: list of four-digit IPCs, which were used by a focal firm
        target_func1 = partial(ipc_list, patent_stock2)
        data3 = multi_process(data2, target_func1)
        # data3 = ipc_list(patent_stock2, data2)
        # data2["focal_ipc"] = data2.swifter.apply(lambda x: ipc_list(x["focal"], patent_stock2), axis = 1)
        data3.to_pickle("data\\01.firm_alliance_v6(with_patent).pkl")
        # data3.to_excel("data\\01.firm_alliance_v2(with_patent).xlsx", index=False)
        return data3


def ipc_list(patent, data):
    data["focal_ipc"] = data.apply(lambda x: ipc_list_firm(x["focal"], x["year"], patent), axis = 1)
    return data


def ipc_list_firm(firm, year, patent):
    firm = firm.lower()
    firm_patent = patent[(patent["firm"].str.lower() == firm)
                    & (pd.to_numeric(patent[10]) <= year-1)
                    & (pd.to_numeric(patent[10]) >= year-5)].reset_index(drop=True, inplace=False)
    # flattening the dataframe to list
    ipcList = flatten_ipc(firm_patent["four_digitIPC"].tolist())
    ipcList2 = [x[0:4] for x in ipcList]
    return ipcList2


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


def split_(data, splitter):
    """to split string with the consideration of NoneType Error"""
    try:
        return data.split(splitter)
    except:
        return ""


def four_digit(data):
    four_digitIPC = [x[0:4] for x in data]
    return four_digitIPC

def edge_list(data):    
    co_words_list = []
    for co_word in data["four_digitIPC"]:
        co_words_list.extend(list(itertools.combinations(co_word, 2)))
    co_words_df = pd.DataFrame(co_words_list, columns = ["ipc1", "ipc2"])
    return co_words_df