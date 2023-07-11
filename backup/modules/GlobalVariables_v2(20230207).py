import pandas as pd
import os
from os.path import join
import math

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
    if cleanseData:
        column_ = ["merged_fpy", "formation", "year", "focal", "partner", "focal_nation", "partner_nation",
        "type", "concurrent", "prior_exp", "port_size", "semi_ind", 
        "employ", "RnD", "revenue", "ratio_size", "ratio_ipc", "hhi_focal", "hhi_partner", "patent_size_focal", "patent_size_partner"]
        data2 = data[column_]
        data2.to_excel("data\\01.firm_alliance_v1.xlsx", index=False)
        return data2


def merge_patent(data, mergePatent):
    if mergePatent:
        # read patent data
        patent_stock = pd.read_pickle("data\\00.patent_stock")
        # select only relevant columns
        patent_stock2 = patent_stock[["firm", 32]]
        # drop None rows
        patent_stock2 = patent_stock2.dropna(subset=[32], how="all").reset_index() # 980764 >> 980754
        data2 = data.copy()
        # make a column: list of four-digit IPCs, which were used by a focal firm
        data2["focal_ipc"] = data2.swifter.apply(lambda x: ipc_list(x["focal"], patent_stock2), axis = 1)
        data2.to_pickle("data\\01.firm_alliance_v2(with_patent).pkl")
        return data2



def ipc_list(firm, patent):
    firm = firm.lower()
    firm_patent = patent[patent["firm"].str.lower() == firm].reset_index(drop=True, inplace=False)
    # flattening the dataframe to list
    ipcList = flatten_ipc(firm_patent[32].tolist())
    ipcList2 = [x[0:4] for x in ipcList]
    return ipcList2

    

def ipc_list(patent, data):
    data["focal_ipc"] = data.apply(lambda x: ipc_list_firm(x["focal"], x["year"], patent), axis = 1)
    return data


def ipc_list_firm(firm, year, patent):
    firm = firm.lower()
    firm_patent = patent[(patent["firm"].str.lower() == firm)
                    & (pd.to_numeric(patent[10]) <= year-1)].reset_index(drop=True, inplace=False)
    # flattening the dataframe to list
    ipcList = flatten_ipc(firm_patent[32].tolist())
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
    

def edge_list(data):    
    co_words_list = []    
    for i in range(len(data)):
        # sort alphabetically to address non-directed relationship
        data[i].sort()
        for j in range(len(data[i])):
            for k in range(len(data[i])):
                if (j < k) & (j + k <= len(data[i])*2 -1):
                    co_words = [[], []]
                    co_words[0] = data[i][j]
                    co_words[1] = data[i][k]
                    co_words_list.append(co_words)  
    co_words_df = pd.DataFrame(co_words_list, columns = ["ipc1", "ipc2"])
    return co_words_df