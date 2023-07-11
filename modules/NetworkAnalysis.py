from modules.GlobalVariables import *

import pandas as pd
import swifter
import networkx as nx
from functools import partial
import numpy as np
import math
from tqdm import tqdm


    

def knowledge_network(data, knowledgeNetwork):
    if knowledgeNetwork:
        # read patent data
        patent_stock = pd.read_pickle("data\\00.patent_stock_v2(four_digitIPC).pkl")
        patent_stock2 = patent_stock[["firm", 10, "four_digitIPC"]] # remove unnecessary column [32]: raw-list of IPCs        
        # analyzing degree centrality and structural hole by year
        year_list = data["year"].unique()
        year_list.sort()
        dc_list = []
        sh_list = []
        for year in tqdm(year_list):
            # during the five-year period prior to an alliance formation (Guan and Liu, 2016; Yan and Gun, 2018)
            patent_stock3 = patent_stock2[(pd.to_numeric(patent_stock2[10]) <= year-1) & 
                            (pd.to_numeric(patent_stock2[10]) >= year-5)].reset_index() # drop=True makes error when making toEdgeList3
            degree_centrality_result = measure_degree_centrality(patent_stock3, year)
            dc_list.append(degree_centrality_result)
            structural_hole_result = measure_structural_hole(patent_stock3, year)
            sh_list.append(structural_hole_result)
        # into dataframe
        dc_result = pd.concat(dc_list, axis = 0)
        sh_result = pd.concat(sh_list, axis = 0)
        # to merge two dataframes
        dc_result["ipc_year"] = dc_result["ipc"] + dc_result["year"].astype(str)
        sh_result["ipc_year"] = sh_result["ipc"] + sh_result["year"].astype(str)
        result = pd.merge(dc_result, sh_result, on="ipc_year", how="outer")
        result = result[["ipc_year", "ipc_x", "year_x", "dc", "constraints", "sh"]]
        result = result.rename(columns = {"ipc_x" : "ipc", "year_x":"year"})
        result.to_excel("data\\03.network_result.xlsx", index=False)
        return result


def measure_degree_centrality(patent, year):    
    # to edge list
    toEdgeList = multi_process(patent, edge_list)
    ipc_freq = {(a, b, len(g)) for (a, b), g in toEdgeList.groupby(['ipc1', 'ipc2'])}
    # identifying network structure
    G = nx.Graph()
    G.add_weighted_edges_from(list(ipc_freq))
    # measuring degree centrality
    degree_centrality_ = nx.degree_centrality(G)
    toDF_dc = pd.DataFrame.from_dict(degree_centrality_, orient="index", columns = ["dc"])
    # from index to a column
    toDF_dc.reset_index(inplace=True)
    toDF_dc = toDF_dc.rename(columns = {"index" : "ipc"})
    toDF_dc["year"] = pd.to_numeric(year)
    period = f"{year-5}-{year-1}"
    filename = f"data\\02.dc_{period}.pkl"
    toDF_dc.to_pickle(filename)
    return toDF_dc

def measure_structural_hole(patent, year):
    # to edge list
    toEdgeList = multi_process(patent, edge_list)
    ipc_freq = {(a, b, len(g)) for (a, b), g in toEdgeList.groupby(['ipc1', 'ipc2'])}
    # identifying network structure
    G = nx.Graph()
    G.add_weighted_edges_from(list(ipc_freq))
    # measuring structural holes
    constraints_ = nx.constraint(G)
    toDF_sh = pd.DataFrame.from_dict(constraints_, orient="index", columns = ["constraints"])
    toDF_sh = toDF_sh.assign(sh = 2 - toDF_sh["constraints"])
    # from index to a column    
    toDF_sh.reset_index(inplace=True)
    toDF_sh = toDF_sh.rename(columns = {"index" : "ipc"})
    toDF_sh["year"] = year
    period = f"{year-5}-{year-1}"
    filename = f"data\\02.sh_{period}.pkl"
    toDF_sh.to_pickle(filename)
    return toDF_sh


def network_result(data, network_result, networkResult):
    if networkResult:  
        # remove irrelevant columns to speed up the analysis
        column_list = ["year", "focal", "focal_ipc"]
        data2 = data[["merged_fpy", "year", "focal", "focal_ipc"]]
        data3 = data.drop(columns = column_list)
        # make columns: average degree centrality and structural hole of each row
        target_func_dc = partial(network_analysis_by_firm, network_result)
        result = multi_process_split_designated(data2, target_func_dc, split_by_year)
        # merge with previous data
        result2 = pd.merge(result, data3, on="merged_fpy", how="outer")
        # result2.to_excel("data\\01.firm_alliance_v11(knowledge_network).xlsx", index=False)
        # return result2

        data4 = pd.read_excel("D:\\Analysis\\2023_Network_v1(alliance)\\data\\01.firm_alliance_v10(knowedge_characteristics).xlsx",
                        engine="openpyxl", sheet_name = "Sheet1")
        data5 = data4[["merged_fpy", "depth", "breadth"]] 
        result3 = pd.merge(result2, data5, on="merged_fpy", how="outer")

        result4 = result3.drop("focal_ipc", axis = 1)

        result4.to_pickle("data\\01.firm_alliance_v11(knowledge_network).pkl")
        result4.to_excel("data\\01.firm_alliance_v11(knowledge_network).xlsx", index=False)
        return result4


def network_analysis_by_firm(network_result, data):
    # read patent data
    # patent_stock = pd.read_pickle("data\\00.patent_stock_v2(four_digitIPC).pkl")
    # patent_stock2 = patent_stock[["firm", 10, "four_digitIPC"]] # remove unnecessary column [32]: raw-list of IPCs 
    # degree cenetrality by firm
    df_list = []
    # for i in tqdm(range(len(data))):
    for i in range(len(data)):        
        subset_by_year = data[i].reset_index(drop=True)
        # subset_network_result_by_year >> need to check
        network_result_by_year = network_result[network_result["year"] == data[i]["year"].iloc[0]].reset_index(drop=True, inplace=False)
        # making a variable: a list of IPCs from patents applied by a focal firm in prior to an alliance formation
        # subset_by_year["focal_ipc"] = subset_by_year.apply(lambda x: ipc_list_firm(x["focal"], x["year"], patent_stock2), axis = 1)
        # average degree centrality of a focal firm >> need to check data["focal_ipc"]
        subset_by_year["focal_dc"] = subset_by_year.apply(lambda x: average_degree_centrality(x["focal_ipc"], network_result_by_year), axis = 1)
        # average degree centrality of a focal firm
        subset_by_year["focal_sh"] = subset_by_year.apply(lambda x: average_structural_hole(x["focal_ipc"], network_result_by_year), axis = 1)
        df_list.append(subset_by_year)
    result = pd.concat(df_list, axis=0)
    # remove the column "focal_ipc" to minimize data size
    # result = result.drop("focal_ipc", axis = 1)
    return result


def average_degree_centrality(focal_ipc, network_result_by_year):
    selected_rows = network_result_by_year[network_result_by_year["ipc"].isin(focal_ipc)]
    # mask = network_result_by_year["ipc"].apply(set_intersect, b = focal_ipc)
    # selected_rows = network_result_by_year[mask]        
    return selected_rows["dc"].mean()


def average_structural_hole(focal_ipc, network_result_by_year):
    # select IPCs that are used by a firm
    selected_rows = network_result_by_year[network_result_by_year["ipc"].isin(focal_ipc)]
    # mask = network_result_by_year["four_digitIPC"].apply(set_intersect, b = focal_ipc)
    # selected_rows = network_result_by_year[mask]  
    # average degree centrality    
    return selected_rows["sh"].mean()

