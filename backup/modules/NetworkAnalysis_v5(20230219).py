from modules.GlobalVariables import *

import pandas as pd
import swifter
import networkx as nx
from functools import partial
import numpy as np
import math



    

def knowledge_network(data, knowledgeNetwork):
    if knowledgeNetwork:
        # read patent data
        patent_stock = pd.read_pickle("data\\00.patent_stock_v2(four_digitIPC).pkl")
        patent_stock2 = patent_stock[["firm", 10, "four_digitIPC"]] # remove unnecessary column [32]: raw-list of IPCs
        # patent_stock2 = pd.read_pickle("data\\00.patent_stock")
        # select only relevant columns
        # patent_stock2 = patent_stock[["firm", 10, 32]]
        # drop None rows
        # patent_stock2 = patent_stock2.dropna(subset=[10, 32]).reset_index(drop=True) # 980764 >> 980754 
        # four digit
        # patent_stock2["four_digitIPC"] = patent_stock2.swifter.apply(lambda x: four_digit(x[32].split(";")), axis = 1)
        # patent_stock2.to_pickle("data\\00.patent_stock_v2(four_digitIPC).pkl")
        data2 = data.copy()
        # to prevent the duplicate work, split dataframe by years
        # year_list = data2["year"].unique()
        # year_list.sort()
        # measuring degree centrality
        # target_func_dc = partial(measure_degree_centrality, year_list)
        dc_result = multi_process_split_designated(patent_stock2, measure_degree_centrality, split_by_year)
        # measuring structrual hole
        # target_func_sh = partial(measure_structural_hole, year_list)
        sh_result = multi_process_split_designated(patent_stock2, measure_structural_hole, split_by_year)
        # make a column of a degree centrality from a knowledge network constructed prior to an alliance formation 
        target_func1 = partial(degree_centrality_firm, dc_result)
        subset_dc = multi_process_split_designated(data2, target_func1, split_by_year)
        # data3 = measure_degree_centrality(patent_stock3, data2)
        # make a column of a structural holes            
        target_func2 = partial(structural_hole_firm, sh_result)
        subset_sh = multi_process_split_designated(subset_dc, target_func2, split_by_year)
        
        subset_sh.to_excel("data\\01.firm_alliance_v8(knowledge_network).xlsx", index=False)
        return subset_sh


def measure_degree_centrality(patent):
    df_list = []
    for i in range(len(patent)):
        subset_by_year = patent[i].reset_index(drop=True)
        # to edge list
        toEdgeList = edge_list(subset_by_year["four_digitIPC"])
        # toEdgeList["count"] = 1
        # toEdgeList2 = pd.DataFrame({"frequency": toEdgeList.groupby(["ipc1", "ipc2"])["count"].sum()}).reset_index()
        ipc_freq = {(a, b, len(g)) for (a, b), g in toEdgeList.groupby(['ipc1', 'ipc2'])}
        # identifying network structure
        G = nx.Graph()
        # toEdgeList3 = list(toEdgeList2.itertuples(index=False, name=None))
        # G.add_weighted_edges_from(toEdgeList3)
        G.add_weighted_edges_from(list(ipc_freq))
        # edge_labels = dict([((u,v),d["weight"]) for u,v,d, in G.edges(data=True)])
        # measuring degree centrality
        degree_centrality_ = nx.degree_centrality(G)
        toDF_dc = pd.DataFrame.from_dict(degree_centrality_, orient="index", columns = ["dc"])
        # from index to a column    
        toDF_dc.reset_index(inplace=True)
        toDF_dc = toDF_dc.rename(columns = {"index" : "ipc"})
        toDF_dc["year"] = pd.to_numeric(subset_by_year[10][0])
        filename = f"data\\02.dc_{pd.to_numeric(subset_by_year[10][0])}.pkl"
        toDF_dc.to_pickle(filename)
        df_list.append(toDF_dc)
    result = pd.concat(df_list, axis = 0)
    return result

def measure_structural_hole(patent):
    df_list = []
    for i in range(len(patent)):        
        subset_by_year = patent[i].reset_index(drop=True)
        # to edge list
        toEdgeList = edge_list(subset_by_year["four_digitIPC"])
        # toEdgeList["count"] = 1
        # toEdgeList2 = pd.DataFrame({"frequency": toEdgeList.groupby(["ipc1", "ipc2"])["count"].sum()}).reset_index()
        ipc_freq = {(a, b, len(g)) for (a, b), g in toEdgeList.groupby(['ipc1', 'ipc2'])}
        # identifying network structure
        G = nx.Graph()
        # toEdgeList3 = list(toEdgeList2.itertuples(index=False, name=None))
        # G.add_weighted_edges_from(toEdgeList3)
        G.add_weighted_edges_from(list(ipc_freq))
        # edge_labels = dict([((u,v),d["weight"]) for u,v,d, in G.edges(data=True)])
        # measuring degree centrality
        constraints_ = nx.constraint(G)
        toDF_sh = pd.DataFrame.from_dict(constraints_, orient="index", columns = ["constraints"])
        toDF_sh = toDF_sh.assign(sh = 2 - toDF_sh["constraints"])
        # from index to a column    
        toDF_sh.reset_index(inplace=True)
        toDF_sh = toDF_sh.rename(columns = {"index" : "ipc"})
        toDF_sh["year"] = pd.to_numeric(subset_by_year[10][0])
        filename = f"data\\02.sh_{pd.to_numeric(subset_by_year[10][0])}.pkl"
        toDF_sh.to_pickle(filename)
        df_list.append(toDF_sh)
    result = pd.concat(df_list, axis = 0)
    return result


def degree_centrality_firm(dc_by_year, data):
    df_list = []
    for i in range(len(data)):        
        patent_by_year = dc_by_year[i].reset_index(drop=True)
        subset_by_year = data[i].reset_index(drop=True)
        # average degree centrality of a focal firm
        subset_by_year["focal_dc"] = subset_by_year.apply(lambda x: average_degree_centrality(x["focal_ipc"], patent_by_year), axis = 1)
        # data["focal_dc"] = data.apply(lambda x:
        #                 np.nan if math.isnan(x["tech_sim"])
        #                 else degree_centrality_firm(x["focal_ipc"], x["year"], patent), axis = 1)
        df_list.append(subset_by_year)
    result = pd.concat(df_list, axis=0)
    return result


def average_degree_centrality(firm_ipc, dc_by_year):
    selected_rows = dc_by_year[dc_by_year["ipc"].isin(firm_ipc)]
    return selected_rows.mean()


def structural_hole_firm(sh_by_year, data):
    df_list = []
    for i in range(len(data)):        
        patent_by_year = sh_by_year[i].reset_index(drop=True)
        subset_by_year = data[i].reset_index(drop=True)
        # average degree centrality of a focal firm
        subset_by_year["focal_sh"] = subset_by_year.apply(lambda x: average_structural_hole(x["focal_ipc"], patent_by_year), axis = 1)
        # data["focal_dc"] = data.apply(lambda x:
        #                 np.nan if math.isnan(x["tech_sim"])
        #                 else degree_centrality_firm(x["focal_ipc"], x["year"], patent), axis = 1)
        df_list.append(subset_by_year)
    result = pd.concat(df_list, axis=0)
    return result


def average_structural_hole(firm_ipc, sh_by_year):
    # select IPCs that are used by a firm
    selected_rows = sh_by_year[sh_by_year["ipc"].isin(firm_ipc)]
    # average degree centrality    
    return selected_rows["sh"].mean()

