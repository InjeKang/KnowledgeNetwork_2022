from modules.GlobalVariables import *

import pandas as pd
import swifter
import networkx as nx
from functools import partial
import numpy as np
import math

def split_dataframe(data, split_func):
    n_cores = multiprocessing.cpu_count()-2
    df_list = split_func(data)
    df_split = [df_list[i::n_cores] for i in range(n_cores)]
    return df_split

def split_by_year(data):
    return [data[data["year"] == year_] for year_ in data["year"].unique()]

def parallelize_dataframe(data, func, split_func):
    n_cores = multiprocessing.cpu_count()-2
    df_split = split_dataframe(data, split_func, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df

result = parallelize_dataframe(ally_data, apply_naics_code, split_by_focal)

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
        year_list = data2["year"].unique()
        year_list.sort()
        # year_list = [2000, 2001]
        df_list = []
        for i in year_list:
            subset_by_year = data2[data2["year"] == i].reset_index(drop=True)
                # patent assigned prior to alliance
            patent_stock3 = patent_stock2[(pd.to_numeric(patent_stock2[10]) <= i-1)].reset_index() # drop=True makes error when making toEdgeList3
            # analyze network structure - align network scores with each element
            dc_by_year = measure_degree_centrality(patent_stock3)
            sh_by_year = measure_structural_hole(patent_stock3)
            filename_dc = f"data\\02.dc_{i}.pkl"
            filename_sh = f"data\\02.sh_{i}.pkl"
            dc_by_year.to_pickle(filename_dc)
            sh_by_year.to_pickle(filename_sh)
            # make a column of a degree centrality from a knowledge network constructed prior to an alliance formation
            target_func1 = partial(degree_centrality_firm, dc_by_year)
            subset_dc = multi_process(subset_by_year, target_func1)
            # data3 = measure_degree_centrality(patent_stock3, data2)
            # make a column of a structural holes            
            target_func2 = partial(structural_hole_firm, sh_by_year)
            subset_sh = multi_process(subset_dc, target_func2)
            # data4 = measure_structural_hole(patent_stock2, data2)            
            df_list.append(subset_sh)
        result = pd.concat(df_list, axis = 0)
        # result = pd.concat(df_list, ignore_index = True)

        # data2["focal_ipc"] = data2.swifter.apply(lambda x: ipc_list(x["focal"], patent_stock2), axis = 1)
        # data2.to_pickle("data\\01.firm_alliance_v2(with_patent).pkl")
        result.to_excel("data\\01.firm_alliance_v8(knowledge_network).xlsx", index=False)
        return result

def measure_degree_centrality(patent):
    # to edge list
    toEdgeList = edge_list(patent["four_digitIPC"])
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
    return toDF_dc

def measure_structural_hole(patent):
    # to edge list
    toEdgeList = edge_list(patent["four_digitIPC"])
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
    return toDF_sh


def degree_centrality_firm(dc_by_year, data):
    # average degree centrality of a focal firm
    data["focal_dc"] = data.apply(lambda x: average_degree_centrality(x["focal_ipc"], dc_by_year), axis = 1)
    # data["focal_dc"] = data.apply(lambda x:
    #                 np.nan if math.isnan(x["tech_sim"])
    #                 else degree_centrality_firm(x["focal_ipc"], x["year"], patent), axis = 1)
    return data


def average_degree_centrality(firm_ipc, dc_by_year):
    selected_rows = dc_by_year[dc_by_year["ipc"].isin(firm_ipc)]
    return selected_rows.mean()


def structural_hole_firm(sh_by_year, data):
    data["focal_sh"] = data.apply(lambda x: average_structural_hole(x["focal_ipc"], sh_by_year), axis = 1)
    return data

def average_structural_hole(firm_ipc, sh_by_year):
    # select IPCs that are used by a firm
    selected_rows = sh_by_year[sh_by_year["ipc"].isin(firm_ipc)]
    # average degree centrality    
    return selected_rows["sh"].mean()



def measure_structural_hole(patent):
    df_list = []
    for i in range(len(patent)):        
        subset_by_year = patent[i].reset_index(drop=True)