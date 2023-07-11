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
        patent_stock2 = pd.read_pickle("data\\00.patent_stock_v2(four_digitIPC).pkl")
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
        year_list = [2000, 2001]
        df_list = []
        for i in range(len(year_list)):
            subset_by_year = data[data["year"] == i].reset_index(drop=True)
                # patent assigned prior to alliance
            patent_stock3 = patent_stock2[(pd.to_numeric(patent_stock2[10]) <= i-1)].reset_index() # drop=True makes error when making toEdgeList3
            # make a column of a degree centrality from a knowledge network constructed prior to an alliance formation
            target_func1 = partial(measure_degree_centrality, patent_stock2)
            subset_dc = multi_process(subset_by_year, target_func1)
            # data3 = measure_degree_centrality(patent_stock3, data2)
            # make a column of a structural holes
            target_func2 = partial(measure_structural_hole, patent_stock3)
            subset_sh = multi_process(subset_dc, target_func2)
            # data4 = measure_structural_hole(patent_stock2, data2)            
            df_list.append(subset_sh)
        result = pd.concat(df_list, axis = 0)

        # data2["focal_ipc"] = data2.swifter.apply(lambda x: ipc_list(x["focal"], patent_stock2), axis = 1)
        # data2.to_pickle("data\\01.firm_alliance_v2(with_patent).pkl")
        return result


def measure_degree_centrality(patent, data):
    # average degree centrality of a focal firm
    data["focal_dc"] = data.apply(lambda x: degree_centrality_firm(x["focal_ipc"], x["year"], patent), axis = 1)
    # data["focal_dc"] = data.apply(lambda x:
    #                 np.nan if math.isnan(x["tech_sim"])
    #                 else degree_centrality_firm(x["focal_ipc"], x["year"], patent), axis = 1)
    return data

    
def degree_centrality_firm(firm_ipc, year, patent):
    # patent assigned prior to alliance
    patent2 = patent[(pd.to_numeric(patent[10]) <= year-1)].reset_index() # drop=True makes error when making toEdgeList3
    # to edge list
    toEdgeList = edge_list(patent2["four_digitIPC"])
    toEdgeList["count"] = 1
    toEdgeList2 = pd.DataFrame({"frequency": toEdgeList.groupby(["ipc1", "ipc2"])["count"].sum()}).reset_index()
    # identifying network structure
    G = nx.Graph()
    toEdgeList3 = list(toEdgeList2.itertuples(index=False, name=None))
    G.add_weighted_edges_from(toEdgeList3)
    # edge_labels = dict([((u,v),d["weight"]) for u,v,d, in G.edges(data=True)])
    # measuring degree centrality
    degree_centrality_ = nx.degree_centrality(G)
    toDF_dc = pd.DataFrame.from_dict(degree_centrality_, orient="index", columns = ["dc"])
    # from index to a column    
    toDF_dc.reset_index(inplace=True)
    toDF_dc = toDF_dc.rename(columns = {"index" : "ipc"})
    # select IPCs that are used by a firm
    selected_rows = toDF_dc[toDF_dc["ipc"].isin(firm_ipc)]
    # average degree centrality    
    return selected_rows.mean()


def measure_structural_hole(patent, data):
    # average degree centrality of a focal firm
    data["focal_sh"] = data.apply(lambda x: structural_hole_firm(x["focal_ipc"], x["year"], patent), axis = 1)
    return data

    
def structural_hole_firm(firm_ipc, year, patent):
    # patent assigned prior to alliance
    patent2 = patent[(pd.to_numeric(patent[10]) <= year-1)].reset_index() # drop=True makes error when making toEdgeList3
    # to edge list
    toEdgeList = edge_list(patent2["four_digitIPC"])
    toEdgeList["count"] = 1
    toEdgeList2 = pd.DataFrame({"frequency": toEdgeList.groupby(["ipc1", "ipc2"])["count"].sum()}).reset_index()
    # identifying network structure
    G = nx.Graph()
    toEdgeList3 = list(toEdgeList2.itertuples(index=False, name=None))
    G.add_weighted_edges_from(toEdgeList3)
    # edge_labels = dict([((u,v),d["weight"]) for u,v,d, in G.edges(data=True)])
    # measuring degree centrality
    constraints_ = nx.constraint(G)
    toDF_sh = pd.DataFrame.from_dict(constraints_, orient="index", columns = ["constraints"])
    toDF_sh = toDF_sh.assign(sh = 2 - toDF_sh["constraints"])
    # from index to a column    
    toDF_sh.reset_index(inplace=True)
    toDF_sh = toDF_sh.rename(columns = {"index" : "ipc"})
    # select IPCs that are used by a firm
    selected_rows = toDF_sh[toDF_sh["ipc"].isin(firm_ipc)]
    # average degree centrality    
    return selected_rows["sh"].mean()






def four_digit(data):
    four_digitIPC = [x[0:4] for x in data]
    return four_digitIPC

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