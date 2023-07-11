from modules.GlobalVariables import *

import pandas as pd
import swifter
import networkx as nx

def knowledge_network(data, knowledgeNetwork):
    if knowledgeNetwork:
        # select only relevant columns
        data2 = data[["firm", 10, 32]]
        # drop None rows
        data2 = data2.dropna(subset=[10, 32], how="all").reset_index() # 980764 >> 980754
        # four digit
        data2["four_digitIPC"] = data2.swifter.apply(lambda x: four_digit(x[32].split(";")), axis = 1)
        # to edge list
        toEdgeList = edge_list(data2["four_digitIPC"])
        toEdgeList["count"] = 1
        toEdgeList2 = pd.DataFrame({"frequency": toEdgeList.groupby(["ipc1", "ipc2"])["count"].sum()}).reset_index()
        # identifying network structure
        G = nx.Graph()
        toEdgeList3 = list(toEdgeList2.itertuples(index=False, name=None))
        G.add_weighted_edges_from(toEdgeList3)
        # edge_labels = dict([((u,v),d["weight"]) for u,v,d, in G.edges(data=True)])
        # measuring degree centrality
        degree_centrality = nx.degree_centrality(G)
        # measuring structural holes: 2 - constraints (Wang et al., 2014)
        constraints = nx.constraint(G)
        toDF_dc = pd.DataFrame.from_dict(degree_centrality, orient="index", columns = ["dc"])
        toDF_con = pd.DataFrame.from_dict(constraints, orient="index", columns = ["constraint"])
        toDF_merged = toDF_dc.join(toDF_con)
        toDF_merged.reset_index(inplace=True)
        toDF_merged = toDF_merged.rename(columns = {'index':'IPC'})
        toDF_merged.to_excel("data\\02.IPC_networkScores.xlsx", index=False)
        return toEdgeList2







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