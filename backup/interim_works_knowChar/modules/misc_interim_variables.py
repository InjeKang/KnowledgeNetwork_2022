import pandas as pd
from modules.misc_interim_variables import *
from modules.GlobalVariables import *

import pandas as pd
import swifter
from multiprocessing import Pool
import multiprocessing
import numpy as np
import itertools
import math
from functools import partial
from collections import Counter

def knowledge_characteristics(interim_knowledge_characteristics):
    if interim_knowledge_characteristics:
        # alliance data
        # ally_data = pd.read_excel("D:\\Analysis\\2023_Network_v1(alliance)\\data\\01.firm_alliance_v5(removed_na).xlsx", engine="openpyxl", sheet_name = "Sheet1")
        ally_data = pd.read_pickle("D:\\Analysis\\2023_Network_v1(alliance)\\data\\01.firm_alliance_v6(with_patent).pkl")
        # remove irrelevant columns to speed up the analysis
        column_list = ["year", "focal", "focal_ipc"]
        ally_data2 = ally_data[["merged_fpy", "year", "focal", "focal_ipc"]]
        data3 = ally_data.drop(columns = column_list)

        # ally_data2 = ally_data[["merged_fpy", "focal", "year"]]
        # patent data
        patent_data = pd.read_pickle("D:\\Analysis\\2023_Network_v1(alliance)\\data\\00.patent_stock_v2(four_digitIPC).pkl")
        patent_data2 = patent_data[["firm", 10, "four_digitIPC"]]
        # ally_data3 = knowledge_characteristics_by_firm(patent_data2, ally_data)
        target_func = partial(knowledge_characteristics_by_firm, patent_data2)
        ally_data3 = multi_process(ally_data2, target_func)

        # merge data
        ally_data4 = pd.merge(ally_data3, data3, on="merged_fpy", how="outer")
        # ally_data3 = parallelize_on_rows(ally_data, target_func)
        ally_data4.to_excel("D:\\Analysis\\2023_Network_v1(alliance)\\data\\01.firm_alliance_v10(knowedge_characteristics).xlsx", index=False)
        return ally_data4

def knowledge_characteristics_by_firm(patent, firm):
        # firm["focal_ipc"] = firm.apply(lambda x: ipc_list_firm(x["focal"], x["year"], patent), axis = 1)
        firm["depth"] = firm.apply(lambda x: knowledge_depth(x["focal"], x["year"], x["focal_ipc"], patent), axis = 1)
        firm["breadth"] = firm.apply(lambda x: knowledge_breadth(x["focal_ipc"]), axis = 1)
        # firm.drop("focal_ipc", axis = 1)
        # remove the column "focal_ipc" to minimize data size
        return firm


def set_intersect(a, b):
    return bool(set(a) & set(b))


def knowledge_depth(firm, year, focal_ipc, patent):
    """Zhu et al. (2021): the average depth of a firm's five deepest technology positions"""
    counter = Counter(focal_ipc)
    top_five_ipc = [element for element, frequency in counter.most_common(5)]
    patent2 = patent[(patent["firm"].str.lower() == firm)
                        & (pd.to_numeric(patent[10]) <= year-1)
                        & (pd.to_numeric(patent[10]) >= year-5)].reset_index(drop=True, inplace=False)
    mask = patent2["four_digitIPC"].apply(set_intersect, b = top_five_ipc)
    no_patents_top5 = patent2[mask]    
    return len(no_patents_top5)/5

def knowledge_breadth(focal_ipc):
    """Zhu et al. (2021): The number of technology positions possessed by a focal firm"""
    unique_ipc = set(focal_ipc)
    return len(unique_ipc)