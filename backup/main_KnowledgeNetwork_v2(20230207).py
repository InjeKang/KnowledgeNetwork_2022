from modules.GlobalVariables import *
from modules.NetworkAnalysis import *


import pandas as pd
from os.path import join
import os
from functools import partial
import swifter


def main():
    data = functions_(
        # read data
        readData = True
        # cleanse data to remove irrelevant columns
        , cleanseData = False
        # knowledge network
        , knowledgeNetwork = True
        # merge with patent data
        , mergePatent = False
    )

def functions_(readData, cleanseData, knowledgeNetwork, mergePatent):
    raw_data = read_data("00.patent_stock", "Sheet1", readData)
    # raw_data = read_data("01.firm_alliance_v1.xlsx", "Sheet1", readData)
    cleansed = cleanse_data(raw_data, cleanseData)
    network_ = knowledge_network(raw_data, knowledgeNetwork)
    patent_merged = merge_patent(raw_data, mergePatent)

    return raw_data


if __name__ == "__main__":
    main()