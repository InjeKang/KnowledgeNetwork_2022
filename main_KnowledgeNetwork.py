from modules.GlobalVariables import *
from modules.NetworkAnalysis import *
from modules.KnowledgeCharacteristics import *
from modules.Spline import *
from modules.ControlVariables import *


import pandas as pd
from os.path import join
import os
from functools import partial
import swifter
import datetime


def main():
    data = functions_(
        # read data
        readData = True
        # cleanse data to remove irrelevant columns
        , cleanseData = False
        # merge with patent data
        , mergePatent = False
        , knowledgeCharacteristics = False
        # knowledge network
        , knowledgeNetwork = False
        # average DC and SH of each focal firm by year
        , networkResult = False
        , splineFunction = True
        , controlVariable = False
        , toStata = True
    )


def functions_(readData, cleanseData, mergePatent,
               knowledgeCharacteristics, knowledgeNetwork, networkResult, splineFunction,
               controlVariable, toStata):
    # read data
    # raw_data = read_data("00.patent_stock", "Sheet1", readData)
    raw_data = read_data("01.firm_alliance_v16(spline_revised2).xlsx", "Sheet1", readData)

    # clean and manipulate data
    cleansed = cleanse_data(raw_data, cleanseData)

    # alliance data concatenating with a focal firm's IPC list
    patent_merged = merge_patent(raw_data, mergePatent)

    # analyzing knowledge characteristics of a focal firm
    df_knowChar = knowledge_characteristics(raw_data, knowledgeCharacteristics)    

    # overall network structure constructed in one year prior to an alliance formation
    if cleanseData:
        df_network_result = knowledge_network(cleansed, knowledgeNetwork)
    else:
        df_network_result = knowledge_network(raw_data, knowledgeNetwork)
        
    # add variables: network analysis    
    if knowledgeNetwork:
        network_column_included = network_result(raw_data, df_network_result, networkResult)
    else:
        df_network_result = read_data("03.network_result.xlsx", "Sheet1", readData)
        network_column_included = network_result(raw_data, df_network_result, networkResult)

    # add variables: employing spline approach
    df_congruency = spline_function(raw_data, splineFunction)
    current_time = datetime.datetime.now()
    time_str = current_time.strftime("%H:%M:%S")
    print("Completed without any error! Completed Spline at:", time_str)

    # add control variables
    df_ctrlVar = control_variables(df_congruency, controlVariable)

    # data for STATA
    df_stata = to_stata(df_congruency, toStata)

    current_time = datetime.datetime.now()
    time_str = current_time.strftime("%H:%M:%S")        
    return print("Completed without any error! Completed All at:", time_str)



if __name__ == "__main__":
    main()