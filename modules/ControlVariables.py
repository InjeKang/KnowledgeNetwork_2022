from modules.GlobalVariables import *

import pandas as pd
import numpy as np



def control_variables(data, controlVariable):
    if controlVariable:
        result = multi_process(data, manipulate_control_variables)
        result.to_excel("data\\01.firm_alliance_v14(ctrlVar).xlsx", index=False)
        return result


def manipulate_control_variables(data):
    data["logRev"] = data.apply(lambda x: np.log(x["revenue"]*1000000), axis = 1)
    data["logRnD"] = data.apply(lambda x: np.log(x["RnD"]*1000000), axis = 1)
    data["logEmp"] = data.apply(lambda x: np.log(x["employ"]*1000), axis = 1)
    data["logHHI"] = data.apply(lambda x: np.log(x["hhi_focal"]), axis = 1)
    data["semi_dummy"] = data.apply(lambda x: semi_to_dummy(x["semi_ind"]), axis = 1)
    return data

def semi_to_dummy(semi_):
    if semi_ == True:
        return 1
    else:
        return 0
