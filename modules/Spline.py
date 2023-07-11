from modules.GlobalVariables import *

import pandas as pd




def spline_function(data, splineFunction):
    if splineFunction:
        # make columns: average degree centrality and structural hole of each row
        # result = multi_process_split_designated(data, spline_dummy_congruent, split_by_year)
        # result = multi_process_split_designated(data, spline_dummy_independent, split_by_year)
        result = multi_process_split_designated(data, spline_dummy_joint, split_by_year)
        # result = spline_dummy(data)
        result.to_excel("data\\01.firm_alliance_v17(spline_revised3).xlsx", index=False)
        return result

def spline_dummy_joint(data):
    df_list = []
    # for i in tqdm(range(len(data))):
    for i in range(len(data)):        
        subset_by_year = data[i].reset_index(drop=True)
        mean_dc = subset_by_year["focal_dc"].mean()
        mean_sh = subset_by_year["focal_sh"].mean()
        subset_by_year["dcH_sh"] = subset_by_year.apply(lambda x:
                                                           joint_dummy(x["focal_dc"], x["focal_sh"], mean_dc, "H"), axis = 1)
        subset_by_year["dcL_sh"] = subset_by_year.apply(lambda x:
                                                           joint_dummy(x["focal_dc"], x["focal_sh"], mean_dc, "L"), axis = 1)
        
        df_list.append(subset_by_year)
    result = pd.concat(df_list, axis=0)
    return result        


def joint_dummy(dc, sh, mean_, type_):
    if type_ == "H":
        if dc >= mean_:
            return sh*dc
        else:
            return 0
    else:
        if dc < mean_:
            return sh*dc
        else:
            return 0        


def spline_dummy_independent(data):
    df_list = []
    # for i in tqdm(range(len(data))):
    for i in range(len(data)):        
        subset_by_year = data[i].reset_index(drop=True)
        mean_dc = subset_by_year["focal_dc"].mean()
        mean_sh = subset_by_year["focal_sh"].mean()
        subset_by_year["dc_H"] = subset_by_year.apply(lambda x:
                                                           convert_to_binary(x["focal_dc"], mean_dc, "H"), axis = 1)
        subset_by_year["dc_L"] = subset_by_year.apply(lambda x:
                                                           convert_to_binary(x["focal_dc"], mean_dc, "L"), axis = 1)
        subset_by_year["sh_H"] = subset_by_year.apply(lambda x:
                                                           convert_to_binary(x["focal_sh"], mean_sh, "H"), axis = 1)
        subset_by_year["sh_L"] = subset_by_year.apply(lambda x:
                                                           convert_to_binary(x["focal_sh"], mean_sh, "L"), axis = 1)
        
        df_list.append(subset_by_year)
    result = pd.concat(df_list, axis=0)
    return result    

def spline_dummy_congruent(data):
    df_list = []
    # for i in tqdm(range(len(data))):
    for i in range(len(data)):        
        subset_by_year = data[i].reset_index(drop=True)
        mean_dc = subset_by_year["focal_dc"].mean()
        mean_sh = subset_by_year["focal_sh"].mean()
        # subset_by_year["dc_binary"] = subset_by_year.apply(lambda x:
        #                                                    convert_to_binary(x["focal_dc"], mean_dc), axis = 1)
        # subset_by_year["sh_binary"] = subset_by_year.apply(lambda x:
        #                                                    convert_to_binary(x["focal_sh"], mean_sh), axis = 1)
        subset_by_year["convergency_HH"] = subset_by_year.apply(lambda x:
                                                             ccongruent_effect(x["focal_dc"], x["focal_sh"], mean_dc, mean_sh, "HH"), axis = 1)
        subset_by_year["convergency_HL"] = subset_by_year.apply(lambda x:
                                                             ccongruent_effect(x["focal_dc"], x["focal_sh"], mean_dc, mean_sh, "HL"), axis = 1)
        subset_by_year["convergency_LH"] = subset_by_year.apply(lambda x:
                                                             ccongruent_effect(x["focal_dc"], x["focal_sh"], mean_dc, mean_sh, "LH"), axis = 1)        
        subset_by_year["convergency_LL"] = subset_by_year.apply(lambda x:
                                                             ccongruent_effect(x["focal_dc"], x["focal_sh"], mean_dc, mean_sh, "LL"), axis = 1)
        df_list.append(subset_by_year)
    result = pd.concat(df_list, axis=0)
    return result

def convert_to_binary(value_, mean_, type_):
    if type_ == "H":
        if value_ >= mean_:
            return value_
        else:
            return 0
    else:
        if value_ < mean_:
            return value_
        else:
            return 0        

def ccongruent_effect(dc, sh, mean_dc, mean_sh, type_): # type = HH, HL, LH, LL
    if type_ == "HH":
        if (dc >= mean_dc and sh >= mean_sh):
            return dc*sh
        else:
            return 0
    elif type_ == "HL":
        if (dc >= mean_dc and sh < mean_sh):
            return dc*sh
        else:
            return 0
    elif type_ == "LH":
        if (dc < mean_dc and sh >= mean_sh):
            return dc*sh
        else:
            return 0
    else: # type = LL
        if (dc < mean_dc and sh < mean_sh):
            return dc*sh
        else:
            return 0