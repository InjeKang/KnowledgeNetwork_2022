# # check a row with nonetype
# for i in range(len(data2)):
#     if data2[32][i] is None:
#         print("none found at row:", i)
#         exit()

# # add a column of tech_sim
# x = pd.read_excel("data\\00.Robustness_v6.8(matched3).xlsx")
# x2 = x[["merged_fpy", "tech_sim"]]
# merged = pd.merge(raw_data, x2, on="merged_fpy")
# merged.to_pickle("data\\01.firm_alliance_v3(sim_added).pkl")


# import pandas as pd
# df = pd.DataFrame({'existing_column': [1, 2, 3, 4, 5]})
# c = 10
# df = df.assign(new_column=df['existing_column'] + c)
# df

import pandas as pd
import swifter
from multiprocessing import Pool
import numpy as np
import tqdm


# convert dataframe for STATA
ally_data = pd.read_excel("data\\01.firm_alliance_v14(ctrlVar).xlsx", engine="openpyxl", sheet_name = "Sheet1")



# # (1) to match partner's nationality (2) to remove focal = partner
# ally_data = pd.read_excel("data\\01.firm_alliance_v12(spline_included).xlsx", engine="openpyxl", sheet_name = "Sheet1")
# ally_data = ally_data.drop(columns = ["partner_nation", "intern"])
# nationality = pd.read_excel("data\\04.partner_nationality.xlsx", engine="openpyxl", sheet_name = "Sheet1")
# # ally_data = pd.read_pickle("data\\01.firm_alliance_v6(with_patent).pkl")
# # ally_data2 = ally_data[["merged_fpy", "focal"]]

# # (1)
# firm_matched = [""]*len(ally_data)
# for i, _ in enumerate(ally_data["partner"]):
#     for partner, country in zip(nationality["firm"], nationality["country"]):
#         if _ == partner:
#             firm_matched[i] = country
#             print(i)

# ally_data["partner_nation"] = firm_matched

# def international(focal, partner):
#     if focal == partner:
#         return 1
#     else:
#         return 0

# ally_data["intern"] = ally_data.swifter.apply(lambda x: international(x["focal_nation"], x["partner_nation"]), axis = 1)



# subset_data = ally_data.query("focal != partner").reset_index()
# subset_data.to_excel("data\\01.firm_alliance_v13(intl_revised).xlsx", index=False)



# def naics_code(firm):
#     firm_data = pd.read_excel("data\\00.compustat_v1.xlsx", engine="openpyxl", sheet_name = "Sheet1")
#     firm2 = firm_data[firm_data["Company Name"].str.contains(firm.lower())].reset_index(drop = True)
#     naics_ = firm2["North American Industry Classification Code"][0]
#     naics_4digit = str(int(naics_))
#     naics_4digit = naics_4digit[:4]
#     return int(naics_4digit)

# firm_list = ally_data["focal"].unique()
# firm_list.sort()

# df_list = []
# for firm_ in firm_list:
#     subset_by_firm =ally_data[ally_data["focal"] == firm_]
#     subset_by_firm["naics"] = naics_code(firm_)
#     df_list.append(subset_by_firm)
# result = pd.concat(df_list, axis = 0)





# ally_data["naics"] = ally_data.swifter.apply(lambda x: naics_code(x["focal"]), axis = 1)
# # ally_data3 = pd.merge(ally_data, ally_data2, on="merged_fpy")
# ally_data.to_excel("data\\01.firm_alliance_v7(naics).xlsx", index=False)



