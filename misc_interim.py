import pandas as pd
from modules.misc_interim_variables import *

def knowledge_characteristics(patent, ally_data):
    ally_data["focal_ipc"] = ally_data.apply(lambda x: ipc_list_firm(x["focal"], x["year"], patent), axis = 1)
    ally_data["depth"] = ally_data.apply(lambda x: knowledge_depth(x["focal_ipc"], axis = 1))
    # ally_data["breadth"] = ally_data.apply(lambda x: knowledge_breadth(x["focal_ipc"], axis = 1))
    return ally_data