import pandas as pd
from modules.misc_interim_variables import *
from modules.GlobalVariables import *

def main():
    data = functions_(
        interim_knowledge_characteristics = True
    )

def functions_(interim_knowledge_characteristics):
    result = knowledge_characteristics(interim_knowledge_characteristics)
    return result

if __name__ == "__main__":
    main()
