#%%
import pandas as pd
import module as mod
import seaborn as sns
from matplotlib import pyplot as plt
import numpy as np
from module2 import *
# from mod_dao2 import * 
import joblib as jl
import os
from tqdm import tqdm


#%%
check_db()


#%%
# TODO
    # if no data in bdd :
# data_dir = 'base-joconde-extrait.csv'
# df = mod.get_data(data_dir)
df = jl.load('dff.jl')


    # Nettoyage
df = clean_encodage(df)
df = reduc_ligne(df)
# df = reduc_colonne(df)
# df = clean_musee_spe(df)
df = df.reset_index(drop=True)


#%%    
extract_data(df)


#%%
