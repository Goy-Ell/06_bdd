#%%
import pandas as pd
import module as mod
import seaborn as sns
from matplotlib import pyplot as plt
import numpy as np
from module2 import *
from mod_dao2 import * 
import joblib as jl
import os


#%%
connection,_ = new_db()




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



for i in range (df.shape[0]):
    line = df.iloc[i,:]
    
    extract_data(line)

    


