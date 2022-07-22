#%%
import pandas as pd
import module as mod
import seaborn as sns
from matplotlib import pyplot as plt
import numpy as np
from module import *
from mod_dao import * 
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
df = reduc_colonne(df)
df = clean_encodage(df)
df = reduc_ligne(df)
df = clean_musee_spe(df)

    # Recup auteur
df,lst_aut = clean_auteur(df)

    # Recup musee
df_musée = clean_musee(df)

    # Recup oeuvre
df_oeuvre = clean_oeuvre(df)


#%%
insert_auteur(lst_aut)
#%%
insert_musee(df_musée)
#%%
insert_oeuvre(df_oeuvre)
#%%
insert_art_oeuv(df[['ID-notice','Auteur']])


#%%
connection.close()
# %%
