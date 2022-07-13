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

# connection, cursor = connect_db()
# cursor.execute("SELECT id FROM artiste WHERE nom = %s ", ['sine '])  # recupere l'id artiste par le nom
# id_aut = cursor.fetchone()[0]
# print(id_aut)



# insert_art_oeuv(df[['ID-notice','Auteur']])
new_db()




# line = [
#  'B9001',
#  'Bibliothèque de l’Assemblée Nationale',
#  'Ile-de-France',
#  '75007',
#  'Paris, Assemblée nationale',
#  '48.86215,2.31897'
#  ]
# mySql_insert_musee = 'INSERT IGNORE INTO musee (id, nom, region, dept, ville, geoloc ) VALUES(%s,%s,%s,%s,%s,%s)'
# cursor.execute(mySql_insert_musee,(line[0],line[1],line[2],line[3],line[4],line[5],))






# df_musee=pd.DataFrame({})
# .to_sql(name, con, schema=None, if_exists='fail', index=True, index_label=None, chunksize=None, dtype=None, method=None)
# lst_aut=['georges','marcel']

# connection, cursor = connect_db()
# mySql_insert_auteur = 'INSERT IGNORE INTO artiste (nom) VALUES(%s)'

# for aut in lst_aut:
#     cursor.execute(mySql_insert_auteur,(aut,))
# connection.commit()
# connection.close()

# insert_auteur(lst_aut)





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
# Insertion des donnees en Data Base

# connection, cursor = connect_db()


insert_auteur(lst_aut)
insert_musee(df_musée)
insert_oeuvre(df_oeuvre)
insert_art_oeuv(df[['ID-notice','Auteur']])



# %%
