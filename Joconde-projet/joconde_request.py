#%%
from turtle import distance
# import pandas as pd
# import seaborn as sns
# from matplotlib import pyplot as plt
# import numpy as np
from mod_jo import *
from mod_dao import * 
# import joblib as jl
# import os
# from tqdm import tqdm

import pandas as pd
# import module as mod
# import seaborn as sns
# from matplotlib import pyplot as plt
import numpy as np
# from module import *
# from mod_dao import * 
# import joblib as jl
# import os
import mysql.connector
from geopy.distance import geodesic

from dotenv import load_dotenv
from dotenv import dotenv_values
# recupere les données du dotenv
DENV = dotenv_values(".env") 


    
    
#%%
#TODO input
artiste = ['picasso']
artiste = list(map(lambda x: x.lower(), artiste))
nb_musee = 2
# ville_depart = 'Paris'


#%%
df=research_by_artiste(artiste)
df = df.set_axis(['nom_musee', 'ville', 'geoloc', 'nom_artiste', 'nom_ouevre','domain', 'id_musee'], axis=1, inplace=False)
df=df.iloc[df.groupby('nom_musee').nom_musee.transform('size').mul(-1).argsort(kind='mergesort')]
df


# %%
dft= df.groupby(['id_musee','nom_musee','geoloc','ville']).count().sort_values('domain',ascending=False)['domain'].reset_index().rename(columns={'domain':'count'})
dft=dft.iloc[:nb_musee,:]

for i in range(1,nb_musee):
    dft.loc[i,'distance'] = int(geodesic(tuple(dft.loc[i,'geoloc'].split(',')),tuple(dft.loc[i-1,'geoloc'].split(','))).kilometers)
dft  

#%%
print(f"Le {dft.loc[0,'nom_musee']} ({dft.loc[0,'ville']}) detient {dft.loc[0,'count']} oeuvre(s) de {artiste[0]}.")
for i in range(1,dft.shape[0]):
    print(f"Le {dft.loc[i,'nom_musee']} ({dft.loc[i,'ville']}), à {dft.loc[i,'distance']} km du précedent musée en detient {dft.loc[i,'count']}.")





# %%
