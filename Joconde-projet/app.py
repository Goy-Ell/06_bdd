#%%

from mod_jo import *
from dao import * 
from geopy.distance import geodesic

# from dotenv import dotenv_values

# recupere les données du dotenv
DENV = dotenv_values(".env") 


    

artiste = input('quel artiste souhaitez vous voir ?')
artiste = [artiste]
artiste = list(map(lambda x: x.lower(), artiste))

nb_musee=0
while  not nb_musee:
    nb_musee = int(input('combien de musée voulez vous visiter ?'))


# recuperation des données en bdd concernant l artiste
df=research_by_artiste2(artiste)
df=df.iloc[df.groupby('m_nom').m_nom.transform('size').mul(-1).argsort(kind='mergesort')]
df

# classement des musées et recuperation du nombre souhaité
dft= df.groupby(['musee','m_nom','geoloc','ville']).count().sort_values('domaine',ascending=False)['domaine'].reset_index().rename(columns={'domaine':'count'})
dft=dft.iloc[:nb_musee,:]

# calcule distance avec le précédent musée
for i in range(1,nb_musee):
    dft.loc[i,'distance'] = int(geodesic(tuple(dft.loc[i,'geoloc'].split(',')),tuple(dft.loc[i-1,'geoloc'].split(','))).kilometers)
dft  


print(f"Le {dft.loc[0,'m_nom']} ({dft.loc[0,'ville']}) detient {dft.loc[0,'count']} oeuvre(s) de {artiste[0]}.")
for i in range(1,dft.shape[0]):
    print(f"Le {dft.loc[i,'m_nom']} ({dft.loc[i,'ville']}), à {dft.loc[i,'distance']} km du précedent musée en detient {dft.loc[i,'count']}.")





# %%
