#%%
from turtle import distance
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
import numpy as np
from mod_jo import *
from mod_dao import * 
import joblib as jl
import os
from tqdm import tqdm
from dotenv import dotenv_values

# recupere les données du dotenv
DENV = dotenv_values(".env") 


    
    
#%%
artiste = 'chaissac gaston'
research(artiste)


# %%

# exemple
artiste = 'artiste'
jours = int
musee_par_j = int
ville_depart = 'ville'


get_musee where artiste

sort by distance

select top jours*musee_par_j





