#%%
# from turtle import distance
from mod_jo import *
from dao import * 


# Creation DB
if not check_db_exist():
    new_db()
print("DB OK")      

# Recuperation données
data_dir = 'base-joconde-extrait.csv'
df = get_data(data_dir)
print("get_data OK")

# Nettoyage donées
df = clean_data(df)
print("clean_data OK")
#%%
# Insertion données dans la db
insert_data(df)
print('insert_data OK')




# %%
