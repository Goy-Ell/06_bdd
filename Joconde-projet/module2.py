import seaborn as sns
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
import numpy as np
from logging import exception
from pandas import isna
from mod_dao2 import *




def analyse_df(dataframe):
    """analyse de la df

    Args:
        dataframe (pd.DataFrame): 
    """
    print(dataframe.shape)
    print('------------------------------------')
    print(dataframe.dtypes)
    print('------------------------------------')
    print(dataframe.isna().sum())
    print('------------------------------------')
    sns.heatmap(dataframe.isnull(), yticklabels=False,
                cbar=False, cmap='viridis')



# Recup data

def get_data(dir):
    """return a df from csv directory with ';' delimiter and utf-8 encoder

    Args:
        dir (string): path du .csv
    """
    data = pd.read_csv(dir, delimiter=';',low_memory=False ,encoding='utf-8', nrows=300000)
    data.dataframeName = dir
    
    return data


# Nettoyage

def reduc_colonne(df):
    """drop df column . can be chose manualy in the function. 

    Args:
        df pd.DataFrame: 

    Returns:
        pd.DataFrame: df with onnly needed columns
    """
    #choix des colonnes utiles
    col = ['ID-notice',
        'Auteur',
        'Titre', 
        'Dénomination',
        'Domaine', 
        'Sujet',
        # 'POP_CONTIENT_GEOLOCALISATION', 
        # 'Statut juridique',
        # 'PRODUCTEUR',
        # "Numéro de l'objet",
        'BASE',
        'CONTIENT_IMAGE', 
        # 'Lieu de conservation', 
        'Identifiant Museofile', 
        # 'Source et date de la notice', 
        'NOMOFF',
        'LOCA2', 
        'REGION', 
        'DPT', 
        'Ville_',
        'POP_COORDONNEES',
        # 'IMAGE', 
        # 'Label Musée de France', 
        # 'Matériaux-techniques',
        # 'Dimensions', 
        # "Date d'import", 
        # 'Période de création', 
        # "Date d'acquisition", 
        # 'APTN', 
        # 'PHOT', 
        # "Précisions sur l'auteur", 
        # 'Ecole', 
        # 'Date de mise à jour', 
        # 'MSGCOM'
    ]
    return df[col]



def convert(s):
    """ decode and encode with good encoding type

    Args:
        s (string): wrong encoding string


    Returns:
        string: clean s
    """
    code="cp1252"

    if s is not None:
        try:
            if isinstance(s,str):
                s = s.encode(code).decode()
            else : 
                s = str(s).encode(code).decode()
                
        except UnicodeError:
            pass
    return s



def clean_encodage(df):
    """_summary_

    Args:
        df (pd.DataFrame): df with wrong encoding

    Returns:
        pd.DataFrame: clean encoded df
    """
    # recup de la valeur de la colonne 'base' mal encodé (c'est le string le plus long)

    a=list(df.BASE.unique())

    if len(a)>1:
        a.sort(key = len, reverse=True)

        # creation de 2 db , dont 1 mal encodé
        df_cp1252=df[df.BASE == a[0]]
        df_ok=df[df.BASE == a[1]]

        columns = ['Auteur','Titre','LOCA2', 'Dénomination', 'Domaine', 'Sujet','BASE','NOMOFF', 'LOCA2','REGION', 'DPT', 'Ville_' ]
        
        for column in columns:
            df_cp1252.loc[:, column] = df[column].apply(convert)

        df = pd.concat([df_cp1252, df_ok])

    return df


def reduc_ligne(df):
    """_summary_

    Args:
        df (_type_): _description_

    Returns:
        _type_: _description_
    """
    # Ligne en double
    df = df.drop_duplicates(subset=['ID-notice'],keep='first')
    # Oeuvre n'ayant pas de musé 
    df = df.dropna(subset=['Identifiant Museofile'])
    #suppression des nan Identifiant Museofile ne contiene aune info  de locallisation
    df = df.loc[df['Identifiant Museofile'].dropna().index]
    # Mettre des NAN
    df.replace('nan',np.nan, inplace=True)
    df.replace('',np.nan, inplace=True)

    return df




# --------------------------------------------------------------




def net_aut(aut):
    if isna(aut) :
        aut = ['non renseigné']
    
    else:
        if not isinstance(aut, str):
            aut=str(aut)
            
        if ';' in aut:
            aut = aut.split(';')
        else:
            aut=[aut]

            #tout passer en minuscule
        aut = list(map(lambda x: x.lower(), aut))
        
    return aut





def extract_data(cursor, line):

    connection, cursor = connect_db()   

    insert_musee(cursor, line['Identifiant Museofile'].upper(),line['NOMOFF'],line['REGION'],line['DPT'],line['Ville_'],line['POP_COORDONNEES'])

    insert_oeuvre(cursor, line['ID-notice'],line['Titre'],line['Dénomination'],line['Sujet'],line['Domaine'],line['Identifiant Museofile'])

    lst_aut = net_aut(cursor, line.Auteur)
    for aut in lst_aut:
        aut_id = insert_auteur(cursor, aut)
        insert_art_oeuv(cursor, aut_id, line['Identifiant Museofile'])


    connection.commit()
    connection.close()










