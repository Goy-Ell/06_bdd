import seaborn as sns
import pandas as pd
# import module as mod
import seaborn as sns
from matplotlib import pyplot as plt
import numpy as np
from logging import exception
from pandas import isna


def test():
    print('test ok')


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
    """ decode en encode with good encoding type

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
    # recup de la valeur base mal encodé (c'est le string le plus long)

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

def normalize(auts):
    clean_auts=[]
    for aut in auts : 
        if '(' in aut:
            aut=aut.split('(')[0]
        if aut:
            clean_auts.append(aut.lower())

    return  clean_auts

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

        aut = normalize(aut) 
        
    return aut



def clean_auteur(df):
    df.Auteur=df.Auteur.apply(net_aut)

    lst=df.Auteur[df.Auteur.notna()].values.tolist()

    aut_lst = []
    for auts in lst:
        aut_lst.extend(auts)
    aut_lst=list(set(aut_lst))

    return df, aut_lst



def clean_musee_spe(df):
    df.drop(df[df['Identifiant Museofile']== '0000'].index,inplace=True)
    df['Identifiant Museofile'].replace('5027','M5027',inplace=True)
    df['Identifiant Museofile']=df['Identifiant Museofile'].apply(lambda x: x.upper())
    return(df)

def clean_oeuvre(df):
    #TODO nettoyage
    return df[['ID-notice','Titre','Dénomination','Sujet','Domaine','Identifiant Museofile']]








# Recup musée
#%%
def clean_musee(df):
    """_summary_

    Args:
        df (_type_): _description_

    Returns:
        _type_: _description_
    """
    df2=df.copy()
    df2 = df2[['Identifiant Museofile','NOMOFF','REGION','DPT','Ville_','POP_COORDONNEES']]

    # ordonne ceux qui ont le plus de données en premier et supprime les doublons
    df2['nb_nan']=df2.isna().sum(axis=1)
    df2=df2.sort_values('nb_nan')
    df2=df2.drop_duplicates(subset='Identifiant Museofile',keep='first')
    df2 = df2.drop(columns=['nb_nan'])
    df2 = df2.sort_values('Identifiant Museofile')

    return df2


#%%



