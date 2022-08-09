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
    data = pd.read_csv(dir, delimiter=';', low_memory=False,
                       encoding='utf-8', nrows=300000)
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
    # choix des colonnes utiles
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
    code = "cp1252"

    if s is not None:
        try:
            if isinstance(s, str):
                s = s.encode(code).decode()
            else:
                s = str(s).encode(code).decode()

        except UnicodeError:
            pass
    return s


def clean_encodage(df):
    """ split df, and clean encode the wrong part

    Args:
        df (pd.DataFrame): df with all line

    Returns:
        pd.DataFrame: clean encoded df
    """
    # j'ai pas réussi a utiliser "apply(clean)"  directement aux lignes concernées. j'ai donc suivi la methode pierre avec separation de la df

    # recup de la valeur de la colonne 'base' mal encodé 
    # la colonne 'base' a une valeur unique. on s'en sert pour séparer la partie mal encodé
    bases = list(df.BASE.unique())
    # liste de colonnes que je veux nettoyer
    columns = ['Auteur', 'Titre', 'LOCA2', 'Dénomination', 'Domaine',
               'Sujet', 'BASE', 'NOMOFF', 'LOCA2', 'REGION', 'DPT', 'Ville_']

    if bases:
        # s'il n'y a qu'une valeur unique et qu'elle est mal encodé on clean toute la df
        if len(bases) == 1 and 'Ã©' in bases[0]:
            df = clean(df_cp1252, columns)

        # s'il y a plus d'une valeur unique  on separe le df mal encodé du propre, puis merge apres clean
        else:
            for base in bases:
                if 'Ã©' in base:
                    bad_base = base

            if bad_base:
                # creation de 2 db , dont 1 mal encodé
                df_cp1252 = df[df.BASE == bad_base]
                df_ok = df[df.BASE != bad_base]

            df_cp1252 = clean(df_cp1252, columns)

            # merge des 2 df
            df = pd.concat([df_cp1252, df_ok])

    return df





def clean(df, columns):
    # nettoyage colonne par colonne
    for column in columns:
        df.loc[:, column] = df[column].apply(convert)

    return df


def reduc_ligne(df):
    """_summary_

    Args:
        df (_type_): _description_

    Returns:
        _type_: _description_
    """
    # Ligne en double
    df = df.drop_duplicates(subset=['ID-notice'], keep='first')
    # Oeuvre n'ayant pas de musé
    df = df.dropna(subset=['Identifiant Museofile'])
    # suppression des nan Identifiant Museofile ne contiene aune info  de locallisation
    df = df.loc[df['Identifiant Museofile'].dropna().index]
    # Mettre des NAN
    df.fillna('nan', inplace=True)
    # df.replace('nan', np.nan, inplace=True)
    df.replace('', 'nan', inplace=True)

    return df


# --------------------------------------------------------------


def net_aut(aut):
    if isna(aut):
        aut = ['non renseigné']

    else:
        if not isinstance(aut, str):
            aut = str(aut)

        if ';' in aut:
            aut = aut.split(';')
        else:
            aut = [aut]

            # tout passer en minuscule
        aut = list(map(lambda x: x.lower(), aut))

    return aut



def extract_data(df):
    musee_keys = ['Identifiant Museofile','NOMOFF','REGION','DPT','Ville_','POP_COORDONNEES']
    oeuvre_keys=['ID-notice','Titre','Dénomination','Sujet','Domaine','Identifiant Museofile']
    musees=[]
    oeuvres=[]
    art_oeuvs=[]
    auts=[]

    for i in tqdm(range (df.shape[0])):
        line = df.iloc[i,:]
        line.fillna('nan', inplace=True)
        line['Identifiant Museofile'] = line['Identifiant Museofile'].upper()
        musees.append(clean_list(musee_keys, line))
        oeuvres.append(clean_list(oeuvre_keys, line))

    
        lst_aut = net_aut( line.Auteur)
        for aut in lst_aut:
            auts.append (aut)
            art_oeuvs.append( (aut, line['ID-notice']))

    insert_many_auts(auts)
    insert_many_musees(musees)
    insert_many_oeuvres(oeuvres)
    insert_many_art_oeuvs(art_oeuvs)




def extract_data2(df):
    musee_keys = ['Identifiant Museofile','NOMOFF','REGION','DPT','Ville_','POP_COORDONNEES']
    oeuvre_keys=['ID-notice','Titre','Dénomination','Sujet','Domaine','Identifiant Museofile']
    
    for i in tqdm(range (df.shape[0])):
        line = df.iloc[i,:]
        line.fillna('nan', inplace=True)
        line['Identifiant Museofile'] = line['Identifiant Museofile'].upper()

        insert_musee(clean_list(musee_keys, line))
        insert_oeuvre(clean_list(oeuvre_keys, line))

        lst_aut = net_aut( line.Auteur)
        for aut in lst_aut:
            aut_id = insert_auteur( aut)
            insert_art_oeuv( aut_id, line['Identifiant Museofile'])






def clean_list(keys , line):
    lst=[]
    for key in keys:
        if not key in line.keys() or not line[key] or line[key] in ['', 'nan']: 
            lst.append(None)
        else:
            lst.append(line[key])
    return lst




