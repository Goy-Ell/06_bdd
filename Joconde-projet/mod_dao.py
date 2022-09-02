from logging import exception
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from mysql.connector import errorcode
import numpy as np
from mod_jo import *
from tqdm import tqdm

from dotenv import dotenv_values

# recupere les données du dotenv
DENV = dotenv_values(".env") 
USER = DENV['USER']
PASSWORD = DENV['PASSWORD']
HOST = DENV['HOST']
DB_NAME = DENV['DB_NAME']


class Database:
    def __init__(self, user,  password, host, database=None):
        try:
            self._cnx = mysql.connector.connect(
                user = user,
                password = password,
                host = host,
                database = database)
            self._cursor = self._cnx.cursor(buffered=True)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
                # connection, cursor = new_db()
                

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    
    def connection(self):
        return self._cnx

    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self._cnx.commit()

    def close(self, commit=True):
        if commit:
            self.commit()
        self._cnx.close()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()
    
    def query1(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchone()
    
    
    
# CONNECTION -----------------------------------------

def check_db_exist():
    try:
        Database(USER,PASSWORD,HOST,DB_NAME)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
            return False
    return True



def new_db():
    dbc= Database(USER,PASSWORD,HOST) 
    # create db
    with open('joconde_new_db.sql', 'r') as fd:
        sql_file = fd.read()

        for line in sql_file.split(";"):
            try:
                dbc.execute(line)
            except IOError as msg:
                print("Command skipped: ", msg)    
    dbc.close()
    




     
    
# AUTEUR ---------------------------------------
def get_all_auteur():
    dbc= Database(USER,PASSWORD,HOST,DB_NAME)
    mySql_get_all_auteur = 'SELECT * FROM artiste'
    try:
        auteurs = dbc.query(mySql_get_all_auteur)
    except Exception as e:
        print(f"error at get_all_auteur // ",  e)
    finally:
        dbc.close()
        return auteurs if 'auteurs' in locals() else None


def get_auteur_id_by_name2(aut) :
    dbc= Database(USER,PASSWORD,HOST,DB_NAME)
    mySql_get_auteur = 'SELECT id FROM artiste WHERE nom = %s'
    try:
        res = dbc.execute(mySql_get_auteur,[aut])
        return res.fetchone()
    except Exception as e:
        print(f"error at get_auteur_id_by_name({aut}) // ",  e)
    finally:
        dbc.close()
        return id if 'id' in locals() else None


def get_auteur_id_by_name(aut,dbc) :
    mySql_get_auteur = "SELECT id FROM artiste WHERE nom = %s"
    id = dbc.query1(mySql_get_auteur,[aut])
    if not id :
        print(aut + " have no id")
    if len(id)>1:
        print( aut + " have multi id : " + id)
    else:
        return id[0]


def insert_auteur(aut):
    dbc= Database(USER,PASSWORD,HOST,DB_NAME)
    mySql_insert_auteur = 'INSERT IGNORE INTO artiste (nom) VALUES(%s)'
    try:
        dbc.execute(mySql_insert_auteur,[aut])
        id_aut = dbc.query('SELECT LAST_INSERT_ID()')
    except Exception as e:
        print(f"error at insert_auteur({aut}) // ",  e)
    finally:
        dbc.close()    
        return id_aut if 'insert_auteur' in locals() else None


def insert_many_auts(auts):
    dbc= Database(USER,PASSWORD,HOST,DB_NAME)
    mySql_insert_auteur = 'INSERT IGNORE INTO artiste (nom) VALUES(%s)'
    for aut in tqdm(auts):
        try:
            dbc.execute(mySql_insert_auteur,[aut])
        except Exception as e:
            print(f"error at insert_auteur({aut}) // ",  e)
            #TODO add to log file
            continue
    dbc.close()    



# MUSEE --------------------------------------------

def get_all_musee():
    dbc= Database(USER,PASSWORD,HOST,DB_NAME)
    mysql_get_all_musee = 'SELECT * FROM musee'
    try:
        musees = dbc.query(mysql_get_all_musee)
    except Exception as e:
        print(f"error at get_all_musee // ",  e)
    finally:
        dbc.close()  
        return musees if 'musees' in locals() else None


def get_musee(id_musee):
    dbc= Database(USER,PASSWORD,HOST,DB_NAME)
    mysql_get_musee = 'SELECT * FROM musee WHERE id = %s'
    try:
        musee = dbc.query(mysql_get_musee, [id_musee])
    except Exception as e:
        print(f"error at get_musee({id_musee}) // ",  e)
    finally:
        dbc.close()  
        return musee if 'musee' in locals() else None



def insert_musee(musee):
    dbc= Database(USER,PASSWORD,HOST,DB_NAME)
    mySql_insert_musee = 'INSERT IGNORE INTO musee (id, nom, region, dept, ville, geoloc ) VALUES(%s,%s,%s,%s,%s,%s)'
    try:
        dbc.execute(mySql_insert_musee,musee)
    except Exception as e:
        print(f"error at insert_musee({musee[0], musee[1]}) // ",  e)
    finally:
        dbc.close()    


def insert_many_musees(musees):
    dbc= Database(USER,PASSWORD,HOST,DB_NAME)
    mySql_insert_musee = 'INSERT IGNORE INTO musee (id, nom, region, dept, ville, geoloc ) VALUES(%s,%s,%s,%s,%s,%s)'
    for musee in tqdm(musees):
        try:
            dbc.execute(mySql_insert_musee,musee)
        except Exception as e:
            print(f"error at insert_musee({musee[0], musee[1]}) // ",  e)
            #TODO add to log file
            continue
    dbc.close()    



# OEUVRE -----------------------------------------------

def get_all_oeuvre():
    dbc= Database(USER,PASSWORD,HOST,DB_NAME)
    mysql_get_all_oeuvre = 'SELECT * FROM oeuvre'
    try : 
        oeuvres = dbc.query(mysql_get_all_oeuvre)
    except Exception as e:
        print(f"error at get_all_oeuvre() // ",  e)
    finally:
        dbc.close()  
        return oeuvres if 'oeuvres' in locals() else None


def get_oeuvre(id_oeuvre):
    dbc= Database(USER,PASSWORD,HOST,DB_NAME)
    mysql_get_oeuvre = 'SELECT id FROM oeuvre WHERE id = %s'
    try : 
        oeuvre = dbc.query(mysql_get_oeuvre, [id_oeuvre])
    except Exception as e:
        print(f"error at get_oeuvre({id_oeuvre}) // ",  e)
    finally:
        dbc.close()  
        return oeuvre if 'oeuvre' in locals() else None


def insert_oeuvre(oeuvre):
    dbc= Database(USER,PASSWORD,HOST,DB_NAME)
    mySql_insert_oeuvre = 'INSERT IGNORE INTO oeuvre (id, nom, denomination, sujet, domaine, musee ) VALUES(%s,%s,%s,%s,%s,%s)'
    try:
        dbc.execute(mySql_insert_oeuvre,oeuvre)
    except Exception as e:
        print(f"error at insert_oeuvre({oeuvre[0], oeuvre[1]}) // ",  e)
    finally:
        dbc.close()    
 
def insert_many_oeuvres(oeuvres) :
    dbc= Database(USER,PASSWORD,HOST,DB_NAME)
    mySql_insert_oeuvre = 'INSERT IGNORE INTO oeuvre (id, nom, denomination, sujet, domaine, musee ) VALUES(%s,%s,%s,%s,%s,%s)'
    for oeuvre in tqdm(oeuvres):
        try:
            dbc.execute(mySql_insert_oeuvre,oeuvre)
        except Exception as e:
            print(f"error at insert_oeuvre({oeuvre[0], oeuvre[1]}) // ",  e)
            #TODO add to log file
            continue
    dbc.close()      
 


 # ART_OEUV ---------------------------------------------
        
# def get_art_oeuv(id_aut, id_oeuvre):
#     mysql_check_oeuvre = 'SELECT * FROM art_oeuv WHERE oeuvre = %s and artiste = %s'
#     cursor.excute(mysql_check_oeuvre, [id_aut, id_oeuvre])
    
#     return cursor.fetchone()


def insert_art_oeuv( id_aut, id_oeuvre):
    dbc= Database(USER,PASSWORD,HOST,DB_NAME)
    mySql_insert_art_oeuv = 'INSERT IGNORE INTO art_oeuv (oeuvre, artiste) VALUES(%s,%s)'
    try:
        dbc.execute(mySql_insert_art_oeuv, [id_aut,id_oeuvre]) 
    except Exception as e:
        print(f"error at insert_art_oeuv({id_aut, id_oeuvre}) // ",  e)
    finally:
        dbc.close()
 

def insert_many_art_oeuvs(art_oeuvs):
    dbc= Database(USER,PASSWORD,HOST,DB_NAME)
    mySql_insert_art_oeuv = 'INSERT IGNORE INTO art_oeuv (artiste, oeuvre ) VALUES(%s,%s)'
    for aut, id_oeuvre in tqdm(art_oeuvs):
        # print (aut + " / " + id_oeuvre)
        id_aut=''
        try:
            id_aut = get_auteur_id_by_name(aut,dbc)
            dbc.execute(mySql_insert_art_oeuv, [id_aut,id_oeuvre]) 
            # dbc.commit()

        except Exception as e:
            print(f"error at insert_many_art_oeuvs({id_aut, id_oeuvre}) // ",  e)
            #TODO add to log file
            continue
    dbc.close()




# RECHERCHE -------------------------------------

def research_by_artiste(artiste): #, ville, jours,musee_par_j=1):
    dbc= Database(USER,PASSWORD,HOST,DB_NAME)
    mySql_research = f"""SELECT m.nom, m.ville, m.geoloc, a.nom, o.nom, o.domaine, o.musee
                        FROM musee m 
                        JOIN oeuvre o ON  o.musee = m.id
                        JOIN art_oeuv ao ON ao.oeuvre = o.id
                        JOIN artiste a ON a.id = ao.artiste
                        WHERE a.nom LIKE CONCAT('%', %s,'%')"""    
    
    df = pd.read_sql(sql= mySql_research, con=dbc.connection(), params=artiste)
    dbc.close()
    return df    
    
# from sqlalchemy import create_engine
# import pandas as pd

# db_connection_str = 'mysql+pymysql://mysql_user:mysql_password@mysql_host/mysql_db'
# db_connection = create_engine(db_connection_str)

# df = pd.read_sql('SELECT * FROM table_name', con=db_connection)












#%%

import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from mysql.connector import errorcode
import numpy as np
from mod_jo import *
from tqdm import tqdm

from dotenv import dotenv_values

DENV = dotenv_values(".env") 
USER = DENV['USER']
PASSWORD = DENV['PASSWORD']
HOST = DENV['HOST']
DB_NAME = DENV['DB_NAME']
# mySql_research = "SELECT m.nom, m.ville, m.geoloc, a.nom, o.nom, o.domaine, o.sujet, o.musee FROM musee m JOIN oeuvre o ON  m.id=o.musee JOIN art_oeuv ao ON o.id=ao.oeuvre JOIN artiste a ON ao.artiste = a.id WHERE a.nom LIKE '%gaston%' and a.nom LIKE '%chaissac%'"


# dbc = Database(USER,PASSWORD,HOST,DB_NAME)
# df = pd.read_sql(mySql_research, con=dbc.connection())
# dbc.connection().close()
# df



artiste = 'chaissac gaston' 
dbc= Database(USER,PASSWORD,HOST,DB_NAME)
mySql_research = "SELECT m.nom, m.ville, m.geoloc, a.nom, o.nom, o.domaine, o.sujet, o.musee FROM musee m JOIN oeuvre o ON  m.id=o.musee JOIN art_oeuv ao ON o.id=ao.oeuvre JOIN artiste a ON ao.artiste = a.id WHERE a.nom LIKE %s and a.nom LIKE %s "
# df = pd.read_sql(mySql_research, artiste.split(' '), con=dbc.connection())

df=pd.read_sql(mySql_research, con=dbc.connection(),  params = artiste.split(' '))


dbc.connection().close()
df

# %%
artiste.split(' ')
# %%
