from logging import exception
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from mysql.connector import errorcode
import numpy as np
from module2 import *
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

    @property
    def connection(self):
        return self._cnx

    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self.connection.commit()

    def close(self, commit=True):
        if commit:
            self.commit()
        self.connection.close()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()

# CONNECTION -----------------------------------------

def check_db():
    try:
        dbc= Database(USER,PASSWORD,HOST,DB_NAME)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
            new_db()
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

def get_auteur_id_by_name2(aut) :
    dbc= Database(USER,PASSWORD,HOST,DB_NAME)
    mySql_get_auteur = 'SELECT id FROM artiste WHERE nom = %s'
    try:
        id = dbc.query(mySql_get_auteur,[aut])
    except Exception as e:
        print(f"error at get_auteur_id_by_name({aut}) // ",  e)
    finally:
        dbc.close()
    return id if 'id' in locals() else None

def get_auteur_id_by_name(aut,dbc) :
    mySql_get_auteur = 'SELECT id FROM artiste WHERE nom = %s'
    id_a = dbc.query(mySql_get_auteur,[aut])
    return id_a 


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
    for aut in auts:
        try:
            dbc.execute(mySql_insert_auteur,[aut])
        except Exception as e:
            print(f"error at insert_auteur({aut}) // ",  e)
            #TODO add to log file
            continue
    dbc.close()    



# MUSEE --------------------------------------------

def get_musee(id_musee):
    dbc= Database(USER,PASSWORD,HOST,DB_NAME)
    mysql_check_musee = 'SELECT * FROM musee WHERE id = %s'
    try:
        musee = dbc.query(mysql_check_musee, [id_musee])
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
    for musee in musees:
        try:
            dbc.execute(mySql_insert_musee,musee)
        except Exception as e:
            print(f"error at insert_musee({musee[0], musee[1]}) // ",  e)
            #TODO add to log file
            continue
    dbc.close()    



# OEUVRE -----------------------------------------------

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
    for oeuvre in oeuvres:
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
    mySql_insert_art_oeuv = 'INSERT IGNORE INTO art_oeuv (oeuvre, artiste) VALUES(%s,%s)'
    for aut, id_oeuvre in art_oeuvs:
        try:
            id_aut = get_auteur_id_by_name(aut,dbc)
            if id_aut:
                dbc.execute(mySql_insert_art_oeuv, [id_aut,id_oeuvre]) 
        except Exception as e:
            print(f"error at insert_art_oeuv({id_aut, id_oeuvre}) // ",  e)
            #TODO add to log file
            continue
    dbc.close()
