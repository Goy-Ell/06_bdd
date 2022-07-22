from logging import exception
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from mysql.connector import errorcode
import numpy as np

from dotenv import dotenv_values

#recupere les données du dotenv
config = dotenv_values(".env") 



# CONNECTION -----------------------------------------

def new_db():
    # connect server
    connection = mysql.connector.connect(
        user=config['USER'],
        password=config['PASSWORD'],
        host=config['HOST']
        )
    cursor = connection.cursor()
    
    # create db
    with open('joconde_new_db.sql', 'r') as fd:
        sql_file = fd.read()

        for line in sql_file.split(";"):
            try:
                cursor.execute(line)
            except IOError as msg:
                print("Command skipped: ", msg)

    return connection, cursor



def connect_db():
    """connection to DB

    Returns:
        _type_: connection , cursor
    """

    try:
        connection = mysql.connector.connect(
            user = config['USER'],
            password = config['PASSWORD'],
            host = config['HOST'],
            database = config['DB_NAME'])
        cursor = connection.cursor(buffered=True)
        return connection, cursor 

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
            connection, cursor = new_db()
            return connection, cursor 

    
    
    
    
# AUTEUR ---------------------------------------

def get_auteur_id_by_name(cursor,aut) :
    mySql_get_auteur = 'SELECT id FROM artiste WHERE nom = %s'
    
    return cursor.execute(mySql_get_auteur,[aut])


def insert_auteur(cursor, aut):
    mySql_insert_auteur = 'INSERT IGNORE INTO artiste (nom) VALUES(%s)'
    id_aut = get_auteur_id_by_name(cursor,aut)
    if not id_aut:
        cursor.execute(mySql_insert_auteur,[aut])
        cursor.execute('SELECT LAST_INSERT_ID()')
        id_aut = cursor.fetchone()
        
    return id_aut




# MUSEE --------------------------------------------

def check_musee(cursor, id_musee):
    mysql_check_musee = 'SELECT id FROM musee WHERE id = %s'
    cursor.excute(mysql_check_musee, [id_musee])
    
    return cursor.fetchone()
    
    
def insert_musee(cursor, id_musee, nom, region, dept, ville, geoloc):
    mySql_insert_musee = 'INSERT IGNORE INTO musee (id, nom, region, dept, ville, geoloc ) VALUES(%s,%s,%s,%s,%s,%s)'
    if not check_musee(cursor, id_musee):
        cursor.execute(mySql_insert_musee,(id_musee, nom, region, dept, ville, geoloc))




# OEUVRE -----------------------------------------------

def check_oeuvre(cursor, id_oeuvre):
    mysql_check_oeuvre = 'SELECT id FROM oeuvre WHERE id = %s'
    cursor.excute(mysql_check_oeuvre, [id_oeuvre])
    
    return cursor.fetchone()


def insert_oeuvre(cursor, id_oeuvre, nom, denomination, sujet, domaine, musee):
    mySql_insert_oeuvre = 'INSERT IGNORE INTO oeuvre (id, nom, denomination, sujet, domaine, musee ) VALUES(%s,%s,%s,%s,%s,%s)'
    if not check_oeuvre(cursor, id):
        cursor.execute(mySql_insert_oeuvre,(id_oeuvre, nom, denomination, sujet, domaine, musee))

 
 
 
 # ART_OEUV ---------------------------------------------
        
def check_art_oeuv(cursor, id_aut, id_oeuvre):
    mysql_check_oeuvre = 'SELECT * FROM art_oeuv WHERE oeuvre = %s and artiste = %s'
    cursor.excute(mysql_check_oeuvre, [id_aut, id_oeuvre])
    
    return cursor.fetchone()


def insert_art_oeuv(cursor, id_aut, id_oeuvre):
    mySql_insert_art_oeuv = 'INSERT IGNORE INTO art_oeuv (oeuvre, artiste) VALUES(%s,%s)'
    if not check_art_oeuv(cursor, id_aut, id_oeuvre):
        cursor.execute(mySql_insert_art_oeuv, [id_aut,id_oeuvre]) 
        
 


