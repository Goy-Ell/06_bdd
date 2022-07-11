from logging import exception
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from mysql.connector import errorcode
import numpy as np


# def df_db():
#     db_connection_str = 'mysql+pymysql://root:toto@127.0.0.1/joconde'
#     db_connection = create_engine(db_connection_str)
#     sql_req = (
#         "SELECT  title, text_ , length_, nom, magazine  FROM articles a JOIN magazines m on a.magazine = m.id")
#     df = pd.read_sql(sql_req, con=db_connection)
#     return df


def new_db():
    connection = mysql.connector.connect(
        user='root',
        password='toto',
        host='127.0.0.1')
    cursor = connection.cursor()

# fd = open('ZooDatabase.sql', 'r')
# sqlFile = fd.read()
# fd.close()


    with open('joconde_new_db.sql', 'r') as fd:
        sql_file = fd.read()
        # print(sql_file)
        for line in sql_file.split(";"):
            # print(line)
            # print('---------')
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
            user='root',
            password='toto',
            host='127.0.0.1',
            database='joconde')
        cursor = connection.cursor()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
            connection, cursor = new_db()

    return connection, cursor 


def check(title):
    connection, cursor = connect_db()
    try:
        cursor.execute("SELECT id FROM articles WHERE title = %s ", [title])
        return cursor.fetchone()

    finally:
        connection.close()


def insert_oeuvre(lst_articles):
    """insertion oeuvre in db

    Args:
        lst_articles (list): list of articles [link, title,article,length,magazine]
    """
    connection, cursor = connect_db()
    try:
        mySql_insert_article = 'INSERT IGNORE INTO articles (link, title, length_,text_,magazine) VALUES(%s,%s,%s,%s,%s)'

        for line in lst_articles:
            try:
                cursor.execute("SELECT id FROM magazines WHERE nom = %s ", [
                               line[4]])  # recupere l'id magazine par le nom
                id = cursor.fetchone()[0]
                cursor.execute(mySql_insert_article,
                               (line[0], line[1], line[3], line[2], id))
            except exception as error:
                print(error, "error at ", line[4], line[1])
                continue

    finally:
        connection.commit()
        connection.close()



def insert_auteur(lst_aut):
    """insertion auteur in db

    Args:
        lst_aut (list): list of auteurs [name]
    """
    connection, cursor = connect_db()
    try:
        mySql_insert_auteur = 'INSERT IGNORE INTO artiste (nom) VALUES(%s)'

        for line in lst_aut:
            try:
                cursor.execute(mySql_insert_auteur,(line,))
            except Exception as error:
                print(error)
                continue

    finally:
        connection.commit()
        connection.close()



def insert_musee(df):
    """insertion musee in db

    Args:
        df (pd.Dataframe): list of musee [id, nom, region, dept, ville, geoloc]
    """
    connection, cursor = connect_db()
    mySql_insert_musee = 'INSERT IGNORE INTO musee (id, nom, region, dept, ville, geoloc ) VALUES(%s,%s,%s,%s,%s,%s)'
    df=df.fillna('')
    for i in range(len(df)):
        line=df.iloc[i,:].values.tolist()
    
        try:
            cursor.execute(mySql_insert_musee,(line[0],line[1],line[2],line[3],line[4],line[5],))
        except Exception as e:
            print( e,
                "error insert for id: ",
                    line[0]
                )
            continue

        # finally:
    connection.commit()
    connection.close()


def insert_oeuvre(df):
    """insertion oeuvre in db

    Args:
        df (pd.Dataframe): df_oeuvre [id, nom, denomination, sujet, domaine, musee]
    """
    connection, cursor = connect_db()
    mySql_insert_oeuvre = 'INSERT IGNORE INTO oeuvre (id, nom, denomination, sujet, domaine, musee ) VALUES(%s,%s,%s,%s,%s,%s)'
    df=df.fillna('')
    for i in range(len(df)):
        line=df.iloc[i,:].values.tolist()
        try:
            cursor.execute(mySql_insert_oeuvre,(line[0],line[1],line[2],line[3],line[4],line[5]))
        except Exception as e:
            print( e,
                "error insert for id: ",
                    line[0]
                )
            continue

        # finally:
    connection.commit()
    connection.close()


def insert_art_oeuv(df):
    """insertion art_oeuv in db

    Args:
        df (pd.Dataframe): df ['ID-notice','Auteur']
    """
    connection, cursor = connect_db()
    mySql_insert_art_oeuv = 'INSERT IGNORE INTO art_oeuv (ID-notice, Auteur) VALUES(%s,%s)'
    mySql_select_art_id = "SELECT id FROM artiste WHERE nom = %s "


    for i in range(len(df)):
        line=df.iloc[i,:].values.tolist()
        id_oeuvre=line[0]
        auteurs = line[1]



        for aut in auteurs:
            try : 
                cursor.execute(mySql_select_art_id, [aut])  # recupere l'id artiste par le nom
                id_aut = cursor.fetchone()[0]

                try:
                    cursor.execute(mySql_insert_art_oeuv,(id_oeuvre,id_aut))
                except Exception as e:
                    print( e,
                        "error insert id: ",
                            id_oeuvre,id_aut
                        )
                    continue

            except Exception as e:
                print (e, "error select id from artiste for ", aut)
                continue

        # finally:
    connection.commit()
    connection.close()    



def df_from_sql():
    """create df from db

    Returns:
        df: _description_
    """
    db_connection_str = 'mysql+pymysql://root:toto@127.0.0.1/projet_article'
    db_connection = create_engine(db_connection_str)

    df = pd.read_sql('SELECT * FROM articles', con=db_connection)
    # db_connection.close
    return df


