import logging

import azure.functions as func

import mysql.connector
from mysql.connector import errorcode


def main(req: func.HttpRequest) -> func.HttpResponse:
    config = {
        # 'host':'z-shb-krc-edm-dev-mrd-edm-db-01.mariadb.database.azure.com',
        'host':'10.2.103.5',
        'user':'azureadmin@z-shb-krc-edm-dev-mrd-edm-db-01',
        'password':'rkskekfk1234!@#$',
        'database':'testdb'
    }

    try:
        conn = mysql.connector.connect(**config)
        logging.info("Connection established")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.info("Something is wrong with the user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.info("Database does not exist")
        else:
            logging.info(err)
    else:
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT * FROM company")
            rows = cursor.fetchall()
            text = ''
            for row in rows:
                text += f'{row[0]} {row[1]} {row[2]}\n'
        except Exception as e:
            logging.info(e)

        return func.HttpResponse(text)