import logging

import azure.functions as func

import mysql.connector
from mysql.connector import errorcode


def main(req: func.HttpRequest) -> func.HttpResponse:
    config = {
        'host':'private-sayun-mysql.privatelink.mysql.database.azure.com',
        'user':'sayun@private-sayun-mysql',
        'password':'rkskekfk1234!@#$',
        'database':'test'
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
            cursor.execute("SELECT * FROM inventory")
            rows = cursor.fetchall()
            text = 'okok'
            for row in rows:
                text += f'{row[0]} {row[1]} {row[2]}\n'
        except Exception as e:
            logging.info(e)

        return func.HttpResponse(text)