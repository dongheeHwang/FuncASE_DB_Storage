import logging

import azure.functions as func

import mysql.connector
from mysql.connector import errorcode



def main(req: func.HttpRequest) -> func.HttpResponse:
    
    config = {
        'host':'private-sayun-mysql.privatelink.mysql.database.azure.com',
        'user':'sayun@private-sayun-mysql',
        'password':'rkskekfk1234!@#$'
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

    databaseName = "test"
    isExists = False
    cursor.execute("SHOW DATABASES")

    for x in cursor:
        if x == databaseName:
            isExists = True
            break

    logging.info(isExists)
    if isExists == False:
        cursor.execute("CREATE DATABASE " + databaseName)

    # Drop previous table of same name if one exists
    cursor.execute("DROP TABLE IF EXISTS inventory;")
    logging.info("Finished dropping table (if existed).")

    # Create table
    cursor.execute("CREATE TABLE inventory (id serial PRIMARY KEY, name VARCHAR(50), quantity INTEGER);")
    logging.info("Finished creating table.")

    # Insert some data into table
    cursor.execute("INSERT INTO inventory (name, quantity) VALUES (%s, %s);", ("banana", 150))
    logging.info("Inserted",cursor.rowcount,"row(s) of data.")
    cursor.execute("INSERT INTO inventory (name, quantity) VALUES (%s, %s);", ("orange", 154))
    logging.info("Inserted",cursor.rowcount,"row(s) of data.")
    cursor.execute("INSERT INTO inventory (name, quantity) VALUES (%s, %s);", ("apple", 100))
    logging.info("Inserted",cursor.rowcount,"row(s) of data.")

    # Cleanup
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Done.")

    return func.HttpResponse("OK")
