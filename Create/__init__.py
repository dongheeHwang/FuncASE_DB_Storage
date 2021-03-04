import logging

import azure.functions as func

import mysql.connector
from mysql.connector import errorcode



def main(req: func.HttpRequest) -> func.HttpResponse:
    
    config = {
        'host':'172.21.10.4',
        'port':3306,
        'user':'sayun@private-sayun-mysql',
        'password':'rkskekfk1234!@#$',
        'database':'test',
        'client_flags': [ClientFlag.SSL],
        'ssl_cert': '/var/wwww/html/DigiCertGlobalRootG2.crt.pem'
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
