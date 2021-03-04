import logging

import azure.functions as func


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

    # Read data
    cursor.execute("SELECT * FROM inventory;")
    rows = cursor.fetchall()
    logging.info("Read",cursor.rowcount,"row(s) of data.")

    # Print all rows
    text = ''
    for row in rows:
        text += "Data row = (%s, %s, %s)" %(str(row[0]), str(row[1]), str(row[2]))

    return func.HttpResponse(text)