import logging

import azure.functions as func

import mysql.connector

import json
import os
import zipfile
import traceback
import shutil
import tarfile
import datetime
import sys

from . import collector
from modules import utils
from modules import common
from modules import scrapingUtil
from modules import database as db
from modules.resource import conf
from modules.sftpUtil import Ftp
from modules.requestUtil import API
from modules.resource import query
from datetime import datetime, timedelta
from mysql.connector import errorcode

from modules import utils
from modules import telegramUtil

# from modules import requestUtil




def main(req: func.HttpRequest) -> func.HttpResponse:
    config = {
        # 'host':'z-shb-krc-edm-dev-mrd-edm-db-01.mariadb.database.azure.com',
        'host':'hdhdemoserver.mariadb.database.azure.com',
        'user':'azureadmin@hdhdemoserver',
        'password':'rkskekfk1234!@#$',
        # 'database':'testdb'
    }

    logtext = ''
    try:
        conn = mysql.connector.connect(**config)
        logtext += "Connection established"
        logging.info("Connection established")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logtext += "Something is wrong with the user name or password"
            logging.info("Something is wrong with the user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logtext += "Database does not exist"
            logging.info("Database does not exist")
        else:
            #logtext += err
            logging.info(err)
    else:
        cursor = conn.cursor()

        text = ''
        try:
            cursor.execute("SELECT * FROM company")
            rows = cursor.fetchall()
            for row in rows:
                text += f'{row[0]} {row[1]} {row[2]}\n'
        except Exception as e:
            # logtext += e.toString()
            logging.info(e)
        logtext += text
        return func.HttpResponse(logtext)