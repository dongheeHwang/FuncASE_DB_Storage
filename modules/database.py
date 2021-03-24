import mysql.connector
from modules.resource import conf

from modules import cryptoUtil

class Database:
    cursor = None

    def __init__(self):
        # self.cursor = self.db.cursor(pymysql.cursors.DictCursor)
        # self.db = mysql.connector.connect(**conf.db_config)
        config = {
            'host' : cryptoUtil.decrypt(conf.db_config['host']),
            'user' : cryptoUtil.decrypt(conf.db_config['user']),
            'password' : cryptoUtil.decrypt(conf.db_config['password']),
            'database' : cryptoUtil.decrypt(conf.db_config['database']),
            'port' : cryptoUtil.decrypt(conf.db_config['port']),
            'client_flags': conf.db_config['client_flags']
        }
        self.db = mysql.connector.connect(**config)
        self.cursor = self.db.cursor(dictionary=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()

    def execute(self, query, args={}):
        self.cursor.execute(query, args)

    def selectOne(self, query, args={}):
        self.cursor.execute(query, args)
        row = self.cursor.fetchone()
        return row

    def selectAll(self, query, args={}):
        self.cursor.execute(query, args)
        row = self.cursor.fetchall()
        return row

    def commit(self):
        self.db.commit()