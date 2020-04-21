import mysql.connector
from datetime import datetime
from mysql.connector import Error
from mysql.connector import errorcode
from database_readconfig import read_config

ARBS = [
    'ETH',
    'LTC',
    'XRP',
    'BCH'
]
DB_NAME = 'crypto_triangle'

TABLES = {}

for arb in ARBS:
    TABLES[arb] = "CREATE TABLE {} (timestamp TIMESTAMP NOT NULL, regular DECIMAL(11,10) NOT NULL, reverse DECIMAL(11,10) NOT NULL) ENGINE=InnoDB".format(arb)
    # """
    #     CREATE TABLE {} (
    #         timestamp TIMESTAMP NOT NULL,
    #         regular DECIMAL(11,10) NOT NULL,
    #         reverse DECIMAL(11,10) NOT NULL
    #     ) ENGINE=InnoDB
    # """

        # "CREATE TABLE {} (".format(arb)
        # "timestamp TIMESTAMP NOT NULL,"
        # "regular DECIMAL(11,10) NULL,"
        # "reverse DECIMAL(11,10)"
        # ") ENGINE=InnoDB"

# print(TABLES)

def connect():
    conn = None
    try:
        conn = mysql.connector.connect(**read_config())
        if conn.is_connected():
            print('Connected to MySQL database')
        
        cursor = conn.cursor()

        try:
            cursor.execute("USE {}".format(DB_NAME))
            print('Using {} DB'.format(DB_NAME))
        except:
            print('DB {} does not exist'.format(DB_NAME))
        
        for table_name in TABLES:
            table_description = TABLES[table_name]
            print('Creating table {}: '.format(table_name), end='')
            try:
                cursor.execute(table_description)
            except Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists.")
                else:
                    print(err.msg)
            else: 
                print('OK')

    except Error as e:
        print(e)

    finally:
        if conn is not None and conn.is_connected():
            conn.close()


if __name__ == '__main__':
    connect()