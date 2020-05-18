import mysql.connector
from datetime import datetime
from mysql.connector import Error
from mysql.connector import errorcode
from configparser import ConfigParser


ARBS = [
    'ETH',
    'LTC',
    'XRP',
    'BCH'
]

TABLES = {}

def read_config(filename='config.ini', section='mariadb'):
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db[item[0]] = item[1]
    else:
        raise Exception('{0} not found in the {1} file'.format(section, filename))
    return db

def connect():
    conn = None
    try:
        conn = mysql.connector.connect(**read_config())
        if conn.is_connected():
            print('Connected to MariaDB')
        
        cursor = conn.cursor()
        for arb in ARBS:
            table_creation = str(
                "CREATE TABLE {} ("
                "timestamp DATETIME(3) NOT NULL,"
                "regular1 DECIMAL(7,6) NULL,"
                "reverse1 DECIMAL(7,6) NULL,"
                "regular10 DECIMAL(7,6) NULL,"
                "reverse10 DECIMAL(7,6) NULL,"
                "regular25 DECIMAL(7,6) NULL,"
                "reverse25 DECIMAL(7,6) NULL,"
                "regular50 DECIMAL(7,6) NULL,"
                "reverse50 DECIMAL(7,6) NULL"
                ") ENGINE=InnoDB".format(arb)
            )
            print('Creating table {}: '.format(arb), end='')
            try:
                cursor.execute(table_creation)
            except Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists.")
                else:
                    print(err.msg)
            else:
                print('OK')
        
        data = (datetime.now(), 1, 2, 3, 4, 5, 6, 7, 8)
        insert_statement = str(
            "INSERT INTO {} "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)".format('ETH')
        )
        cursor.execute(insert_statement, data)
        conn.commit()
        cursor.close

    except Error as e:
        print(e)
    finally:
        if conn is not None and conn.is_connected():
            conn.close()
            print('Disconnected from MariaDB')


if __name__ == '__main__':
    connect()
    print(datetime.now())