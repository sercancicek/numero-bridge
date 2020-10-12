import os
from datetime import datetime
from io import StringIO
import pandas as pd
import psycopg2
import pyodbc

CONNECTION_STRING = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={os.getenv("HOST_NAME")};DATABASE={os.getenv("DB_NAME")};UID={os.getenv("USER_NAME")};PWD={os.getenv("PASSWORD")}'
TEMP_TABLE_NAME = 'tempsapsaptecdoc'


def read_data():
    # Create/Open a Connection to Microsoft's SQL Server
    conn = pyodbc.connect(CONNECTION_STRING)
    sql = "SELECT TipoParte, ItemCode, ItemName, Marca, ItemPrice, ExistenciaMinerva, ExistenciaZapopan, ExistenciaCDMX, ExistenciaQRO, ExistenciaMTY FROM dbo.XXSINTECDOC"
    df = pd.read_sql(sql, conn)
    # Loop through the result set
    print(df)

    # Close the Connection
    conn.close()
    insert_df(df, TEMP_TABLE_NAME)


def connect_to_target_db():
    """ Connect to the PostgreSQL database server """
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except Exception as error:
        print(f'connect_to_target_db: {error}')
        return None

    print("Connection successful...")
    return conn


def get_drop_query(temp_table):
    return f'DROP TABLE IF EXISTS "{temp_table}"';


def get_insert_temp_qurey(temp_table):
    print(f'CREATE TABLE "{temp_table}" AS TABLE "SAPTECDOC" WITH NO DATA')
    return f'CREATE TABLE "{temp_table}" AS TABLE "SAPTECDOC" WITH NO DATA';


def get_swap_tables_sql(tmp_table):
    return f"""
        TRUNCATE "SAPTECDOC";
        INSERT INTO "SAPTECDOC"
        SELECT *
        FROM "{tmp_table}";
        DROP TABLE "{tmp_table}";
    """


def get_buffer(df):
    # save data frame to an in memory buffer
    try:
        buffer = StringIO()
        df.to_csv(buffer, sep="|", index=False, header=False)
        buffer.seek(0)
        return buffer
    except Exception as e:
        print(f'get_buffer ERROR: {e}')
        return None


def insert_df(df, tmp_table):
    conn = connect_to_target_db()
    if conn is None:
        print('Connection error to target database(numero)')
        return

    buffer = get_buffer(df)
    if buffer is None:
        print('an error occured at get buffer event')
        return

    cursor = conn.cursor()
    cursor.execute(get_drop_query(tmp_table))
    conn.commit()
    cursor = conn.cursor()
    res = insert_to_temp(buffer, conn, cursor, tmp_table)
    if res:
        if not swap_tables(conn, cursor, tmp_table):
            return
    else:
        return

    print("copy_from_stringio() done")
    cursor.close()


def swap_tables(conn, cursor, tmp_table):
    try:
        print("......swapping")
        cursor.execute(get_swap_tables_sql(tmp_table))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        # cursor.close()
        print(f'swap_tables ERROR: {e}')
        return False


def insert_to_temp(buffer, conn, cursor, tmp_table):
    try:
        cursor.execute(get_insert_temp_qurey(tmp_table))
        cursor.copy_from(buffer, tmp_table, sep="|")
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        # cursor.close()
        print(f'insert_to_temp ERROR: {e}')
        return False




params_dic = {
    "host": os.getenv('NUMEROE_DB_HOST'),
    "database": os.getenv('NUMEROE_DB_NAME'),
    "user": os.getenv('NUMEROE_DB_USER'),
    "password": os.getenv('NUMEROE_DB_PASS'),
    "port": os.getenv('NUMEROE_DB_PORT'),
}


