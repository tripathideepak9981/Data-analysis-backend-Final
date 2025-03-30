# app/utils/db_helpers.py
import os
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from app.config import MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DATABASE
from app.state import state


def refresh_tables(connection, table_names, original_table_names) -> None:
    if connection is None:
        print("Cannot refresh tables: connection is None.")
        return
    if hasattr(connection, "cursor"):
        engine = sqlalchemy.create_engine(
            f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DATABASE}"
        )
        cursor = connection.cursor(buffered=True)
        try:
            cursor.execute("SHOW TABLES;")
            db_tables = cursor.fetchall()
        finally:
            cursor.close()
        with engine.connect() as conn:
            for tbl in db_tables:
                tbl_name = tbl[0]
                try:
                    df = pd.read_sql_query(f"SELECT * FROM `{tbl_name}`", conn)
                except Exception as e:
                    print(f"Error loading table '{tbl_name}': {e}")
                    continue
                if tbl_name not in [tn for tn, _ in table_names]:
                    table_names.append((tbl_name, df))
                else:
                    idx = next(i for i, (name, _) in enumerate(table_names) if name == tbl_name)
                    table_names[idx] = (tbl_name, df)
    else:
        dialect_name = ""
        if hasattr(connection, "engine"):
            dialect_name = connection.engine.dialect.name
        if dialect_name == "mysql":
            query = text("SHOW TABLES;")
            result = connection.execute(query)
            db_tables = result.fetchall()
            for tbl in db_tables:
                tbl_name = tbl[0]
                try:
                    df = pd.read_sql_query(f"SELECT * FROM `{tbl_name}`", connection)
                except Exception as e:
                    print(f"Error loading table '{tbl_name}': {e}")
                    continue
                if tbl_name not in [tn for tn, _ in table_names]:
                    table_names.append((tbl_name, df))
                else:
                    idx = next(i for i, (name, _) in enumerate(table_names) if name == tbl_name)
                    table_names[idx] = (tbl_name, df)
        else:
            query = text(
                "SELECT table_name FROM v_catalog.tables "
                "WHERE is_system_table = false "
                "AND table_schema NOT IN ('v_catalog','v_monitor','v_internal')"
            )
            result = connection.execute(query)
            db_tables = [(row[0],) for row in result.fetchall()]
            for tbl in db_tables:
                tbl_name = tbl[0]
                try:
                    df = pd.read_sql_query(f"SELECT * FROM `{tbl_name}`", connection)
                except Exception as e:
                    print(f"Error loading table '{tbl_name}': {e}")
                    continue
                if tbl_name not in [tn for tn, _ in table_names]:
                    table_names.append((tbl_name, df))
                else:
                    idx = next(i for i, (name, _) in enumerate(table_names) if name == tbl_name)
                    table_names[idx] = (tbl_name, df)


 
def list_tables(connection) -> list:

    try:

        # Case 1: If using a mysql.connector connection (has .cursor())

        if hasattr(connection, "cursor"):

            cursor = connection.cursor(buffered=True)

            try:

                # Use the connection's database if available; fallback to MYSQL_DATABASE from env

                db_name = getattr(connection, "database", MYSQL_DATABASE)

                cursor.execute(

                    """

                    SELECT TABLE_NAME 

                    FROM INFORMATION_SCHEMA.TABLES 

                    WHERE TABLE_SCHEMA = %s;

                    """,

                    (db_name,)

                )

                tables = cursor.fetchall()

            finally:

                cursor.close()

        # Case 2: Using an SQLAlchemy connection/engine

        elif hasattr(connection, "connect"):

            with connection.connect() as conn:

                # Determine the dialect from the connection

                dialect = ""

                if hasattr(conn, "engine"):

                    dialect = conn.engine.dialect.name.lower()

                elif hasattr(connection, "dialect"):

                    dialect = connection.dialect.name.lower()

                # Extract the database name from the connection URL; use it instead of the env variable

                db_name = connection.url.database if hasattr(connection, "url") and connection.url.database else MYSQL_DATABASE
 
                if dialect == "vertica":

                    query = text(

                        "SELECT table_name FROM v_catalog.tables "

                        "WHERE is_system_table = false "

                        "AND table_schema NOT IN ('v_catalog','v_monitor','v_internal')"

                    )

                    result = conn.execute(query)

                else:

                    query = text(

                        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = :schema"

                    )

                    result = conn.execute(query, {"schema": db_name})

                tables = result.fetchall()

        else:

            print("Unsupported connection type for listing tables.")

            return []

        return [table[0] for table in tables]

    except Exception as e:

        print(f"Error listing tables: {e}")

        return []

 

def get_personal_engine(db_type: str, host: str, user: str, password: str, database: str, port: int):
    try:
        if db_type.lower().startswith("vert"):
            port = port or 5433
            sqlalchemy.dialects.registry.register("vertica.vertica_python", "vertica_sqlalchemy.dialect", "VerticaDialect")
            engine_url = f"vertica+vertica_python://{user}:{password}@{host}:{port}/{database}"
        elif db_type.lower() == "mysql":
            port = port or 3306
            engine_url = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}?buffered=true"
        else:
            port = port or 5432
            engine_url = f"{db_type}://{user}:{password}@{host}:{port}/{database}"
        engine = sqlalchemy.create_engine(engine_url)
        with engine.connect() as connection:
            print(f"Connected to {db_type.upper()} database successfully!")
        return engine
    except Exception as e:
        print(f"Error connecting to {db_type} DB: {e}")
        return None

def connect_personal_db(db_type: str, host: str, user: str, password: str, database: str, port: int = None):
    return get_personal_engine(db_type, host, user, password, database, port)

def load_tables_from_personal_db(engine, table_list: list) -> tuple:
    loaded_tables = []
    original_tables = []
    dialect = engine.url.get_dialect().name.lower() if engine.url.get_dialect() else ""
    for tbl in table_list:
        try:
            if dialect == "vertica":
                query = f"SELECT * FROM {tbl}"
            else:
                query = f"SELECT * FROM `{tbl}`"
            df = pd.read_sql_query(query, con=engine)
            df.columns = [col.strip().replace(" ", "_").lower() for col in df.columns]
            original_df = df.copy()
            from app.utils.cleaning import clean_data
            df = clean_data(df)
            loaded_tables.append((tbl, df))
            original_tables.append((tbl, original_df))
        except Exception as e:
            print(f"Error loading table '{tbl}': {e}")
    return loaded_tables, original_tables

def disconnect_database():
    from app.state import state
    if state.get("personal_engine"):
        try:
            state["personal_engine"].dispose()
            print("Personal database disconnected.")
        except Exception as e:
            print(f"Error disconnecting personal database: {e}")
        state["personal_engine"] = None
    if state.get("mysql_connection"):
        try:
            state["mysql_connection"].close()
            print("MySQL connection disconnected.")
        except Exception as e:
            print(f"Error disconnecting MySQL connection: {e}")
        state["mysql_connection"] = None
    state["table_names"] = []
    state["original_table_names"] = []