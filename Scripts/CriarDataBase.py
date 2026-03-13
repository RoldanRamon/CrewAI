import sqlite3
import json

def create_connection(db_file="Database/empresas.db"):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        print(f"Erro ao conectar ao banco: {e}")
    return conn

def create_table(conn):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :return:
    """
    sql_create_empresas_table = """ CREATE TABLE IF NOT EXISTS empresas (
                                        id integer PRIMARY KEY AUTOINCREMENT,
                                        title text NOT NULL,
                                        address text,
                                        description text,
                                        latitude real,
                                        longitude real,
                                        hours text,
                                        phone text,
                                        website text,
                                        directions text,
                                        rating real,
                                        reviews integer,
                                        type text,
                                        place_id text UNIQUE,
                                        provider_id text,
                                        position integer
                                    ); """

    try:
        c = conn.cursor()
        c.execute(sql_create_empresas_table)
    except sqlite3.Error as e:
        print(f"Erro ao criar tabela: {e}")

def save_empresas(conn, local_results):
    """
    Insere ou atualiza os resultados no banco de dados.
    """
    sql = ''' INSERT OR REPLACE INTO empresas(title, address, description, latitude, longitude, hours, phone, website, directions, rating, reviews, type, place_id, provider_id, position)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    
    for item in local_results:
        # Extrair campos aninhados com segurança
        gps = item.get('gps_coordinates', {})
        lat = gps.get('latitude')
        lng = gps.get('longitude')
        
        links = item.get('links', {})
        website = links.get('website')
        directions = links.get('directions')
        
        # Preparar dados
        empresa = (
            item.get('title'),
            item.get('address'),
            item.get('description'),
            lat,
            lng,
            item.get('hours'),
            item.get('phone'),
            website,
            directions,
            item.get('rating'),
            item.get('reviews'),
            item.get('type'),
            item.get('place_id'),
            item.get('provider_id'),
            item.get('position')
        )
        
        try:
            cur.execute(sql, empresa)
        except sqlite3.Error as e:
            print(f"Erro ao inserir empresa {item.get('title')}: {e}")
            
    conn.commit()
    return cur.lastrowid

if __name__ == "__main__":
    # Teste básico se executado diretamente
    pass
