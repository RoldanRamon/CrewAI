from serpapi import GoogleSearch
import os
from dotenv import load_dotenv
import pprint
import time
try:
    from Scripts.CriarDataBase import create_connection, create_table, save_empresas
except ImportError:
    from CriarDataBase import create_connection, create_table, save_empresas

load_dotenv()
serper_api_key = os.getenv("SERPAPI_KEY")

# Configurações de paginação
config_lotes = {"lote": 20, "maximo": 10000}
tema = 'Odontologia'
local = 'Colombo, Parana, Brazil'

def buscar_e_salvar():
    conn = create_connection()
    if conn is None:
        print("Erro! Não foi possível estabelecer conexão com o banco de dados.")
        return

    create_table(conn)
    total_salvo = 0

    for start in range(0, config_lotes["maximo"], config_lotes["lote"]):
        print(f"Buscando lote iniciando em: {start}...")
    

        params = {
            "engine": "google_local",
            "q": tema,
            "location": local,
            "google_domain": "google.com",
            "hl": "pt-br",
            "gl": "br",
            "start": start,
            "api_key": serper_api_key
        }

        try:
            search = GoogleSearch(params)
            results = search.get_dict()
            local_results = results.get("local_results", [])

            if not local_results:
                print(f"Não foram encontrados mais resultados a partir de {start}. Encerrando.")
                break

            save_empresas(conn, local_results)
            total_salvo += len(local_results)
            print(f"Salvos {len(local_results)} resultados deste lote.")
            time.sleep(1)

        except Exception as e:
            print(f"Erro ao buscar lote {start}: {e}")
            break

    conn.close()
    print(f"\nBusca finalizada! Total de empresas salvas no banco: {total_salvo}")

if __name__ == "__main__":
    buscar_e_salvar()


