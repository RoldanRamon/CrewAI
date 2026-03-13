from serpapi import GoogleSearch
import os
from dotenv import load_dotenv
import pprint

load_dotenv()
serper_api_key = os.getenv("SERPAPI_KEY")

lotes = [20, 40, 60, 80, 100]
tema = 'Odontologia'
local = 'Colombo, Parana, Brazil'

params = {
  "engine": "google_local",
  "q": tema,
  "location": local,
  "google_domain": "google.com",
  "hl": "pt-br",
  "gl": "br",
  "api_key": serper_api_key
}

search = GoogleSearch(params)
results = search.get_dict()
local_results = results["local_results"]
pprint.pprint(local_results)

