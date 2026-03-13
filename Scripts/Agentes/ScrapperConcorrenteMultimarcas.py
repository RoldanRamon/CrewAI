import os
import pandas as pd
from io import StringIO
from crewai import Agent, Task, Crew, Process, LLM
from crewai_tools import ScrapeWebsiteTool
from dotenv import load_dotenv

load_dotenv()

deepseek_key = os.environ.get("DEEPSEEK_API_KEY")

myllm = LLM(
    model='deepseek/deepseek-chat',
    api_key=deepseek_key,
    base_url="https://api.deepseek.com/v1"
)

# Lista de URLs e nomes das empresas
companies = [
    {"url": "https://nmmultimarcas.com.br/multipla", "name": "NM Multimarcas"},
    # Adicione mais empresas aqui:
    # {"url": "https://outraempresa.com.br", "name": "Outra Empresa"},
]

all_results = []

for company in companies:
    tool = ScrapeWebsiteTool(website_url=company["url"])
    text = tool.run()

    agent = Agent(
        role="Data Extractor",
        goal="Extract vehicle data from scraped text into structured CSV format",
        backstory="You are an expert at parsing unstructured text into structured data. "
                  "You classify vehicles as: Moto, Carro, or Caminhão based on the model.",
        llm=myllm
    )

    task = Task(
        description=(
            f"Extract ALL vehicles from the text below.\n"
            f"Return ONLY a CSV-formatted text (comma-separated) with these columns:\n"
            f"Company,Brand,Model,Type,Price,Year,Km,Fuel\n\n"
            f"Rules:\n"
            f"- Company: always '{company['name']}'\n"
            f"- Type: classify as 'Moto', 'Carro', or 'Caminhão' based on the vehicle model\n"
            f"- Do NOT include headers, only data rows\n"
            f"- Do NOT include any explanation, only CSV lines\n\n"
            f"Text:\n{text}"
        ),
        expected_output="CSV lines with columns: Company,Brand,Model,Type,Price,Year,Km,Fuel",
        agent=agent
    )

    crew = Crew(agents=[agent], tasks=[task])
    result = crew.kickoff()
    all_results.append(str(result))

# Parse results into DataFrame
from io import StringIO

csv_text = "\n".join(all_results)
df = pd.read_csv(
    StringIO(csv_text),
    header=None,
    names=["Company", "Brand", "Model", "Type", "Price", "Year", "Km", "Fuel"]
)

df.to_csv("vehicles.csv", index=False, encoding="utf-8-sig")
print(df)
print("\nSaved to vehicles.csv")