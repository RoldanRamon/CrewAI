"""
Agente de Consulta ao Banco de Dados - Linguagem Natural → SQL
Banco: Database/empresas.db | Tabela: empresas

Configuração via .env:
  - Para OpenAI:    MODEL=gpt-4o-mini   OPENAI_API_KEY=sk-...
  - Para DeepSeek:  MODEL=deepseek/deepseek-chat   DEEPSEEK_API_KEY=sk-...
  - Para Groq:      MODEL=groq/llama-3.1-8b-instant   GROQ_API_KEY=gsk_...
"""

import os
import sqlite3
from pathlib import Path
from dotenv import load_dotenv
from crewai import Agent, Task, Crew, LLM
from crewai.tools import tool

# ─── Configuração ──────────────────────────────────────────────────────────────

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "Database" / "empresas.db"

# Detecta automaticamente qual provider usar com base nas chaves disponíveis
def configurar_llm() -> LLM:
    openai_key   = os.getenv("OPENAI_API_KEY")
    deepseek_key = os.getenv("DEEPSEEK_API_KEY")
    groq_key     = os.getenv("GROQ_API_KEY")
    model_env    = os.getenv("MODEL")

    if model_env:
        # Usuário definiu MODEL manualmente → usa ele
        print(f"🔧 Usando modelo configurado: {model_env}")
        return LLM(model=model_env, temperature=0.0)

    if openai_key:
        print("🔧 Usando OpenAI (gpt-4o-mini)")
        return LLM(model="gpt-4o-mini", api_key=openai_key, temperature=0.0)

    if groq_key:
        print("🔧 Usando Groq (llama-3.1-8b-instant) — gratuito!")
        return LLM(
            model="groq/llama-3.1-8b-instant",
            api_key=groq_key,
            temperature=0.0,
        )

    if deepseek_key:
        print("🔧 Usando DeepSeek (deepseek-chat)")
        return LLM(
            model="deepseek/deepseek-chat",
            api_key=deepseek_key,
            base_url="https://api.deepseek.com",
            temperature=0.0,
        )

    raise EnvironmentError(
        "\n❌ Nenhuma chave de API encontrada no .env!\n"
        "Adicione uma das seguintes ao arquivo .env:\n"
        "  OPENAI_API_KEY=sk-...           (OpenAI)\n"
        "  GROQ_API_KEY=gsk_...            (Groq — gratuito em groq.com)\n"
        "  DEEPSEEK_API_KEY=sk-...         (DeepSeek)\n"
    )


# ─── Schema do banco ───────────────────────────────────────────────────────────

SCHEMA = """
Tabela: empresas
Colunas:
  id          INTEGER  - identificador único (auto)
  title       TEXT     - nome da empresa/clínica
  address     TEXT     - endereço completo
  description TEXT     - avaliação/comentário do cliente
  latitude    REAL     - latitude geográfica
  longitude   REAL     - longitude geográfica
  hours       TEXT     - horário de funcionamento
  phone       TEXT     - telefone de contato
  website     TEXT     - site da empresa
  directions  TEXT     - link para rota no Google Maps
  rating      REAL     - nota média (0 a 5)
  reviews     INTEGER  - número de avaliações
  type        TEXT     - categoria (ex: Dentista, Clínica odontológica, Ortodontista)
  place_id    TEXT     - ID único do Google Maps
  provider_id TEXT     - ID do provedor
  position    INTEGER  - posição no ranking de busca

Total de registros: 277
Tipos disponíveis: Dentista, Clínica odontológica, Laboratório de odontologia,
  Radiologia odontológica, Cirurgião dentista, Ortodontista, Cirurgião oral e
  maxilofacial, Policlínica, Clínica especializada, Médico, Hospital, e outros.
"""

# ─── Ferramentas ───────────────────────────────────────────────────────────────

@tool("ExecutarSQL")
def executar_sql(query: str) -> str:
    """
    Executa uma query SQL SELECT no banco de dados de empresas e retorna os resultados.
    Use APENAS queries SELECT. Não execute UPDATE, DELETE ou DROP.
    Parâmetro: query (string SQL válida para SQLite).
    """
    query = query.strip()
    if not query.upper().startswith("SELECT"):
        return "❌ Erro: apenas consultas SELECT são permitidas."

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            return "Nenhum resultado encontrado para esta consulta."

        cols = rows[0].keys()
        resultado = [dict(row) for row in rows]

        linhas = []
        for i, r in enumerate(resultado, 1):
            partes = [f"{c}: {r[c]}" for c in cols if r[c] is not None]
            linhas.append(f"[{i}] " + " | ".join(partes))

        total = len(resultado)
        return f"✅ {total} resultado(s) encontrado(s):\n\n" + "\n".join(linhas)

    except Exception as e:
        return f"❌ Erro ao executar SQL: {str(e)}"


@tool("InspecionarSchema")
def inspecionar_schema(placeholder: str = "") -> str:
    """
    Retorna o schema completo da tabela 'empresas' com descrição de cada coluna.
    Chame esta ferramenta ANTES de montar qualquer query SQL.
    """
    return SCHEMA


# ─── Agente e Consulta ─────────────────────────────────────────────────────────

def criar_crew(llm: LLM) -> tuple:
    analista = Agent(
        role="Analista de Banco de Dados",
        goal=(
            "Traduzir perguntas em linguagem natural para SQL preciso e "
            "retornar respostas claras e amigáveis para usuários leigos."
        ),
        backstory=(
            "Você é um assistente especialista em banco de dados de empresas e "
            "clínicas odontológicas. Seu papel é ajudar usuários sem conhecimento "
            "técnico a obter informações do banco. Você sempre verifica o schema "
            "antes de montar queries, usa LIMIT quando há risco de retornar muitos "
            "dados, e explica os resultados de forma simples, direta e em português."
        ),
        tools=[inspecionar_schema, executar_sql],
        llm=llm,
        verbose=False,
        max_iter=10,
    )
    return analista


def consultar(pergunta: str, analista: Agent) -> str:
    task = Task(
        description=(
            f"O usuário perguntou: \"{pergunta}\"\n\n"
            "Siga estes passos:\n"
            "1. Use InspecionarSchema para verificar as colunas disponíveis.\n"
            "2. Monte uma query SQL SELECT adequada para responder a pergunta.\n"
            "   - Use LOWER() para comparações de texto (ex: LOWER(type) LIKE '%dentista%')\n"
            "   - Use LIMIT quando a pergunta não pedir todos os registros.\n"
            "3. Use ExecutarSQL para executar a query.\n"
            "4. Responda em português claro e amigável.\n"
            "   - Se forem muitas linhas, destaque os mais relevantes e informe o total.\n"
            "   - Use emojis moderados (🏥, 📍, ⭐, 📞).\n"
            "   - NUNCA mostre SQL na resposta final."
        ),
        expected_output=(
            "Resposta em português claro, amigável e sem código SQL, "
            "explicando os dados encontrados no banco de dados."
        ),
        agent=analista,
    )

    crew = Crew(agents=[analista], tasks=[task], verbose=False)
    return str(crew.kickoff())


# ─── Interface de Chat ─────────────────────────────────────────────────────────

BANNER = """
╔══════════════════════════════════════════════════════════════╗
║       🏥  Assistente de Consulta - Banco de Empresas        ║
║                                                              ║
║  Faça perguntas em português sobre as empresas cadastradas!  ║
║  Digite  'sair'  ou  'exit'  para encerrar.                  ║
╚══════════════════════════════════════════════════════════════╝
"""

EXEMPLOS = """
💡 Exemplos de perguntas:
   • Quantas empresas estão cadastradas no total?
   • Quais clínicas têm nota acima de 4.8?
   • Me mostre as 5 empresas com mais avaliações.
   • Tem algum ortodontista cadastrado? Qual o telefone?
   • Qual clínica fica na rua dos Eucalíptos?
   • Qual a média de avaliações das clínicas odontológicas?
   • Qual a melhor clínica cadastrada?
"""


def main():
    print(BANNER)

    try:
        llm = configurar_llm()
    except EnvironmentError as e:
        print(e)
        return

    analista = criar_crew(llm)
    print(EXEMPLOS)
    print("─" * 64)

    while True:
        try:
            pergunta = input("\n🧑 Você: ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\n\n👋 Encerrando. Até logo!")
            break

        if not pergunta:
            continue

        if pergunta.lower() in {"sair", "exit", "quit", "q"}:
            print("👋 Encerrando. Até logo!")
            break

        print("\n⏳ Consultando o banco de dados...\n")
        try:
            resposta = consultar(pergunta, analista)
            print(f"🤖 Assistente:\n{resposta}")
        except Exception as e:
            err = str(e)
            if "Insufficient Balance" in err or "insufficient_quota" in err:
                print("❌ Sua chave de API está sem créditos.")
                print("   → Adicione uma chave válida ao arquivo .env e tente novamente.")
            else:
                print(f"❌ Erro: {err}")

        print("\n" + "─" * 64)


if __name__ == "__main__":
    main()
