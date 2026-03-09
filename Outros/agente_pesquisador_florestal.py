from crewai import Agent, Task, Crew, Process, LLM
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

load_dotenv()
deepseek_api_key = os.getenv('DEEPSEEK_API_KEY')
serper_api_key = os.getenv('SERPER_API_KEY')

# Configurar o LLM do DeepSeek
llm = LLM(
    model="deepseek/deepseek-chat",
    api_key=deepseek_api_key,
    base_url="https://api.deepseek.com/v1",
    temperature=0.7,
)

# Criar agente especializado em pesquisa de notícias do setor florestal
pesquisador_noticias = Agent(
    role="Pesquisador de Notícias do Setor Florestal",
    goal="Pesquisar e analisar notícias recentes sobre o setor florestal, com foco em empresas como Komatsu Forest e TimberPro",
    backstory="""Você é um analista especializado em notícias do setor florestal e de máquinas pesadas.
    Tem experiência em identificar tendências, inovações tecnológicas, lançamentos de produtos,
    parcerias estratégicas e movimentos de mercado no segmento florestal.
    Sua pesquisa sempre inclui fontes confiáveis e referências verificáveis.""",
    llm=llm,
    verbose=True,
)

# Criar tarefa de pesquisa focada em notícias do ramo florestal
tarefa_pesquisa_noticias = Task(
    description="""Pesquise notícias RECENTES do ramo florestal, considerando que estamos em FEVEREIRO DE 2026.
    Foque em notícias dos anos 2025 e 2026, com ênfase especial nas empresas Komatsu Forest e TimberPro.
    
    CONTEXTO TEMPORAL IMPORTANTE:
    - Data atual: Fevereiro de 2026
    - Período de interesse: 2025-2026 (últimos 12-14 meses)
    - Ignore notícias anteriores a 2025
    
    TÓPICOS A SEREM PESQUISADOS:
    1. Novos lançamentos de produtos ou tecnologias (2025-2026)
    2. Expansões de mercado e presença global (2025-2026)
    3. Parcerias estratégicas e joint ventures recentes
    4. Inovações em sustentabilidade e eficiência
    5. Tendências atuais do mercado florestal
    6. Desafios e oportunidades do setor em 2025-2026
    
    REQUISITOS DE PESQUISA:
    - Foque em fontes confiáveis (sites oficiais, portais especializados, revistas do setor)
    - Inclua datas das notícias quando disponíveis
    - PRIORIZE NOTÍCIAS DE 2025 E 2026
    - Evite informações anteriores a 2025
    - Para cada informação, inclua a fonte de referência com data
    - Se não encontrar notícias recentes, busque projeções e tendências para 2026""",
    
    expected_output="""Um relatório detalhado contendo:
    
    CABEÇALHO TEMPORAL:
    - Data da pesquisa: Fevereiro de 2026
    - Período coberto: 2025-2026
    - Data atual de referência: 2026
    
    1. RESUMO EXECUTIVO: Visão geral das principais notícias do setor florestal (2025-2026)
    
    2. NOTÍCIAS POR EMPRESA (2025-2026):
       - Komatsu Forest: Principais desenvolvimentos, lançamentos, expansões RECENTES
       - TimberPro: Inovações, parcerias, presença de mercado ATUAIS
    
    3. TENDÊNCIAS ATUAIS DO SETOR (2025-2026):
       - Inovações tecnológicas em máquinas florestais
       - Sustentabilidade e práticas ambientais recentes
       - Mercados em crescimento/declínio atual
    
    4. ANÁLISE COMPARATIVA 2025-2026:
       - Posicionamento competitivo das empresas no período atual
       - Diferenciais tecnológicos recentes
       - Estratégias de mercado atuais
    
    5. REFERÊNCIAS COMPLETAS (COM DATAS):
       - Lista organizada de todas as fontes utilizadas
       - URLs completos das notícias
       - DATAS DE PUBLICAÇÃO (priorizar 2025-2026)
       - Títulos das notícias e veículos de comunicação
    
    6. PROJEÇÕES E RECOMENDAÇÕES PARA 2026:
       - Insights sobre oportunidades no setor
       - Áreas para monitoramento futuro
       - Tendências esperadas para o restante de 2026""",
    
    agent=pesquisador_noticias,
)

# Criar e executar a crew
crew = Crew(
    agents=[pesquisador_noticias],
    tasks=[tarefa_pesquisa_noticias],
   # process=Process.sequential,
    verbose=True,
)

print("Iniciando pesquisa de notícias do setor florestal...")
print("Foco: Komatsu Forest e TimberPro")
print("=" * 60)

resultado = crew.kickoff()
print("\n" + "=" * 60)
print("RESULTADO DA PESQUISA:")
print("=" * 60)
print(resultado)

# Salvar resultados em CSV com estrutura organizada
data_atual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

df = pd.DataFrame({
    'data_pesquisa': [data_atual],
    'tema': ['Notícias do Setor Florestal - Komatsu Forest e TimberPro'],
    'relatorio_completo': [str(resultado)],
    'empresas_foco': ['Komatsu Forest, TimberPro'],
    'periodo_pesquisa': ['2025-2026 (Fevereiro 2026)']
})

# Salvar para CSV com encoding UTF-8 para caracteres especiais
nome_arquivo = f'noticias_florestal_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
df.to_csv(nome_arquivo, index=False, encoding='utf-8')

print("\n" + "=" * 60)
print(f"Resultados salvos em: {nome_arquivo}")
print("=" * 60)

# Criar também um arquivo de texto legível para fácil visualização
with open(f'relatorio_noticias_florestal_{datetime.now().strftime("%Y%m%d")}.txt', 'w', encoding='utf-8') as f:
    f.write(f"RELATÓRIO DE NOTÍCIAS DO SETOR FLORESTAL\n")
    f.write(f"Data da pesquisa: {data_atual}\n")
    f.write(f"Empresas em foco: Komatsu Forest e TimberPro\n")
    f.write("=" * 80 + "\n\n")
    f.write(str(resultado))

print(f"Relatório em texto salvo em: relatorio_noticias_florestal_{datetime.now().strftime('%Y%m%d')}.txt")
print("Pesquisa concluída com sucesso!")