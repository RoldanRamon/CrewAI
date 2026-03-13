from crewai import Agent, Task, Crew, Process, LLM
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import json
import re

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

# Quantidade de empresas desejada
QTD_EMPRESAS = 400
CIDADE = "Colombo"

# Bairros de Colombo organizados em grupos para pesquisa em lotes
BAIRROS_RURAIS = [
    "Águas Fervidas", "Bacaetava", "Boicininga", "Butiatumirim", "Campestre",
    "Capivari", "Colônia Antonio Prado", "Colônia Faria", "Gabirobal", "Imbuial",
    "Itajacuru", "Morro Grande", "Poço Negro", "Ribeirão das Onças", "Roseira",
    "Santa Gema", "São João", "Sapopema", "Serrinha", "Uvaranal",
]

BAIRROS_URBANOS = [
    "Arruda", "Atuba", "Campo Pequeno", "Canguiri", "Centro", "Das Graças",
    "Embú", "Fátima", "Guaraituba", "Guarani", "Maracanã", "Mauá", "Monza",
    "Osasco", "Palmital", "Paloma", "Rincão", "Rio Verde", "Roça Grande",
    "Santa Terezinha", "São Dimas", "São Gabriel",
]

TODOS_BAIRROS = BAIRROS_URBANOS + BAIRROS_RURAIS

# Dividir bairros em 4 grupos para 4 lotes de pesquisa
def dividir_lista(lista, n):
    """Divide uma lista em n partes aproximadamente iguais."""
    k, m = divmod(len(lista), n)
    return [lista[i*k+min(i,m):(i+1)*k+min(i+1,m)] for i in range(n)]

GRUPOS_BAIRROS = dividir_lista(TODOS_BAIRROS, 4)

# === FUNÇÕES AUXILIARES PARA PARSING ===

def limpar_markdown(texto):
    """Remove blocos de código markdown (```json ... ```) do texto."""
    texto = re.sub(r'```json\s*', '', texto)
    texto = re.sub(r'```\s*', '', texto)
    return texto.strip()


def corrigir_json_truncado(texto):
    """Tenta corrigir JSON truncado fechando arrays/objetos abertos."""
    # Conta chaves e colchetes abertos/fechados
    abertos_chave = texto.count('{') - texto.count('}')
    abertos_colchete = texto.count('[') - texto.count(']')

    # Remove último objeto incompleto se necessário
    # (procura a última vírgula antes de um objeto incompleto)
    if abertos_chave > 0:
        # Tenta remover o último objeto incompleto
        ultimo_obj_completo = texto.rfind('},')
        ultimo_obj_fechado = texto.rfind('}')
        if ultimo_obj_completo > 0 and ultimo_obj_completo < ultimo_obj_fechado:
            # Tem um objeto completo com vírgula, corta depois dele
            pass
        elif ultimo_obj_completo > 0:
            texto = texto[:ultimo_obj_completo + 1]
            abertos_chave = texto.count('{') - texto.count('}')
            abertos_colchete = texto.count('[') - texto.count(']')

    # Fecha colchetes e chaves restantes
    texto += ']' * abertos_colchete
    texto += '}' * abertos_chave
    return texto


def extrair_empresas_do_resultado(resultado_str):
    """Extrai lista de empresas do resultado, tratando vários formatos."""
    # 1. Limpar markdown
    texto = limpar_markdown(resultado_str)

    # 2. Tentar extrair JSON diretamente
    json_match = re.search(r'\{.*\}', texto, re.DOTALL)
    if json_match:
        json_texto = json_match.group()
        # Tentar parse direto
        try:
            dados = json.loads(json_texto)
            return extrair_lista_empresas(dados)
        except json.JSONDecodeError:
            pass

        # Tentar corrigir JSON truncado
        try:
            json_corrigido = corrigir_json_truncado(json_texto)
            dados = json.loads(json_corrigido)
            return extrair_lista_empresas(dados)
        except json.JSONDecodeError:
            pass

    # 3. Tentar extrair array JSON diretamente
    array_match = re.search(r'\[.*\]', texto, re.DOTALL)
    if array_match:
        try:
            empresas = json.loads(array_match.group())
            if isinstance(empresas, list):
                return empresas
        except json.JSONDecodeError:
            pass

    # 4. Tentar extrair objetos individuais por regex
    empresas = []
    pattern = r'\{[^{}]*"nome"\s*:\s*"[^"]*"[^{}]*\}'
    matches = re.findall(pattern, texto)
    for m in matches:
        try:
            obj = json.loads(m)
            empresas.append(obj)
        except json.JSONDecodeError:
            continue
    return empresas


def extrair_lista_empresas(dados):
    """Extrai a lista de empresas de um dicionário JSON."""
    if isinstance(dados, list):
        return dados
    for chave in ['empresas_validadas', 'empresas', 'lista', 'resultados']:
        if chave in dados:
            val = dados[chave]
            if isinstance(val, list):
                return val
    return []


# Criar agente especializado em pesquisa de empresas multimarcas
pesquisador_empresas = Agent(
    role="Pesquisador de Empresas de Revenda de Veículos (Multimarcas)",
    goal=f"Pesquisar e listar o MÁXIMO possível de empresas multimarcas na cidade de {CIDADE}-PR em TODOS os bairros, com meta de {QTD_EMPRESAS} empresas",
    backstory=f"""Você é um especialista em pesquisa de empresas e negócios locais.
    Tem experiência em identificar empresas que revendem veiculos como motos, carros e caminhões de diversas marcas (multimarcas).
    Sabe como encontrar informações de contato, sites, redes sociais e dados dos proprietários.
    Sua pesquisa é focada em empresas na cidade de {CIDADE}, Paraná, cobrindo TODOS os bairros da cidade.
    IMPORTANTE: Você DEVE listar TODAS as empresas que encontrar, mesmo que não consiga todos os dados.
    É melhor ter uma empresa com dados parciais do que não tê-la na lista.
    Os bairros de Colombo incluem: {', '.join(TODOS_BAIRROS)}""",
    llm=llm,
    verbose=True,
)

# Criar tarefas de pesquisa por grupo de bairros (4 lotes)
tarefas = []
for idx, grupo in enumerate(GRUPOS_BAIRROS):
    bairros_str = ', '.join(grupo)
    tarefa = Task(
        description=f"""Pesquise e liste 100 empresas multimarcas na cidade de {CIDADE}-PR, 
        focando nos seguintes bairros: {bairros_str}.
        
        FOCO PRINCIPAL:
        - Empresas que trabalham com revenda de veículos (multimarcas) em {CIDADE}-PR
        - Pesquise ESPECIFICAMENTE em cada um destes bairros: {bairros_str}
        - Inclua TODAS as empresas encontradas, mesmo com dados incompletos
        - Empresas de todos os tamanhos (pequenas, médias, grandes)
        
        {'IMPORTANTE: Liste empresas DIFERENTES das já mencionadas nos lotes anteriores.' if idx > 0 else ''}
        
        INFORMAÇÕES A SEREM COLETADAS (preencha o que conseguir):
        1. Nome da empresa (OBRIGATÓRIO - sem nome, não inclua)
        2. Bairro (IMPORTANTE - informe o bairro onde a empresa está localizada)
        3. Telefone de contato
        4. Endereço completo (incluindo bairro)
        5. Site/Link da empresa
        6. E-mail
        7. Contato do proprietário/dono (quando disponível)
        8. Descrição breve
        
        REGRA FUNDAMENTAL:
        - INCLUA a empresa MESMO que só tenha o nome e bairro
        - Campos sem informação devem ser preenchidos com "" (vazio)
        - NÃO descarte empresas por falta de dados
        - NÃO invente dados - se não sabe, deixe vazio
        - Pesquise em CADA bairro individualmente: {bairros_str}
        
        MÉTODOS DE PESQUISA:
        - Google Maps / Google Business (pesquise "multimarcas {CIDADE} [nome do bairro]")
        - Diretórios de negócios (Guia Mais, TeleListas, Apontador)
        - Redes sociais (Facebook, Instagram)
        - OLX, WebMotors, iCarros (anunciantes de {CIDADE})
        - Associações comerciais locais
        
        SAÍDA: Retorne APENAS o JSON, sem blocos de código markdown.
        NÃO use ```json ou ```. Retorne o JSON puro.""",
        
        expected_output=f"""JSON puro (sem markdown) com lista de empresas de {CIDADE} - Lote {idx+1}:
{{
  "empresas": [
    {{
      "nome": "Nome da Empresa",
      "cidade": "{CIDADE}",
      "bairro": "Nome do Bairro",
      "telefone": "(41) XXXX-XXXX",
      "endereco": "Rua X, 123 - Bairro, {CIDADE}-PR",
      "site": "https://...",
      "email": "contato@...",
      "contato_proprietario": "Nome",
      "descricao": "Descrição"
    }}
  ]
}}

MÍNIMO 100 empresas neste lote. Inclua TODAS mesmo com dados parciais.
Bairros deste lote: {bairros_str}""",
        
        agent=pesquisador_empresas,
    )
    tarefas.append(tarefa)

# Criar e executar a crew
crew = Crew(
    agents=[pesquisador_empresas],
    tasks=tarefas,
    process=Process.sequential,
    verbose=True,
)

print("=" * 80)
print(f"INICIANDO PESQUISA DE EMPRESAS MULTIMARCAS")
print(f"Cidade: {CIDADE}-PR | Meta: {QTD_EMPRESAS} empresas")
print("=" * 80)

resultado = crew.kickoff()

print("\n" + "=" * 80)
print("RESULTADO DA PESQUISA:")
print("=" * 80)

# Processar resultados de todas as tarefas
todas_empresas = []
nomes_vistos = set()

# Processar output de cada tarefa
for i, task_output in enumerate(crew.tasks):
    print(f"\nProcessando lote {i+1}...")
    try:
        output_str = str(task_output.output)
        empresas_lote = extrair_empresas_do_resultado(output_str)
        print(f"  Lote {i+1}: {len(empresas_lote)} empresas extraídas")
        
        for emp in empresas_lote:
            nome = emp.get('nome', '').strip()
            if nome and nome.lower() not in nomes_vistos:
                nomes_vistos.add(nome.lower())
                # Garantir que todos os campos existam
                empresa_padrao = {
                    'nome': nome,
                    'cidade': CIDADE,
                    'bairro': emp.get('bairro', ''),
                    'telefone': emp.get('telefone', ''),
                    'endereco': emp.get('endereco', ''),
                    'site': emp.get('site', ''),
                    'email': emp.get('email', ''),
                    'contato_proprietario': emp.get('contato_proprietario', ''),
                    'descricao': emp.get('descricao', ''),
                    'setor': emp.get('setor', 'Automotivo - Revenda de Veículos Multimarcas'),
                    'marcas': emp.get('marcas', []),
                }
                todas_empresas.append(empresa_padrao)
    except Exception as e:
        print(f"  Erro no lote {i+1}: {e}")

# Também tentar o resultado geral caso as tarefas individuais não tenham dado certo
if not todas_empresas:
    print("\nTentando extrair do resultado geral...")
    resultado_str = str(resultado)
    todas_empresas = extrair_empresas_do_resultado(resultado_str)
    # Padronizar campos
    empresas_padronizadas = []
    for emp in todas_empresas:
        nome = emp.get('nome', '').strip()
        if nome:
            empresa_padrao = {
                'nome': nome,
                'cidade': CIDADE,
                'bairro': emp.get('bairro', ''),
                'telefone': emp.get('telefone', ''),
                'endereco': emp.get('endereco', ''),
                'site': emp.get('site', ''),
                'email': emp.get('email', ''),
                'contato_proprietario': emp.get('contato_proprietario', ''),
                'descricao': emp.get('descricao', ''),
                'setor': emp.get('setor', 'Automotivo - Revenda de Veículos Multimarcas'),
                'marcas': emp.get('marcas', []),
            }
            empresas_padronizadas.append(empresa_padrao)
    todas_empresas = empresas_padronizadas

print(f"\nTotal de empresas únicas extraídas: {len(todas_empresas)}")

# Criar DataFrame
if todas_empresas:
    df = pd.DataFrame(todas_empresas)
    
    # Converter lista de marcas para string no CSV
    if 'marcas' in df.columns:
        df['marcas'] = df['marcas'].apply(
            lambda x: ', '.join(x) if isinstance(x, list) else str(x) if x else ''
        )
    
    # Salvar para CSV
    data_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_arquivo_csv = f'empresas_multimarcas_{CIDADE.lower()}_{data_hora}.csv'
    df.to_csv(nome_arquivo_csv, index=False, encoding='utf-8')
    
    # Salvar JSON
    nome_arquivo_json = f'empresas_multimarcas_{CIDADE.lower()}_{data_hora}.json'
    with open(nome_arquivo_json, 'w', encoding='utf-8') as f:
        json.dump({'empresas': todas_empresas, 'total': len(todas_empresas)}, f, ensure_ascii=False, indent=2)
    
    # Estatísticas
    total = len(df)
    com_telefone = len(df[df['telefone'].notna() & (df['telefone'] != '')])
    com_site = len(df[df['site'].notna() & (df['site'] != '')])
    com_email = len(df[df['email'].notna() & (df['email'] != '')])
    com_contato = len(df[df['contato_proprietario'].notna() & (df['contato_proprietario'] != '')])
    com_endereco = len(df[df['endereco'].notna() & (df['endereco'] != '')])
    com_bairro = len(df[df['bairro'].notna() & (df['bairro'] != '')])
    
    print(f"\n{'='*80}")
    print(f"RESUMO ESTATÍSTICO - {CIDADE}")
    print(f"{'='*80}")
    print(f"Total de empresas encontradas: {total}")
    print(f"Empresas com bairro: {com_bairro}")
    print(f"Empresas com telefone: {com_telefone}")
    print(f"Empresas com site: {com_site}")
    print(f"Empresas com e-mail: {com_email}")
    print(f"Empresas com contato do proprietário: {com_contato}")
    print(f"Empresas com endereço: {com_endereco}")
    
    # Distribuição por bairro
    if com_bairro > 0:
        print(f"\nDISTRIBUIÇÃO POR BAIRRO:")
        bairro_counts = df[df['bairro'] != '']['bairro'].value_counts()
        for bairro, count in bairro_counts.items():
            print(f"  {bairro}: {count} empresas")
    
    # Criar relatório em texto
    nome_relatorio = f'relatorio_empresas_{CIDADE.lower()}_{data_hora}.txt'
    with open(nome_relatorio, 'w', encoding='utf-8') as f:
        f.write(f"RELATÓRIO DE EMPRESAS MULTIMARCAS - {CIDADE.upper()}-PR\n")
        f.write(f"Data da pesquisa: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Total de empresas: {total}\n")
        f.write(f"Com telefone: {com_telefone}\n")
        f.write(f"Com site: {com_site}\n")
        f.write(f"Com e-mail: {com_email}\n")
        f.write(f"Com contato do proprietário: {com_contato}\n")
        f.write(f"Com endereço: {com_endereco}\n")
        f.write(f"Com bairro: {com_bairro}\n\n")
        
        f.write("LISTA DE EMPRESAS:\n")
        f.write("=" * 80 + "\n")
        
        for i in range(len(df)):
            empresa = df.iloc[i]
            f.write(f"\n{i+1}. {empresa.get('nome', 'N/A')}\n")
            f.write(f"   Cidade: {empresa.get('cidade', CIDADE)}\n")
            f.write(f"   Bairro: {empresa.get('bairro', '') or 'N/A'}\n")
            f.write(f"   Telefone: {empresa.get('telefone', '') or 'N/A'}\n")
            f.write(f"   Endereço: {empresa.get('endereco', '') or 'N/A'}\n")
            f.write(f"   Site: {empresa.get('site', '') or 'N/A'}\n")
            f.write(f"   E-mail: {empresa.get('email', '') or 'N/A'}\n")
            f.write(f"   Contato: {empresa.get('contato_proprietario', '') or 'N/A'}\n")
            marcas = empresa.get('marcas', '')
            f.write(f"   Marcas: {marcas or 'N/A'}\n")
            f.write(f"   Descrição: {empresa.get('descricao', '') or 'N/A'}\n")
    
    print(f"\nARQUIVOS GERADOS:")
    print(f"  1. CSV: {nome_arquivo_csv}")
    print(f"  2. JSON: {nome_arquivo_json}")
    print(f"  3. Relatório: {nome_relatorio}")
else:
    print("\nNenhuma empresa extraída dos resultados.")
    print("Salvando resultado bruto para análise...")
    
    data_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
    nome_arquivo_bruto = f'resultado_bruto_{CIDADE.lower()}_{data_hora}.txt'
    with open(nome_arquivo_bruto, 'w', encoding='utf-8') as f:
        f.write(str(resultado))
        f.write("\n\n--- OUTPUTS DAS TAREFAS ---\n")
        for i, task in enumerate(crew.tasks):
            f.write(f"\n--- TAREFA {i+1} ---\n")
            f.write(str(task.output))
    
    print(f"Resultado bruto salvo em: {nome_arquivo_bruto}")

print("\n" + "=" * 80)
print("PESQUISA CONCLUÍDA!")
print("=" * 80)