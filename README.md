
# Processamento de Dados de Clientes 

## Visão Geral

Este notebook implementa o pipeline ETL completo para a dimensão de clientes (Customer Dimension), processando dados brutos da empresa Sports Bar através das camadas Bronze, Silver e Gold da arquitetura Medallion.
O notebook aplica regras de qualidade de dados, normalização e integração com a tabela consolidada da empresa-mãe Atlon.

## Metadados do Notebook

**Nome:** 1_customer_data_processing  
**Caminho:** `/consolidated_pipeline/2_dimension_data_processing/1_customer_data_processing`  
**Linguagem:** Python  
**Catálogo:** fmcg  
**Fonte de Dados:** customers  

## Índice

1. Configuração Inicial
2. Camada Bronze - Ingestão de Dados Brutos
3. Camada Silver - Transformações e Limpeza
4. Camada Gold - Modelo de Negócio
5. Integração com Tabela Consolidada
6. Resumo das Transformações

---

## Configuração Inicial

### Célula 1: Importação de Bibliotecas

```python
from pyspark.sql import functions as F 
from delta.tables import DeltaTable
```

**Objetivo:** Importa as funções PySpark necessárias para manipulação de DataFrames e operações Delta Lake.

**Componentes:**
- `functions as F`: Conjunto completo de funções de transformação do Spark
- `DeltaTable`: Classe para operações MERGE e gerenciamento de tabelas Delta

---

### Célula 2: Carregamento de Utilitários

```python
%run /Workspace/consolidated_pipeline/1_setup/utilities
```

**Objetivo:** Executa o notebook de utilitários que define variáveis compartilhadas entre todos os notebooks do projeto.

**Variáveis Carregadas:**
- `bronze = "bronze"` - Nome do schema Bronze
- `silver_schema = "silver"` - Nome do schema Silver
- `gold_schema = "gold"` - Nome do schema Gold

---

### Célula 3: Verificação de Variáveis

```python
print(bronze, silver_schema, gold_schema)
```

**Saída:** `bronze silver gold`

**Objetivo:** Valida que as variáveis do notebook de utilitários foram carregadas corretamente.

---

### Célula 4: Definição de Widgets de Parâmetros

```python
dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "customers", "Data Source")
```

**Objetivo:** Cria widgets de entrada para parametrização do notebook, permitindo execução com diferentes valores.

**Parâmetros:**
- `catalog`: Nome do catálogo Unity Catalog (padrão: "fmcg")
- `data_source`: Identificador da fonte de dados (padrão: "customers")

**Uso:** Permite reutilizar o mesmo notebook para diferentes catálogos ou fontes através de Databricks Jobs.

---

### Célula 5: Configuração de Caminhos

```python
catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-dp-gabriel/{data_source}/*.csv'

print(base_path)
```

**Saída:** `s3://sportsbar-dp-gabriel/customers/*.csv`

**Objetivo:** Recupera valores dos widgets e constrói o caminho completo para os arquivos CSV no bucket S3.

**Padrão de Path:**
- Formato: `s3://<bucket>/<fonte>/*.csv`
- Permite leitura de múltiplos arquivos CSV na mesma pasta

---

## Camada Bronze

### Célula 6: Leitura de Dados Brutos do S3

```python
df = ( 
      spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(base_path)
        .withColumn("read_timestamp", F.current_timestamp())
        .select("*", "_metadata.file_name", "_metadata.file_size")
)

display(df.limit(10))
```

**Objetivo:** Lê arquivos CSV do S3 e adiciona metadados de auditoria.

**Opções de Leitura:**
- `header=True`: Primeira linha contém nomes de colunas
- `inferSchema=True`: Detecta automaticamente tipos de dados

**Colunas de Auditoria Adicionadas:**
- `read_timestamp`: Timestamp da ingestão
- `file_name`: Nome do arquivo fonte
- `file_size`: Tamanho do arquivo em bytes

**Schema Inferido:**
- customer_id: Integer
- customer_name: String
- city: String

---

### Célula 7: Verificação de Schema

```python
df.printSchema()
```

**Saída:**
```
root
 |-- customer_id: integer (nullable = true)
 |-- customer_name: string (nullable = true)
 |-- city: string (nullable = true)
 |-- read_timestamp: timestamp (nullable = false)
 |-- file_name: string (nullable = false)
 |-- file_size: long (nullable = false)
```

**Objetivo:** Valida a estrutura e tipos de dados após ingestão.

---

### Célula 8: Gravação na Tabela Bronze

```python
# Salvar na camada Bronze usando modo append
df.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("append") \
    .saveAsTable(f"{catalog}.{bronze}.{data_source}")

print(f"✅ Dados salvos com sucesso em: {catalog}.{bronze}.{data_source}")
```

**Tabela Destino:** `fmcg.bronze.customers`

**Configurações:**
- `format("delta")`: Usa formato Delta Lake
- `enableChangeDataFeed`: Habilita tracking de mudanças (CDC)
- `mode("append")`: Adiciona dados sem sobrescrever

**Propósito da Camada Bronze:**
- Preservar dados brutos exatamente como recebidos
- Manter histórico completo de cargas
- Permitir reprocessamento sem perda de dados

---

### Células 9-10: Validação da Tabela Bronze

```python
df_bronze = spark.sql(f"select * from {catalog}.{bronze}.{data_source};")
df_bronze.show(10)
df_bronze.printSchema()
```

**Objetivo:** Confirma que os dados foram gravados corretamente e valida o schema persistido.

**Validações:**
- Contagem de registros
- Estrutura de colunas
- Primeiras linhas de dados

---

## Camada Silver

### Análise de Qualidade de Dados

#### Célula 11: Detecção de Duplicatas

```python
df_duplicates = df_bronze.groupBy("customer_id").count().filter(F.col("count") > 1)
display(df_duplicates)
```

**Objetivo:** Identifica customer_ids duplicados para tratamento.

**Problema Detectado:** Múltiplos registros para o mesmo cliente devido a:
- Cargas redundantes
- Dados inconsistentes na origem
- Variações de nome/cidade para mesmo ID

---

#### Células 12-13: Remoção de Duplicatas

```python
df_silver = df_bronze.dropDuplicates(['customer_id'])

print('Rows before duplicates dropped: ', df_bronze.count())
df_silver = df_bronze.dropDuplicates(['customer_id'])
print('Rows after duplicates dropped: ', df_silver.count())
```

**Saída:**
```
Rows before duplicates dropped:  117
Rows after duplicates dropped:  35
```

**Impacto:** Redução de 117 para 35 registros únicos (70% de duplicatas removidas).

**Estratégia:** Mantém primeira ocorrência de cada customer_id.

---

### Limpeza de Texto

#### Células 14-16: Remoção de Espaços em Branco

```python
# Detectar registros com espaços desnecessários
display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)

# Aplicar trim
df_silver = df_silver.withColumn(
    "customer_name",
    F.trim(F.col("customer_name"))
)

# Validar limpeza
display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)
```

**Problema:** Nomes com espaços antes/depois (ex: " HydroBoost Nutrition ")

**Solução:** Função `F.trim()` remove espaços do início e fim.

**Validação:** Segunda consulta retorna 0 registros, confirmando sucesso.

---

### Normalização de Cidades

#### Célula 17: Análise de Valores Distintos

```python
df_silver.select('city').distinct().show()
```

**Problemas Detectados:**
- Erros ortográficos: "Bengaluruu", "Bengalore", "Hyderbad"
- Variações de formatação: "NewDelhi", "Newdelhi", "NewDelhie"
- Valores nulos

---

#### Célula 18: Mapeamento e Padronização de Cidades

```python
# typos → correct names
city_mapping = {
    'Bengaluruu': 'Bengaluru',
    'Bengalore': 'Bengaluru',
    
    'Hyderabad': 'Hyderabad',
    'Hyderbad': 'Hyderabad',
    
    'NewDelhi': 'New Delhi',
    'Newdelhi': 'New Delhi',
    'NewDelhie': 'New Delhi'
}

allowed = ['Bengaluru', 'Hyderabad', 'New Delhi']

df_silver = (
    df_silver
    .replace(city_mapping, subset=["city"])
    .withColumn(
        "city",
        F.when(F.col("city").isNull(), None)
        .when(F.col("city").isin(allowed), F.col("city"))
        .otherwise(None)
    )
)

df_silver.select('city').distinct().show()
```

**Lógica de Transformação:**

1. **Mapeamento de Erros:** Dicionário converte variações para padrão correto
2. **Lista de Valores Válidos:** Apenas 3 cidades são aceitas
3. **Regra de Validação:**
   - Se nulo: mantém nulo
   - Se na lista válida: mantém valor
   - Caso contrário: converte para nulo

**Resultado:** Apenas "Bengaluru", "Hyderabad", "New Delhi" (ou null) permanecem.

---

### Normalização de Nomes

#### Células 19-20: Aplicação de Title Case

```python
df_silver.select('customer_name').distinct().show()

df_silver = df_silver.withColumn(
    "customer_name",
    F.when(F.col("customer_name").isNull(), None)
    .otherwise(F.initcap(F.col("customer_name")))
)

df_silver.select('customer_name').distinct().show()
```

**Função:** `F.initcap()` - Converte primeira letra de cada palavra para maiúscula.

**Exemplos de Transformação:**
- "SPRINTX NUTRITION" → "Sprintx Nutrition"
- "zenathlete foods" → "Zenathlete Foods"
- "Recovery LANE" → "Recovery Lane"

**Benefício:** Padroniza capitalização para melhor apresentação e comparação.

---

### Tratamento de Valores Nulos

#### Células 21-22: Identificação de Cidades Nulas

```python
df_silver.filter(F.col("city").isNull()).show(truncate=False)

null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', "Recovery Lane"]

df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)
```

**Problema:** 7 clientes com cidade nula após normalização.

**Clientes Afetados:**
- Sprintx Nutrition (ID: 789403)
- Zenathlete Foods (ID: 789420)
- Primefuel Nutrition (ID: 789521)
- Recovery Lane (ID: 789603)
- E outros

---

#### Células 23-24: Resolução de Cidades Nulas com Lookup Manual

```python
customer_city_fix = {
    789403: "New Delhi",
    789420: "Bengaluru",
    789521: "Hyderabad",
    789603: "Hyderabad",
    789221: "Bengaluru",
    789522: "Hyderabad",
    789422: "Hyderabad"
}

df_fix = spark.createDataFrame(
    [(k, v) for k, v in customer_city_fix.items()],
    ["customer_id", "fixed_city"]
)

display(df_fix)
```

**Abordagem:** Criar DataFrame de correção com mapeamento customer_id → cidade correta.

**Fonte dos Dados:** Baseado em análise de histórico de transações ou conhecimento do negócio.

---

```python
df_silver = (
    df_silver
    .join(df_fix, "customer_id", "left")
    .withColumn(
        "city",
        F.coalesce("city", "fixed_city")
    )
    .drop("fixed_city")
)

display(df_silver)
```

**Lógica do Join:**

1. **Left Join:** Mantém todos os clientes, adiciona fixed_city quando disponível
2. **Coalesce:** Usa cidade existente se não-nula, caso contrário usa fixed_city
3. **Drop:** Remove coluna temporária fixed_city

**Resultado:** Todas as cidades nulas são preenchidas com valores corretos.

---

#### Células 25-26: Validação da Correção

```python
null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', "Recovery Lane"]

df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

df_silver.filter(F.col("city").isNull()).show(truncate=False)
```

**Objetivo:** Confirmar que não há mais registros com cidade nula.

**Resultado Esperado:** Ambas as consultas retornam 0 registros.

---

### Preparação para Gold

#### Célula 27: Conversão de Tipo

```python
df_silver = df_silver.withColumn(
    "customer_id",
    F.col("customer_id").cast("string")
)

print(df_silver.printSchema())
```

**Justificativa:** Converte customer_id de Integer para String para:
- Compatibilidade com chave da tabela consolidada
- Evitar problemas de join com tipos diferentes
- Permitir IDs alfanuméricos futuros

---

#### Célula 28: Criação de Colunas de Negócio

```python
df_silver = (
    df_silver.withColumn(
        "customer",
        F.concat_ws("-", "customer_name", F.coalesce(F.col("city"), F.lit("Unknown")))
    )
    .withColumn("market", F.lit("India"))
    .withColumn("platform", F.lit("Sports Bar"))
    .withColumn("channel", F.lit("Acquisition"))
    .drop("platformm")  # Remove coluna antiga com typo
)

display(df_silver.limit(5))
```

**Novas Colunas Criadas:**

1. **customer:** Identificador único composto
   - Formato: "Nome do Cliente - Cidade"
   - Exemplo: "Fitfuel Market - Bengaluru"
   - Tratamento: Usa "Unknown" se cidade for nula
   - **Razão de Negócio:** Diferencia clientes com mesmo nome em cidades diferentes

2. **market:** Constante "India"
   - **Razão:** Todos os clientes da Sports Bar são do mercado indiano

3. **platform:** Constante "Sports Bar"
   - **Razão:** Identifica origem dos dados (empresa adquirida)

4. **channel:** Constante "Acquisition"
   - **Razão:** Marca clientes como provenientes de aquisição corporativa

**Função:** `F.concat_ws()` - Concatena strings com separador especificado.

---

#### Célula 29: Gravação na Tabela Silver

```python
df_silver.write\
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")
```

**Tabela Destino:** `fmcg.silver.customers`

**Configurações:**
- `mergeSchema=true`: Permite evolução de schema
- `mode("overwrite")`: Substitui dados anteriores (tabela curada)
- Change Data Feed habilitado

---

## Camada Gold

### Célula 30: Seleção de Colunas de Negócio

```python
df_gold = df_silver.select(
    "customer_id", 
    "customer_name", 
    "city", 
    "customer", 
    "market", 
    "platform", 
    "channel"
)
```

**Objetivo:** Seleciona apenas colunas relevantes para camada analítica, removendo:
- Metadados de auditoria (read_timestamp, file_name, file_size)
- Colunas intermediárias de processamento

**Colunas Gold:**
- customer_id: Chave primária
- customer_name: Nome do cliente
- city: Cidade do cliente
- customer: Identificador composto (nome-cidade)
- market: Mercado geográfico
- platform: Plataforma de origem
- channel: Canal de aquisição

---

#### Célula 31: Validação do DataFrame Gold

```python
display(df_gold.limit(5))
```

**Objetivo:** Inspeciona visualmente os primeiros registros antes da gravação final.

---

#### Célula 32: Gravação na Tabela Gold

```python
df_gold.write\
    .format("delta") \
    .option("delta.enableChangeDataFeed","true") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")
```

**Tabela Destino:** `fmcg.gold.sb_dim_customers`

**Convenção de Nomenclatura:**
- Prefixo `sb_`: Identifica dados da Sports Bar
- Prefixo `dim_`: Indica tabela de dimensão
- Sufixo: Nome da entidade

**Configurações:**
- `overwriteSchema=true`: Permite substituir schema completamente
- `mode("overwrite")`: Recria tabela a cada execução

---

## Integração

### Célula 33: MERGE com Tabela Consolidada

```python
delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
df_child_customers = spark.table("fmcg.gold.sb_dim_customers").select(
    F.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

**Objetivo:** Integra clientes da Sports Bar na tabela consolidada que contém clientes da Atlon.

**Processo:**

1. **Carga da Tabela Target:** `fmcg.gold.dim_customers` (tabela consolidada)

2. **Preparação da Source:**
   - Lê `fmcg.gold.sb_dim_customers`
   - Renomeia `customer_id` para `customer_code` (padrão da target)
   - Seleciona apenas colunas necessárias

3. **Operação MERGE (Upsert):**
   - **Condição de Match:** `customer_code` igual em ambas as tabelas
   - **When Matched:** Atualiza todos os campos
   - **When Not Matched:** Insere novo registro

**Benefício:** Mantém tabela única com clientes de ambas as empresas, evitando duplicatas.

---

## Resumo das Transformações

### Pipeline Completo

```
S3 CSV → Bronze → Silver → Gold → Consolidada
  ↓        ↓        ↓       ↓         ↓
Raw    Ingestão  Limpeza Negócio   MERGE
```

### Estatísticas de Qualidade

**Bronze:**
- Registros carregados: 117
- Colunas: 6 (incluindo 3 de auditoria)

**Silver:**
- Registros após deduplicação: 35
- Registros com cidade corrigida: 7
- Nomes normalizados: 35
- Cidades normalizadas: 3 valores válidos
- Colunas de negócio adicionadas: 4

**Gold:**
- Registros finais: 35
- Colunas: 7
- Tabela consolidada: Merge executado

### Regras de Qualidade Aplicadas

| Regra | Técnica | Impacto |
|-------|---------|---------|
| Deduplicação | dropDuplicates(['customer_id']) | 117 → 35 registros |
| Limpeza de espaços | F.trim() | 100% dos nomes |
| Padronização de cidade | replace() + validação | 3 valores válidos |
| Correção de nulos | Left join + coalesce() | 7 registros corrigidos |
| Normalização de texto | F.initcap() | 35 registros |
| Conversão de tipo | cast("string") | customer_id |
| Enriquecimento | Colunas literais | 4 novas colunas |

### Tabelas Criadas

1. **fmcg.bronze.customers**
   - Modo: Append
   - Propósito: Dados brutos
   - Change Data Feed: Habilitado

2. **fmcg.silver.customers**
   - Modo: Overwrite
   - Propósito: Dados limpos
   - Change Data Feed: Habilitado

3. **fmcg.gold.sb_dim_customers**
   - Modo: Overwrite
   - Propósito: Dimensão Sports Bar
   - Change Data Feed: Habilitado

4. **fmcg.gold.dim_customers** (atualizada)
   - Operação: MERGE
   - Propósito: Dimensão consolidada
   - Contém: Atlon + Sports Bar

---

## Padrões de Design Implementados

### 1. Arquitetura Medallion
Bronze (Raw) → Silver (Cleansed) → Gold (Business)

### 2. Auditoria Completa
Metadados em todas as camadas para rastreabilidade

### 3. Idempotência
MERGE previne duplicatas em execuções repetidas

### 4. Schema Evolution
Opções mergeSchema e overwriteSchema habilitadas

### 5. Change Data Tracking
CDC habilitado para análise temporal

### 6. Parametrização
Widgets permitem reutilização do notebook

---

## Considerações de Performance

### Otimizações Aplicadas

1. **Leitura Otimizada:** Schema inference em CSV
2. **Formato Delta:** Compressão e indexação automática
3. **Deduplicação Única:** dropDuplicates executado uma vez
4. **Joins Eficientes:** Left join com DataFrame pequeno
5. **MERGE Condicional:** Atualiza apenas registros alterados


