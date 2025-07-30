# Pipeline Avançado de Análise de Sessões de Usuários com PySpark
### Da "Sessionization" à Criação de uma Visão 360º do Cliente

![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange.svg)
![Analysis](https://img.shields.io/badge/Analysis-Behavioral-blueviolet.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

Este repositório apresenta um pipeline de dados de ponta a ponta, construído em PySpark, que transforma logs de eventos brutos de e-commerce em datasets analíticos de alto valor. O projeto vai muito além de uma simples "sessionização", demonstrando como construir uma base sólida para análises comportamentais, segmentação de clientes e iniciativas de ciência de dados.

Utilizamos um grande dataset open-source de eventos de uma loja de cosméticos para simular um cenário de produção realista e de larga escala.

## 🎯 Visão Geral do Projeto

Logs de eventos brutos, por si só, são como um rio de informações desestruturadas. Para extrair valor, precisamos agrupar as ações dos usuários em "sessões" coerentes e, a partir delas, entender seus padrões de comportamento.

Este pipeline executa uma jornada analítica completa, transformando dados brutos em dois outputs principais:
1.  **Tabela de Sessões:** Cada linha representa uma sessão de usuário única, enriquecida com métricas e features de comportamento (duração, contagem de eventos, análise de funil, etc.).
2.  **Tabela de Usuários (Visão 360º):** Cada linha representa um usuário único, com métricas agregadas que descrevem todo o seu ciclo de vida (total de sessões, tempo médio entre visitas, total de compras, etc.).

## 🗺️ A Jornada Analítica: De Eventos a Insights

O pipeline é estruturado em etapas lógicas que progressivamente enriquecem os dados.

### Etapa 1: Limpeza e Preparação dos Dados
A base de tudo é ter dados limpos. Esta etapa carrega os dados brutos do arquivo CSV, aplica um schema rigoroso para garantir a consistência dos tipos de dados, converte as strings de tempo para o formato `Timestamp` do Spark e remove registros com informações essenciais ausentes.

### Etapa 2: A "Sessionization" (O Coração da Lógica)
Esta é a etapa central, onde os eventos são agrupados em sessões.

* **Conceito:** Uma sessão é uma sequência de eventos de um usuário, interrompida por um período de inatividade (ex: 30 minutos).
* **Técnica:** Utilizamos **Window Functions** do PySpark para realizar essa tarefa de forma eficiente e escalável.
    1.  `Window.partitionBy("user_id").orderBy("event_timestamp")`: Criamos uma "janela" que nos permite olhar para os eventos de cada usuário de forma sequencial e ordenada no tempo.
    2.  `lag("event_timestamp", 1)`: Dentro dessa janela, usamos a função `lag` para acessar o timestamp do evento *anterior*.
    3.  `when(diff > timeout, 1)`: Calculamos a diferença de tempo. Se for maior que o nosso limite de inatividade (ou se for o primeiro evento do usuário), marcamos essa linha como o início de uma nova sessão.
    4.  `sum(...) over(window)`: Usamos uma soma cumulativa sobre essa marcação. O resultado é um ID de grupo que incrementa a cada nova sessão, nos permitindo atribuir um `session_id` único.

### Etapa 3: Engenharia de Features a Nível de Sessão
Com as sessões definidas, agregamos os dados para descrever o que aconteceu dentro de cada uma.

* **Métricas Básicas:** Duração da sessão, número de eventos, produtos distintos visualizados.
* **Features Temporais:** Hora do dia e dia da semana do início da sessão, para entender padrões de acesso.
* **Análise de Funil de Conversão:** Analisamos os tipos de evento (`view`, `cart`, `purchase`) dentro de cada sessão para determinar o estágio mais profundo que o usuário alcançou (`view_only`, `added_to_cart`, ou `completed_purchase`).

### Etapa 4: Agregação para a Visão 360º do Usuário
A etapa final eleva a análise do nível de sessão para o nível de usuário, criando um perfil completo do comportamento de cada cliente ao longo do tempo.

* **Métricas de Lifetime:** Total de sessões, total de eventos, número de sessões com compra.
* **Métricas Comportamentais:** Duração média das sessões.
* **Análise de Recorrência:** Usando a função `lag` novamente (desta vez sobre as sessões), calculamos o **tempo médio entre as visitas** de um usuário, um indicador poderoso de engajamento.

## 🏗️ Arquitetura e Boas Práticas

* **Código Modular e Orientado a Objetos:** Toda a lógica está encapsulada na classe `AdvancedRetailAnalyticsPipeline`, com métodos privados para cada etapa do processo, tornando o código limpo e de fácil manutenção.
* **Configuração Centralizada:** Um arquivo `config.yaml` controla todos os parâmetros e caminhos, permitindo fácil ajuste sem alterar o código.
* **Otimização de Performance:** O pipeline utiliza `.cache()` para armazenar em memória o DataFrame intermediário de eventos "sessionizados". Como este DataFrame é usado como base para múltiplas agregações, o cache evita recálculos e acelera significativamente o processo.

## 🚀 Como Executar o Projeto

Siga os passos abaixo para configurar e rodar o pipeline.

### 1. Pré-requisitos
* Python 3.9 ou superior.
* Java Development Kit (JDK) 8 ou 11.
* Instale as dependências Python:
    ```bash
    pip install pyspark pyyaml
    ```

### 2. Download do Dataset
Este projeto utiliza um dataset público do Kaggle.
1.  Vá para a página do dataset: [E-commerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechin/ecommerce-behavior-data-from-multi-category-store).
2.  Baixe o arquivo `2019-Oct.csv`.
3.  Crie a seguinte estrutura de diretórios em seu projeto: `data/source/`.
4.  Coloque o arquivo `2019-Oct.csv` dentro da pasta `data/source/`.

### 3. Crie o Arquivo de Configuração
Crie um arquivo chamado **`config.yaml`** na raiz do seu projeto com o seguinte conteúdo:
```yaml
pipeline_name: "AdvancedRetailUserAnalysis"
log_level: "INFO"

spark:
  app_name: "AdvancedRetailAnalyticsPipeline"
  master: "local[*]"

session_timeout_seconds: 1800 # 30 minutos

data:
  source_path: "data/source/2019-Oct.csv"
  output_path:
    sessionized_events: "data/intermediate/sessionized_events.parquet"
    session_features: "data/analytical/session_features.parquet"
    user_summary: "data/analytical/user_summary.parquet"
```

4. Execução

Com os arquivos no lugar, execute o script Python principal:

```Bash
python advanced_retail_analytics_pipeline.py
```


📊 Estrutura dos Dados de Saída
O pipeline gera dois datasets analíticos principais no formato Parquet:

session_features.parquet: Contém uma linha por sessão, com colunas como:

`session_id`, `user_id`, `session_start_time`, `session_end_time`, `total_events`, `session_duration_seconds`, `funnel_stage`, etc.

user_summary.parquet: Contém uma linha por usuário, com colunas como:

`user_id`, `total_sessions`, `total_events_lifetime`, `total_purchase_sessions`, `avg_session_duration_seconds`, `avg_time_between_sessions_hours`, etc.

Estes datasets são a base para inúmeras aplicações, desde a criação de dashboards em ferramentas de BI até a segmentação de clientes e a modelagem de propensão à compra em projetos de ciência de dados.

