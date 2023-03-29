# Processo de ETL @ Raízen Analytics


Este documento apresenta o projeto técnico de um Pipeline ETL de Vendas de Combustíveis da ANP (Agência Nacional do Petróleo, Gás Nautral e Biocombustíveis), detalhando o processo de extração e estruturação dos dados de tabelas dinâmicas.

O objetivo desse projeto é criar um processo eficiente de ETL, de modo a coletar e organizar dados de vendas de combustíveis de forma consistente e confiável. Para isso, foram utilizadas ferramentas de ETL como o Apache Airflow.

O Pipeline ETL foi desenvolvido em fases distintas, incluindo extração dos dados brutos, transformação dos dados para adequá-los às necessidades de análise, e carregamento dos dados em um banco de dados.

Tabelas de Referência:

 * Vendas, pelas distribuidoras, dos derivados combustíveis de petróleo por Unidade da Federação e produto - 2000-2023 (m³)

![tab_din_petroleo](https://user-images.githubusercontent.com/116772002/228603848-6e4a670e-1903-4a83-97f0-3e8275d95588.jpg)


 * Vendas, pelas distribuidoras, de óleo diesel por tipo e Unidade da Federação - 2013-2023 (m³)

![tab_din_diesel](https://user-images.githubusercontent.com/116772002/228603894-d7aff146-8221-439d-bf01-2b83cfe8351c.jpg)




# ⚙️ Extração dos Dados

Arquivo com dados brutos disponibilizados pela [ANP](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-estatisticos) (Agência Nacional do Petróleo, Gás Nautral e Biocombustíveis).
 



# 🔩 Dicionário de Dados


* <b>Tabelas Stage (Óleo Diesel e Derivados Combustíveis de Petróleo)</b>

| **variável** |   **tipo**  |            **descrição**           |
|:------------:|:-----------:|:----------------------------------:|
|  COMBUSTÍVEL | VARCHAR(30) | Derivados Combustíveis de Petróleo ou Óleo Diesel|
|      ANO     |     INT     |        Ano referente a venda       |
|    REGIÃO    | VARCHAR(50) |          Região do Brasil          |
|    ESTADO    | VARCHAR(50) |          Estado do Brasil          |
|    UNIDADE   |  VARCHAR(3) |           metros cúbicos           |
|      Jan     |    FLOAT    |      Vendas referentes ao mês      |
|      Fev     |    FLOAT    |      Vendas referentes ao mês      |
|      Mar     |    FLOAT    |      Vendas referentes ao mês      |
|      Abr     |    FLOAT    |      Vendas referentes ao mês      |
|      Mai     |    FLOAT    |      Vendas referentes ao mês      |
|      Jun     |    FLOAT    |      Vendas referentes ao mês      |
|      Jul     |    FLOAT    |      Vendas referentes ao mês      |
|      Ago     |    FLOAT    |      Vendas referentes ao mês      |
|      Set     |    FLOAT    |      Vendas referentes ao mês      |
|      Out     |    FLOAT    |      Vendas referentes ao mês      |
|      Nov     |    FLOAT    |      Vendas referentes ao mês      |
|      Dez     |    FLOAT    |      Vendas referentes ao mês      |
|     TOTAL    |    FLOAT    |      Vendas referentes ao mês      |


* <b>Tabelas Final (Óleo Diesel e Derivados Combustíveis de Petróleo)</b>

| **variável** |   **tipo**  |                   **descrição**                   |
|:------------:|:-----------:|:-------------------------------------------------:|
|  year_month  |     DATE    |            Data de referência da Venda            |
|      uf      | VARCHAR(50) |                  Estado do Brasil                 |
|    product   | VARCHAR(30) | Derivados Combustíveis de Petróleo ou Óleo Diesel |
|     unit     |  VARCHAR(2) |                   metros cúbicos                  |
|    volume    |    FLOAT    |                       Vendas                      |
|  created_at  |   DATETIME  |                  Data do registro                 |
|              |             |                                                   |



# 📦 Desenvolvimento





Para o desenvolvimento deste projeto, foram realizados dois processos de ETL, um sendo realizado de forma local com Python e o outro processo utilizando o Apache Airflow, sendo processado em um container Docker. 

Nesta seção de desenvolvimento, será detalhado cada um dos processos de ETL.

Verificando as tabelas dinâmicas do arquivo xlsx disponibilizado pela ANP (Agência Nacional do Petróleo, Gás Natural e Biocombustíveis), foi utilizada a opção de visualizar os dados brutos referentes à tabela dinâmica para ter acesso a todas as informações de vendas disponibilizadas pela ANP.



## ETL - Python

- Arquitetura do Processo de ETL com Python
![arquitetura_v1](https://user-images.githubusercontent.com/116772002/228604438-5e470910-6453-4e9f-b2af-4223bbe9beba.png)

Após a verificação do arquivo com os dados brutos, foi realizada a leitura desses dados utilizando Python, onde foram lidos os dados brutos referentes às informações de vendas de Derivados Combustíveis de Petróleo e Óleo Diesel. Realizou-se uma análise exploratória dos arquivos, criou-se um dataframe e concatenou-se as informações relevantes sobre as vendas. 

O dataframe resultante contém 4968 linhas e 18 colunas para as informações sobre Petróleo e um dataframe com 1350 linhas e 17 colunas para as informações sobre Óleo Diesel. Os dataframes foram enviados para o schema Stage no banco de dados SQL Server, utilizando uma função para conectar com o banco de dados por meio da biblioteca SQLAlchemy.

Shape e Tabela Stage (petróleo)

![df_stage_pet_shape](https://user-images.githubusercontent.com/116772002/228604895-d7df8cdc-fbec-45aa-b860-1b15a1e00ea9.jpg)

![stage_petroleo_sqlserver](https://user-images.githubusercontent.com/116772002/228605474-2206b514-5124-4038-99cb-7798c6059d29.jpg)

Shape e Tabela Stage (diesel)

![df_stage_diesel_shape](https://user-images.githubusercontent.com/116772002/228604935-5cee69c0-db28-44a1-bed6-dc03a94dcd8d.jpg)

![stage_diesel_sqlserver](https://user-images.githubusercontent.com/116772002/228605510-2110e1f2-3c0b-4983-ab93-7d2a6e69f586.jpg)




Em outro script Python, foi realizada a leitura desses dados brutos no schema Stage e armazenados em um dataframe stage. Em seguida, foram criadas algumas variáveis auxiliares para a organização dos dados do dataframe final. Então, foi criada uma função para concatenar as informações e realizar a criação desse dataframe final.
Realizando iterações utilizando o método iterrows, foi realizado o mapeamento dos valores referentes ao produto, ano e mês para inserir os dados no novo dataframe. 
Chegando no dataframe final referente às informações sobre Petróleo com 59616 linhas e 6 colunas e um dataframe final com as informações sobre Óleo Diesel  16200 linhas e 6 colunas.

Shape e Tabela Final (petróleo)

![df_final_pet_shape](https://user-images.githubusercontent.com/116772002/228605597-fa7a3b31-8de5-4d63-b5d4-ef825ca06f2e.jpg)
 
![final_petroleo_sqlserver](https://user-images.githubusercontent.com/116772002/228605703-d9f0b6c3-dfae-4994-8c09-570b53f8243b.jpg)

Shape e Tabela Final (diesel)

![df_final_diesel_shape](https://user-images.githubusercontent.com/116772002/228605618-aaf011ed-b7dd-45e7-bcb7-b3a2a1a2fe08.jpg)

![final_diesel_sqlserver](https://user-images.githubusercontent.com/116772002/228605729-31df4ac6-98a6-41e8-8f53-63c948f99b81.jpg)




Os dataframes finais foram enviados para o schema Final no banco de dados SQL Server utilizando uma função para conectar ao banco de dados.

Após a inserção dos dados no banco de dados, foram realizadas consultas de validação para obter o somatório dos valores correspondentes à tabela dinâmica. Essas consultas foram executadas com o objetivo de verificar se os dados inseridos foram armazenados corretamente e se os cálculos estão sendo feitos de forma precisa. Essa etapa é fundamental para garantir a integridade dos dados e evitar erros nos resultados obtidos.

## ETL - Airflow 

 - Arquitetura utilizando Apache Airflow
![Diagrama sem nome drawio (2)](https://user-images.githubusercontent.com/116772002/228604080-2fa7076b-1c60-4bd6-b869-4b5c545d8d81.png)

Para implantar o Airflow em um container Docker, utilizou-se um arquivo YAML fornecido pela Apache Airflow que contém vários serviços essenciais para o funcionamento da ferramenta, tais como:
 - airflow-scheduler - responsável por monitorar todas as tarefas e DAGs e acionar as instâncias de tarefa assim que suas dependências são concluídas.

 - airflow-webserver - o servidor web que permite acessar a interface gráfica do Airflow.

 - airflow-worker - worker que executa as tarefas definidas pelo escalonador.

 - airflow-triggerer - acionador executa um loop de eventos para tarefas adiáveis.

 - airflow-init - serviço de inicialização.

 - postgres - banco de dados utilizado pelo Airflow.

 - redis - broker que encaminha mensagens do escalonador para o worker.

![docker](https://user-images.githubusercontent.com/116772002/228605785-191f3515-978f-4990-a1ce-bd23906ef955.jpg)



Referente ao processo de ETL sendo orquestrado pelo Apache Airflow, o desenvolvimento do código foi semelhante, sendo necessário alterar algumas funções para adequar às necessidades de processamento de dados no Airflow e também ao envio das informações para um banco de dados. Neste caso, foi utilizado o banco de dados PostgreSQL para armazenar os dados do dataframe stage e final.

![pipeline_air](https://user-images.githubusercontent.com/116772002/228634072-c1f45478-092f-4771-878c-171e699c9fe7.jpg)

Amostra de Dados inseridos no PostgreSQL

![stage_postgres](https://user-images.githubusercontent.com/116772002/228637651-0c57dd80-cfc7-4296-a4a6-3b275b54489c.jpg)









# 🛠️ Construído com


* [Python](https://www.python.org/)
* [Microsoft SQL Server Management Studio](https://www.microsoft.com/pt-br/sql-server/sql-server-downloads) - Banco de dados SQL Server
* [Apache Airflow](https://airflow.apache.org/) - Orquestrador de Fluxo de Trabalho (Pipelines)
* [Docker](https://www.docker.com/products/docker-desktop/) - Ferramenta para criação, execução e gerenciamento de contêineres 
* [PostgreSQL](https://www.postgresql.org/) - Banco de dados PosgreSQL


# 🏃 Autor


* **Fernando Barbosa** - *Engenheiro de Dados Jr* - [github](https://github.com/FernandoBarbosaB)
