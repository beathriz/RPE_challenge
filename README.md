# RPE_challenge
Pipeline de dados end-to-end com Databricks usando o dataset público da Olist. 
Projeto com ingestão, transformação (Bronze e Silver) e orquestração com Workflows.   

ORIGEM:

A origem dos dados são subconjunto de dados do "Brazilian E-Commerce Public Dataset by Olist" no qual estamos usando 3 arquivos no formato .csv. A arquitetura tem 3 camadas: Bronze, Silver_1 e Silver_2 (da qual acredito ser a Gold mas estou mantendo o que está descrito no desafio).

BRONZE:

Na camada Bronze os arquivos são lidos, usados em um dataframe e analisados para verificar se é necessário alterações. 
Na camada Bronze não são aplicadas alterações. O formato 2017-02-02T14:08:10.000+00:00 é padrão ISO 8601 válido para timestamp.
O Spark reconhece esse formato como válido para TimestampType e, ao inferir o schema, ele já transforma internamente esses valores em objetos de tipo timestamp — mesmo que a visualização ainda mostre o mesmo formato de string ISO (isso é apenas a forma de exibição).
Após isso são persistidos como delta. 

SILVER_1 — Transformação de Dimensão (silver.dim_customers):

Na primeira camada Silver, o foco foi transformar a dimensão customers com os seguintes passos:
1. Leitura da tabela bronze.olist_customers.
2. Seleção e renomeação de colunas para nomes mais descritivos (ex: customer_unique_id para unique_id, customer_zip_code_prefix para zip_prefix).
3. Padronização do nome das cidades para letras maiúsculas com upper().
4. Remoção de duplicatas para garantir a unicidade do campo customer_id.

Verificações de Qualidade:
Verificação de nulos nos campos principais.
Verificação de duplicidade de customer_id.
Verificação do padrão da coluna state (deve conter exatamente 2 caracteres).

Os dados tratados foram gravados na tabela Delta:
silver.dim_customers

SILVER_2 — Transformação de Fatos (silver.fct_order_items):

Apesar de estar descrito como "Silver Layer" no desafio, considerei essa etapa como uma espécie de Silver_2 (ou mesmo Gold, na prática), pois consolida dados de diferentes fontes em uma tabela de fatos.
Nessa etapa, foram combinadas as tabelas: bronze.olist_order_items, bronze.olist_orders e silver.dim_customers

Foram feitos joins com base nos campos order_id e customer_id, e criadas métricas:
preco_total_item = price + freight_value
preco_medio_unitario = preco_total_item / order_item_id

Verificações de Qualidade:
Ausência de valores nulos nas colunas principais (order_id, price, etc.).
Validação para impedir valores negativos em price e freight_value.

O resultado foi salvo como tabela Delta:
silver.fct_order_items

WORKFLOW:

A funcionalidade de Workflows (Jobs) não está disponível na Databricks Community Edition.
Abaixo está a descrição de como o workflow seria configurado, conforme solicitado:

- Tarefa 1: `Ingestao_Bronze`
- Tarefa 2: `Dim_Customers` (executa após o sucesso da Tarefa 1)
- Tarefa 3: `Fatos_OrderItems` (executa após o sucesso da Tarefa 2)
- Gatilho: execução manual
Cada tarefa seria configurada como execução de notebook, rodando no mesmo cluster compartilhado, com dependências definidas na aba “Task Dependencies”.
Coloquei gatilho de execução manual pois na Community Edition não é possível configurar gatilhos automáticos, como agendamentos com cron ou execuções diárias. Porém, em um ambiente de produção, seria possível agendar a execução do pipeline para ocorrer diariamente durante a madrugada, por exemplo, às 3h, para não interferir no fluxo de dados do horário comercial. 

Exemplo: 
schedule:
  quartz_cron_expression: "0 0 3 * * ?"
  timezone_id: "America/Sao_Paulo"

EXEMPLOS DE QUERIE SQL (para consultar e verificar os dados) 


-- Visualizar os primeiros registros
SELECT * FROM silver.dim_customers LIMIT 10;

-- Verificar quantidade de clientes por estado
SELECT state, COUNT(*) AS total_clientes
FROM silver.dim_customers
GROUP BY state
ORDER BY total_clientes DESC;

-- Verificar cidades com mais clientes
SELECT city, COUNT(*) AS total
FROM silver.dim_customers
GROUP BY city
ORDER BY total DESC
LIMIT 5;

-- Visualizar os primeiros registros da tabela de fatos
SELECT * FROM silver.fct_order_items LIMIT 10;

-- Total de pedidos e valor médio por status
SELECT order_status, COUNT(*) AS total_pedidos,
       ROUND(AVG(preco_total_item), 2) AS preco_medio
FROM silver.fct_order_items
GROUP BY order_status;

-- Top 5 produtos com maior valor total de venda
SELECT product_id, SUM(preco_total_item) AS total_vendido
FROM silver.fct_order_items
GROUP BY product_id
ORDER BY total_vendido DESC
LIMIT 5;

-- Frete médio por estado de destino
SELECT c.state, ROUND(AVG(f.freight_value), 2) AS frete_medio
FROM silver.fct_order_items f
JOIN silver.dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.state
ORDER BY frete_medio DESC;

PROCESSAMENTO INCREMENTAL:

ORIGEM - Necessário adaptação na origem pois podemos incluir a ingestão contínua de novos dados, integrando com fontes como diretórios monitorados ou Kafka (Structured Streaming) 

Auto Loader com Delta Lake: detectar automaticamente novos arquivos no armazenamento e realizar a carga incremental sem reprocessar dados antigos.

Delta MERGE (upsert): aplicar lógica de atualização/inserção para manter os dados nas camadas Silver ou Gold atualizados sem recriar tudo.

Filtragem incremental: usar colunas como order_purchase_timestamp para identificar e processar apenas registros novos ou alterados.
