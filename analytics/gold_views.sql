-- VIEW: gold_sales_summary
-- Resumo geral de vendas por ano/mês, loja, canal e categoria
CREATE OR REPLACE VIEW hortfruit_db.gold_sales_summary AS
SELECT
  CAST(ano AS INTEGER) AS ano,
  CAST(mes AS INTEGER) AS mes,
  loja,
  canal,
  categoria,

  CAST(ROUND(SUM(receita_total), 2) AS DECIMAL(18,2)) AS receita_total,
  CAST(ROUND(SUM(custo_total),   2) AS DECIMAL(18,2)) AS custo_total,
  CAST(ROUND(SUM(lucro_total),   2) AS DECIMAL(18,2)) AS lucro_total,
  
  CAST(ROUND(AVG(margem_lucro), 4) AS DECIMAL(10,4)) AS margem_media,

  COUNT(DISTINCT sale_id) AS qtd_vendas,
  COUNT(DISTINCT produto) AS qtd_produtos_distintos

FROM hortfruit_db.curated_clean_year_2025

GROUP BY
  CAST(ano AS INTEGER),
  CAST(mes AS INTEGER),
  loja,
  canal,
  categoria;



-- VIEW: gold_product_summary
-- Desempenho por produto e categoria (inclui perdas e promoções)
CREATE OR REPLACE VIEW hortfruit_db.gold_product_summary AS
SELECT
  CAST(ano AS INTEGER) AS ano,
  CAST(mes AS INTEGER) AS mes,
  produto,
  categoria,

  CAST(ROUND(SUM(receita_total), 2) AS DECIMAL(18,2)) AS receita_total,
  CAST(ROUND(SUM(custo_total),   2) AS DECIMAL(18,2)) AS custo_total,
  CAST(ROUND(SUM(lucro_total),   2) AS DECIMAL(18,2)) AS lucro_total,

  CAST(ROUND(AVG(margem_lucro), 4) AS DECIMAL(10,4)) AS margem_media,

  CAST(ROUND(SUM(perda_quebra_custo), 2) AS DECIMAL(18,2)) AS perda_total,
  CAST(ROUND(SUM(quantidade), 3)       AS DECIMAL(18,3))   AS volume_total,

  CAST(ROUND(SUM(CASE WHEN promocao = 1 THEN receita_total ELSE 0 END), 2) AS DECIMAL(18,2)) AS receita_promocao,
  CAST(ROUND(SUM(CASE WHEN promocao = 0 THEN receita_total ELSE 0 END), 2) AS DECIMAL(18,2)) AS receita_sem_promocao

FROM hortfruit_db.curated_clean_year_2025

GROUP BY
  CAST(ano AS INTEGER),
  CAST(mes AS INTEGER),
  produto,
  categoria;



-- VIEW: gold_channel_client_summary
-- Vendas por canal, tipo de cliente e bairro (inclui ticket médio e promoções)
CREATE OR REPLACE VIEW hortfruit_db.gold_channel_client_summary AS
SELECT
  CAST(ano AS INTEGER) AS ano,
  CAST(mes AS INTEGER) AS mes,
  canal,
  tipo_cliente,
  bairro_entrega,

  CAST(ROUND(SUM(receita_total), 2) AS DECIMAL(18,2)) AS receita_total,
  CAST(ROUND(SUM(lucro_total),   2) AS DECIMAL(18,2)) AS lucro_total,

  CAST(ROUND(AVG(margem_lucro), 4) AS DECIMAL(10,4)) AS margem_media,

  COUNT(*) AS total_vendas,

  CAST(
    ROUND(SUM(receita_total) / NULLIF(COUNT(*),0), 2)
    AS DECIMAL(18,2)
  ) AS ticket_medio,

  SUM(CASE WHEN promocao = 1 THEN 1 ELSE 0 END) AS vendas_promocao,

  CAST(
    ROUND(
      SUM(CASE WHEN promocao = 1 THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*),0),
      4
    )
    AS DECIMAL(10,4)
  ) AS pct_vendas_promocao

FROM hortfruit_db.curated_clean_year_2025

GROUP BY
  CAST(ano AS INTEGER),
  CAST(mes AS INTEGER),
  canal,
  tipo_cliente,
  bairro_entrega;



-- VIEW: gold_supplier_loss_summary
-- Perdas e performance por fornecedor e categoria
CREATE OR REPLACE VIEW hortfruit_db.gold_supplier_loss_summary AS
SELECT
  CAST(ano AS INTEGER) AS ano,
  CAST(mes AS INTEGER) AS mes,
  fornecedor,
  categoria,

  CAST(ROUND(SUM(receita_total), 2) AS DECIMAL(18,2)) AS receita_total,
  CAST(ROUND(SUM(lucro_total),   2) AS DECIMAL(18,2)) AS lucro_total,

  CAST(ROUND(AVG(margem_lucro), 4) AS DECIMAL(10,4)) AS margem_media,

  CAST(ROUND(SUM(perda_quebra_custo), 2) AS DECIMAL(18,2)) AS perda_total,

  CAST(
    ROUND(SUM(perda_quebra_custo) / NULLIF(SUM(receita_total),0), 4)
    AS DECIMAL(10,4)
  ) AS pct_perda_sobre_receita

FROM hortfruit_db.curated_clean_year_2025

GROUP BY
  CAST(ano AS INTEGER),
  CAST(mes AS INTEGER),
  fornecedor,
  categoria;



  -- VIEW: gold_dq_summary
-- Quantidade de registros rejeitados por motivo (qualidade de dados)
CREATE OR REPLACE VIEW hortfruit_db.gold_dq_summary AS
SELECT
  motivo_rejeicao,
  COUNT(*) AS qtd
FROM hortfruit_db.rejected_fail_year_2025
GROUP BY motivo_rejeicao
ORDER BY qtd DESC;