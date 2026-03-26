## 📊 Dataset & Data Dictionary (RAW Layer)

Este projeto utiliza um dataset simulado de vendas de hortifruti, contendo aproximadamente **5.000 registros** e **22 colunas**, representando transações com inconsistências típicas de dados reais.

---

## 📥 Fonte dos Dados

Dataset gerado de forma simulada, representando um sistema ERP de vendas de hortifruti, com inserção intencional de inconsistências para simular cenários reais de baixa qualidade de dados.

---

## 🧾 Visão Geral

A camada **RAW** representa a ingestão inicial no Data Lake (Amazon S3), onde os dados são armazenados **sem tratamento e no formato original**.

Essa camada pode conter:

* Inconsistências de dados
* Problemas de formatação
* Valores nulos ou inválidos
* Diferenças de padronização

O principal objetivo é garantir **rastreabilidade, auditoria e reprocessamento**.

---

## 📏 Granularidade dos Dados

- Nível: transação de venda (linha por venda)
- Volume: ~5.000 registros
- Frequência simulada: diária

---

## 📚 Dicionário de Dados

| #  | Coluna               | Tipo (RAW)       | Tipo (CURATED) | Descrição (resumida)          | Problemas comuns no RAW          | Exemplo                |
| -- | -------------------- | ---------------- | -------------- | ----------------------------- | -------------------------------- | ---------------------- |
| 1  | sale_id              | string           | string         | ID único da venda             | Nulo, duplicado, espaços         | HF202500123            |
| 2  | data_venda           | string      | date           | Data da venda (base temporal) | Formatos inválidos/múltiplos     | 2025-03-15             |
| 3  | ano                  | string       | int            | Ano da venda                  | Inconsistente com data           | 2025                   |
| 4  | mes                  | string           | int            | Mês da venda                  | Formatos variados (3, 03, texto) | 3                      |
| 5  | loja                 | string           | string         | Unidade da venda              | Variação de escrita              | Hortfruit Vila Mariana |
| 6  | bairro_entrega       | string           | string         | Região de entrega             | Nulo ou inconsistente            | Moema                  |
| 7  | canal                | string           | string         | Canal de venda                | Diferença de nomenclatura        | Delivery app           |
| 8  | tipo_cliente         | string           | string         | Perfil do cliente             | Categorias inconsistentes        | PF                     |
| 9  | categoria            | string           | string         | Tipo de produto               | Problema de padronização         | Fruta                  |
| 10 | produto              | string           | string         | Nome do produto               | Erros e espaços extras           | Banana nanica          |
| 11 | unidade              | string           | string         | Unidade de medida             | Variações (kg, KG, etc.)         | kg                     |
| 12 | quantidade           | string | double         | Quantidade vendida            | Negativo, texto, vírgula         | 2.5                    |
| 13 | custo_unitario       | string   | double         | Custo por unidade             | Formato monetário                | 4.99                   |
| 14 | preco_venda_unitario | string   | double         | Preço de venda                | Símbolos ou valor inválido       | 7.99                   |
| 15 | promocao             | string       | int            | Indicador de promoção         | Texto ou nulo                    | 1                      |
| 16 | perda_quebra_custo   | string   | double         | Custo de perdas               | Nulo ou formato inválido         | 2.50                   |
| 17 | receita_total        | string   | double         | Receita da venda              | Pode estar incorreta             | 39.95                  |
| 18 | custo_total          | string   | double         | Custo total                   | Pode estar incorreto             | 25.00                  |
| 19 | lucro_total          | string   | double         | Lucro da venda                | Pode estar incorreto             | 14.95                  |
| 20 | margem_lucro         | string   | double         | Margem percentual             | Divisão inválida/erro            | 0.3745                 |
| 21 | fornecedor           | string           | string         | Origem do produto             | Pode ser nulo                    | CEAGESP                |
| 22 | forma_pagamento      | string           | string         | Forma de pagamento            | Variações de escrita             | Pix                    |

---

💡 **Observação:**
A camada RAW contém dados não tratados. Todas as validações, padronizações e cálculos são aplicados na camada Curated via AWS Glue.

---

## 🏷️ Classificação dos Dados

Durante o processamento (Glue ETL), os registros são classificados em:

- **clean** → dados válidos e prontos para uso  
- **flagged** → dados com inconsistências leves  
- **rejected** → dados inválidos que não atendem regras de qualidade  

---

## ⚠️ Regras de Data Quality (DQ) Aplicadas

As seguintes validações são aplicadas durante o processamento na camada Curated:

- sale_id não pode ser nulo ou duplicado  
- quantidade deve ser maior que zero  
- preco_venda_unitario ≥ custo_unitario  
- datas devem estar em formato válido  
- margem_lucro deve estar dentro de faixa aceitável  

---

## 🔗 Relação com o Pipeline

- RAW → ingestão de dados sem tratamento  
- CURATED → aplicação de Data Quality e regras de negócio  
- GOLD → consumo analítico e dashboards  

---