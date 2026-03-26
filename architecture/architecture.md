# 🏗️ Arquitetura do Pipeline

## 📌 Visão Geral

Este projeto implementa uma arquitetura moderna de Data Lake na AWS, baseada no padrão:

**RAW → CURATED → GOLD**

Essa abordagem permite separar responsabilidades, garantir qualidade dos dados e otimizar consumo analítico.

---

## 🔄 Fluxo de Dados

ERP → S3 (RAW) → Athena → Glue ETL → S3 (Curated) → Data Catalog → Athena (Gold) → QuickSight

1. ERP (dados simulados)
2. S3 RAW (dados brutos)
3. Athena (análise exploratória)
4. Glue (ETL + Data Quality)
5. S3 Curated (dados tratados)
6. Glue Crawlers (catalogação)
7. Athena (camada analítica)
8. QuickSight (visualização)

---

## 🧱 Camadas da Arquitetura

### 🟡 RAW (Landing Zone)

- Armazenamento de dados brutos em formato CSV  
- Dados sem tratamento (dirty data)  
- Estrutura: `raw/hortfruit/year=2025/dirty/`  
- Objetivo:
  - Rastreabilidade
  - Auditoria
  - Reprocessamento  

---

### 🟢 CURATED (Processing Layer)

- Dados tratados via AWS Glue (PySpark)  
- Conversões aplicadas:
  - string → double  
  - string → date  
- Padronização de categorias  
- Aplicação de regras de negócio  
- Escrita em formato **Parquet (otimizado para leitura)**  
- Particionamento por:
  - ano  
  - mês  

- Inclusão de **Data Quality (DQ)**:
  - registros classificados como:
    - clean
    - flagged
    - rejected  

---

### 🔵 GOLD (Semantic Layer)

- Dados agregados e prontos para consumo  
- Implementação via **views no Athena**  
- Principais objetivos:
  - Simplificar consultas analíticas  
  - Centralizar lógica de negócio  
  - Reduzir complexidade para BI  

---

## ⚙️ Tecnologias Utilizadas

- Amazon S3 (Data Lake)
- AWS Glue (ETL com PySpark)
- AWS Glue Crawlers (catalogação automática)
- AWS Data Catalog
- Amazon Athena (query engine serverless)
- Amazon QuickSight (BI)

---

## 🧠 Decisões Arquiteturais

- Uso de **S3** como Data Lake devido à escalabilidade e baixo custo  
- Uso de **Parquet** para reduzir custo de leitura no Athena  
- Particionamento para otimização de queries  
- Uso de **Athena** para evitar necessidade de infraestrutura (serverless)  
- Uso de **views (Gold)** ao invés de tabelas físicas para flexibilidade  
- Separação em camadas para garantir governança e organização  

---

## 🎯 Objetivos da Arquitetura

- Escalabilidade horizontal  
- Redução de custos  
- Alta performance de consulta  
- Governança de dados  
- Facilidade de manutenção