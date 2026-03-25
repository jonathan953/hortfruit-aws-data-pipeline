import sys
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# =========================
# CONFIG (S3)
# =========================
# Caminhos no S3:
# - RAW: dados brutos (ainda não tratados)
# - CURATED: dados já limpos e prontos para análise
# - FLAGGED: dados válidos, mas com alertas de qualidade
# - REJECTED: dados inválidos que não podem ser utilizados
# - DQ_REPORT: resumo da qualidade dos dados processados
RAW_DIRTY_PATH = "s3://hortfruit-data-lake-2025-jonathan/raw/hortfruit/year=2025/dirty/"
OUT_CURATED_CLEAN = "s3://hortfruit-data-lake-2025-jonathan/curated_clean/hortfruit/year=2025/"
OUT_CURATED_FLAGGED = "s3://hortfruit-data-lake-2025-jonathan/curated_flagged/hortfruit/year=2025/"
OUT_REJECTED_FAIL = "s3://hortfruit-data-lake-2025-jonathan/rejected_fail/hortfruit/year=2025/"
OUT_DQ_REPORT = "s3://hortfruit-data-lake-2025-jonathan/dq_report/hortfruit/year=2025/"

# =========================
# CONTROLE DE EXECUÇÃO (AUDITORIA)
# =========================
# RUN_ID: identificador único da execução do job (formato data + hora)
# PROCESSED_AT: timestamp de quando os dados foram processados
RUN_ID_VALUE = spark.sql("SELECT date_format(current_timestamp(), 'yyyyMMdd_HHmmss') AS v").collect()[0]["v"]
PROCESSED_AT = F.current_timestamp()

# =========================
# LEITURA DOS DADOS BRUTOS (CSV)
# =========================
# Lê os arquivos CSV do S3 considerando:
# - primeira linha como cabeçalho
# - separador ';' (padrão Brasil)
# - tratamento correto de aspas
# - cada linha como um registro único
df_raw = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", "false")
    .csv(RAW_DIRTY_PATH)
)

# ===================================================================
# FUNÇÕES AUXILIARES DE LIMPEZA, PADRONIZAÇÃO E CONVERSÃO DE TIPOS
# ===================================================================
# Padroniza valores:
# - Remove espaços extras no início e fim
# - Converte strings vazias ("") ou só com espaços ("   ") em NULL
# - Garante consistência para validações e regras de negócio
def null_if_blank(c):
    return F.when(F.col(c).isNull(), None).otherwise(
        F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c)))
    )

# Limpa texto básico:
# - remove espaços no início e fim
# - transforma valores vazios em NULL
# - reduz múltiplos espaços internos para apenas um
def norm_text(c):
    x = null_if_blank(c)
    x = F.regexp_replace(x, r"\s+", " ")
    return x

# Padroniza texto em MAIÚSCULO
# Útil para comparações, regras e validações técnicas
def norm_upper(c):
    x = norm_text(c)
    return F.upper(x)

# Padroniza texto em Title Case
# Exemplo: "HORTFRUIT SANTO AMARO" -> "Hortfruit Santo Amaro"
# Útil para colunas descritivas que serão exibidas em análise ou dashboard
def norm_title(c):
    x = norm_text(c)
    return F.initcap(F.lower(x))

# Padroniza a categoria em valores canônicos
# Resolve variações como:
# - FRUTA / fruta / Frutas
# - LEGUME / legume
# - VERDURA / verdura
# Também remove acentos simples para facilitar a comparação
def norm_categoria(c):
    x = norm_upper(c)
    x = F.translate(x, "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ", "AAAAAEEEEIIIIOOOOOUUUUC")
    return (
        F.when(x.isNull(), None)
         .when(x.rlike(r"^FRUT"), F.lit("Fruta"))
         .when(x.rlike(r"^LEGUM"), F.lit("Legume"))
         .when(x.rlike(r"^VERD"), F.lit("Verdura"))
         .otherwise(F.initcap(F.lower(norm_text(c))))
    )

# Padroniza o indicador de promoção para valores binários:
# - 1 = em promoção
# - 0 = sem promoção
# Aceita variações como: 1/0, sim/não, true/false, yes/no
# Também remove acentos e espaços para melhorar a consistência
def norm_promocao(c):
    x = norm_upper(c)
    x = F.translate(x, "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ", "AAAAAEEEEIIIIOOOOOUUUUC")
    return (
        F.when(x.isNull(), F.lit(0))
         .when(x.isin("1", "TRUE", "SIM", "S", "YES", "Y"), F.lit(1))
         .when(x.isin("0", "FALSE", "NAO", "NÃO", "NAO ", "N"), F.lit(0))
         .otherwise(F.col(c).cast("int"))
    )

# Converte textos numéricos para tipo double, tratando formatos comuns do padrão brasileiro:
# - remove "R$"
# - remove espaços
# - troca vírgula por ponto
# - remove caracteres inválidos
# Se a conversão falhar, o resultado será NULL
def to_double_ptbr(c):
    x = F.col(c)
    x = F.when(x.isNull(), None).otherwise(F.trim(x))
    x = F.regexp_replace(x, "R\\$", "")
    x = F.regexp_replace(x, r"\s+", "")
    x = F.regexp_replace(x, ",", ".")
    x = F.regexp_replace(x, r"[^0-9\.\-]", "")
    return x.cast("double")

# Converte textos para data, tentando múltiplos formatos de entrada:
# - yyyy-MM-dd
# - dd/MM/yyyy
# - formato padrão reconhecido pelo Spark
# Retorna a primeira conversão válida; se nenhuma funcionar, retorna NULL
def parse_date(c):
    return F.coalesce(
        F.to_date(F.col(c), "yyyy-MM-dd"),
        F.to_date(F.col(c), "dd/MM/yyyy"),
        F.to_date(F.col(c))
    )

# ===========================================================
# SELEÇÃO, PADRONIZAÇÃO E PRESERVAÇÃO DOS CAMPOS ORIGINAIS
# ===========================================================
# Nesta etapa:
# - selecionamos as colunas que serão usadas no pipeline
# - aplicamos padronização textual nos campos descritivos
# - preservamos os campos numéricos originais como string (_raw)
#   para permitir conversão segura e auditoria posterior
df = df_raw.select(
    norm_text("sale_id").alias("sale_id"),
    norm_text("data_venda").alias("data_venda_raw"),
    F.col("ano").cast("int").alias("ano"),
    F.col("mes").cast("int").alias("mes"),

    # Padroniza colunas textuais em Title Case para melhorar consistência e legibilidade
    norm_title("loja").alias("loja"),
    norm_title("bairro_entrega").alias("bairro_entrega"),
    norm_title("canal").alias("canal"),
    norm_title("tipo_cliente").alias("tipo_cliente"),

    # Padroniza a categoria em valores canônicos
    norm_categoria("categoria").alias("categoria"),

     # Produto em Title Case e unidade em maiúsculo
    norm_title("produto").alias("produto"),
    norm_upper("unidade").alias("unidade"),

    F.col("quantidade").cast("string").alias("quantidade_raw"),
    F.col("custo_unitario").cast("string").alias("custo_unitario_raw"),
    F.col("preco_venda_unitario").cast("string").alias("preco_venda_unitario_raw"),

    # Padroniza o indicador de promoção para 0 ou 1
    norm_promocao("promocao").alias("promocao"),

    # Mantém as métricas originais como texto para auditoria e comparação com os valores calculados
    F.col("perda_quebra_custo").cast("string").alias("perda_quebra_custo_raw"),
    F.col("receita_total").cast("string").alias("receita_total_raw"),
    F.col("custo_total").cast("string").alias("custo_total_raw"),
    F.col("lucro_total").cast("string").alias("lucro_total_raw"),
    F.col("margem_lucro").cast("string").alias("margem_lucro_raw"),

    # Padroniza colunas textuais complementares
    norm_title("fornecedor").alias("fornecedor"),
    norm_title("forma_pagamento").alias("forma_pagamento"),
)

# ============================================
# CONVERSÃO DE TIPOS E COLUNAS DE AUDITORIA
# ============================================
# Nesta etapa:
# - convertemos os campos raw para os tipos corretos
# - criamos colunas numéricas tipadas para uso nas regras e métricas
# - adicionamos colunas de auditoria da execução
df = (
    df
    .withColumn("data_venda", parse_date("data_venda_raw"))
    .withColumn("quantidade", to_double_ptbr("quantidade_raw"))
    .withColumn("custo_unitario", to_double_ptbr("custo_unitario_raw"))
    .withColumn("preco_venda_unitario", to_double_ptbr("preco_venda_unitario_raw"))
    
    # Valores de entrada convertidos para comparação futura com as métricas recalculadas
    .withColumn("perda_quebra_custo_in", to_double_ptbr("perda_quebra_custo_raw"))
    .withColumn("receita_total_in", to_double_ptbr("receita_total_raw"))
    .withColumn("custo_total_in", to_double_ptbr("custo_total_raw"))
    .withColumn("lucro_total_in", to_double_ptbr("lucro_total_raw"))
    .withColumn("margem_lucro_in", to_double_ptbr("margem_lucro_raw"))
    
    # Auditoria da execução
    .withColumn("processed_at", PROCESSED_AT)
    .withColumn("run_id", F.lit(RUN_ID_VALUE))
)

# ======================================
# REMOÇÃO DE DUPLICIDADES POR SALE_ID
# ======================================
# Nesta etapa:
# - agrupamos os registros pelo identificador da venda (sale_id)
# - definimos uma ordem de prioridade para escolher qual linha manter
# - preservamos apenas a primeira linha de cada grupo duplicado
#
# Observação:
# se o dataset representar "1 linha = 1 item da venda", deduplicar apenas por sale_id
# pode remover itens válidos. Essa lógica só é segura se cada sale_id deveria existir uma única vez na base final.
w = Window.partitionBy("sale_id").orderBy(
    # Prioriza a data de venda mais recente; valores nulos ficam por último
    F.col("data_venda").desc_nulls_last(),

    # Em caso de empate, prioriza o registro processado mais recentemente
    F.col("processed_at").desc(),

    # Como critério adicional de desempate, prioriza a maior receita informada na origem
    F.col("receita_total_in").desc_nulls_last()
)

df = (
    df
    # Numera as linhas dentro de cada grupo de sale_id conforme a ordem definida acima
    .withColumn("rn", F.row_number().over(w))

    # Mantém apenas a primeira linha de cada grupo (registro escolhido como principal)
    .filter(F.col("rn") == 1)

    # Remove a coluna auxiliar usada na deduplicação
    .drop("rn")
)

# ====================================
# RECÁLCULO DAS MÉTRICAS DE NEGÓCIO
# ====================================
# Nesta etapa:
# - recalculamos as métricas principais com base nos campos já tratados
# - definimos essas colunas como a fonte de verdade do pipeline
# - evitamos depender totalmente dos valores recebidos no arquivo de origem
df = (
    df
    # Se perda/quebra vier nula, assume zero; resultado final arredondado em 2 casas
    .withColumn("perda_quebra_custo", F.round(F.coalesce(F.col("perda_quebra_custo_in"), F.lit(0.0)), 2))

    # Receita total = quantidade x preço de venda unitário
    .withColumn("receita_total", F.round(F.col("quantidade") * F.col("preco_venda_unitario"), 2))

    # Custo total = quantidade x custo unitário
    .withColumn("custo_total", F.round(F.col("quantidade") * F.col("custo_unitario"), 2))

    # Lucro total = receita total - custo total - perdas/quebras
    .withColumn("lucro_total", F.round(F.col("receita_total") - F.col("custo_total") - F.col("perda_quebra_custo"), 2))

    # Margem de lucro = lucro total / receita total
    # Se a receita for nula ou zero, retorna NULL para evitar divisão inválida
    .withColumn("margem_lucro",
        F.when(F.col("receita_total").isNull() | (F.col("receita_total") == 0), None)
         .otherwise(F.round(F.col("lucro_total") / F.col("receita_total"), 4))
    )
)

# ===========================================
# REGRAS DE QUALIDADE DOS DADOS (DQ FLAGS)
# ===========================================
# Nesta etapa:
# - criamos flags booleanas para identificar problemas de qualidade nos dados
# - separamos os problemas em dois grupos:
#   1) erros críticos: tornam o registro inválido para uso
#   2) alertas: não invalidam o registro, mas indicam inconsistência ou atenção necessária

df = (
    df
    # Erro crítico: identificador da venda ausente
    .withColumn("flag_sale_id_vazio", F.col("sale_id").isNull())

    # Erro crítico: data da venda ausente ou inválida após conversão
    .withColumn("flag_data_invalida", F.col("data_venda").isNull())

    # Erro crítico: quantidade ausente, zero ou negativa
    .withColumn("flag_quantidade_invalida", F.col("quantidade").isNull() | (F.col("quantidade") <= 0))

    # Erro crítico: custo unitário ausente, zero ou negativo
    .withColumn("flag_custo_invalido", F.col("custo_unitario").isNull() | (F.col("custo_unitario") <= 0))

    # Erro crítico: preço de venda ausente, zero ou negativo
    .withColumn("flag_preco_invalido", F.col("preco_venda_unitario").isNull() | (F.col("preco_venda_unitario") <= 0))
)

df = (
    df
    # Alerta: fornecedor não informado
    .withColumn("flag_fornecedor_vazio", F.col("fornecedor").isNull())

    # Alerta: preço de venda menor ou igual ao custo
    # Pode indicar margem zero, prejuízo ou inconsistência de cadastro
    .withColumn("flag_preco_menor_igual_custo",
        (F.col("preco_venda_unitario").isNotNull()) &
        (F.col("custo_unitario").isNotNull()) &
        (F.col("preco_venda_unitario") <= F.col("custo_unitario"))
    )

    # Alerta: margem fora da faixa esperada
    # Menor que 0 indica prejuízo; maior que 1 indica possível inconsistência
    .withColumn("flag_margem_fora_faixa",
        (F.col("margem_lucro").isNotNull()) &
        ((F.col("margem_lucro") < 0) | (F.col("margem_lucro") > 1))
    )

    # Alerta: receita informada na origem diverge da receita recalculada no pipeline
    .withColumn("flag_receita_diverge",
        F.col("receita_total_in").isNotNull() &
        (F.abs(F.col("receita_total_in") - F.col("receita_total")) > 0.01)
    )

    # Alerta: custo informado na origem diverge do custo recalculado no pipeline
    .withColumn("flag_custo_diverge",
        F.col("custo_total_in").isNotNull() &
        (F.abs(F.col("custo_total_in") - F.col("custo_total")) > 0.01)
    )
)

# ===============================
# MOTIVO PRINCIPAL DE REJEIÇÃO
# ===============================
# Define o primeiro erro crítico encontrado em cada registro.
# Esse campo será usado para classificar e auditar os dados rejeitados.
df = df.withColumn(
    "motivo_rejeicao",
    F.when(F.col("flag_sale_id_vazio"), F.lit("sale_id_vazio"))
     .when(F.col("flag_data_invalida"), F.lit("data_invalida"))
     .when(F.col("flag_quantidade_invalida"), F.lit("quantidade_invalida"))
     .when(F.col("flag_custo_invalido"), F.lit("custo_invalido"))
     .when(F.col("flag_preco_invalido"), F.lit("preco_invalido"))
     .otherwise(F.lit(None))
)

# Registro rejeitado = possui algum motivo de rejeição
is_rejected = F.col("motivo_rejeicao").isNotNull()

# Registro com alerta = não é rejeitado, mas possui ao menos um alerta de qualidade
has_alert = (
    F.col("flag_fornecedor_vazio") |
    F.col("flag_preco_menor_igual_custo") |
    F.col("flag_margem_fora_faixa") |
    F.col("flag_receita_diverge") |
    F.col("flag_custo_diverge")
)

# ====================================
# CLASSIFICAÇÃO FINAL DOS REGISTROS
# ====================================
# rejected: falhou em regra crítica
# flagged : passou nas regras críticas, mas possui alertas
# clean   : sem erros críticos e sem alertas
df_rejected = df.filter(is_rejected)
df_flagged  = df.filter(~is_rejected & has_alert)
df_clean    = df.filter(~is_rejected & ~has_alert)

# =================================
# DEFINIÇÃO DOS SCHEMAS DE SAÍDA
# =================================
# Nesta etapa:
# - definimos quais colunas cada dataset final terá
# - clean: dados limpos e prontos para consumo
# - flagged: dados válidos, mas com alertas de qualidade
# - rejected: dados rejeitados, preservando campos originais e motivo da rejeição
clean_cols = [
    "sale_id","data_venda","ano","mes","loja","bairro_entrega","canal","tipo_cliente","categoria","produto","unidade",
    "quantidade","custo_unitario","preco_venda_unitario","promocao",
    "perda_quebra_custo","receita_total","custo_total","lucro_total","margem_lucro",
    "fornecedor","forma_pagamento","processed_at","run_id"
]

# O dataset flagged mantém todas as colunas do clean e adiciona as flags de alerta
flag_cols = clean_cols + [
    "flag_fornecedor_vazio","flag_preco_menor_igual_custo","flag_margem_fora_faixa",
    "flag_receita_diverge","flag_custo_diverge"
]

# O dataset rejected prioriza os valores originais (_raw) para facilitar auditoria e inclui o motivo da rejeição
rej_cols = [
    "sale_id","data_venda_raw","ano","mes","loja","bairro_entrega","canal","tipo_cliente","categoria","produto","unidade",
    "quantidade_raw","custo_unitario_raw","preco_venda_unitario_raw","margem_lucro_raw",
    "fornecedor","forma_pagamento","motivo_rejeicao","processed_at","run_id"
]

# Aplica os schemas definidos acima em cada conjunto final
df_clean_out    = df_clean.select(*clean_cols)
df_flagged_out  = df_flagged.select(*flag_cols)
df_rejected_out = df_rejected.select(*rej_cols)

# =========================
# RELATÓRIO DE QUALIDADE DOS DADOS (DQ REPORT)
# =========================
# Conta o total de registros processados
total_all = df.count()

# Cria um resumo por status:
# - "ok" para registros não rejeitados
# - motivo_rejeicao para registros rejeitados
# Em seguida calcula quantidade e percentual de cada status
df_dq = (
    df.select(
        F.when(is_rejected, F.col("motivo_rejeicao")).otherwise(F.lit("ok")).alias("status")
    )
    .groupBy("status")
    .agg(F.count("*").alias("qtd"))
    .withColumn("percentual", F.round(F.col("qtd") * 100.0 / F.lit(total_all), 2))
    .orderBy(F.desc("qtd"))
)

# ==================================
# ESCRITA DOS DATASETS EM PARQUET
# ==================================
# clean e flagged são particionados por ano e mês para melhorar performance no Athena
# rejected e dq_report são gravados sem partição por serem usados principalmente para auditoria

(df_clean_out
 .repartition(8, "ano", "mes")
 .write.mode("overwrite")
 .partitionBy("ano", "mes")
 .parquet(OUT_CURATED_CLEAN)
)

(df_flagged_out
 .repartition(8, "ano", "mes")
 .write.mode("overwrite")
 .partitionBy("ano", "mes")
 .parquet(OUT_CURATED_FLAGGED)
)

(df_rejected_out
 .write.mode("overwrite")
 .parquet(OUT_REJECTED_FAIL)
)

(df_dq
 .write.mode("overwrite")
 .parquet(OUT_DQ_REPORT)
)

# ===================
# LOGS DA EXECUÇÃO
# ===================
# Exibe informações úteis para monitoramento e auditoria no log do Glue
print(f"RUN_ID={RUN_ID_VALUE}")
print(f"TOTAL_ALL={total_all}")
print(f"TOTAL_CLEAN={df_clean.count()}")
print(f"TOTAL_FLAGGED={df_flagged.count()}")
print(f"TOTAL_REJECTED={df_rejected.count()}")

# Finaliza oficialmente a execução do job no Glue
job.commit()