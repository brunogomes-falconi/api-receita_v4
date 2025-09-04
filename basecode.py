# -*- coding: utf-8 -*-
"""
Pipeline unificado – Power Query (M) → Python (pandas)
Autor: Bruno Gomes + ChatGPT
Descrição:
  Tradução dos ~40 scripts M enviados para um único módulo Python.
  Estrutura focada em:
    - Funções equivalentes por consulta M
    - Conectores parametrizados (Access / CSV / Excel / BigQuery)
    - "run_pipeline(cfg)" para orquestração do fluxo completo

Observações:
  1) Mantive nomes de colunas próximos aos do M para reduzir retrabalho nos visuais.
  2) Onde o M dependia de fontes externas (Access/BigQuery/Excel/CSV), as rotas foram
     parametrizadas no dataclass "Config". Ajuste conforme seu ambiente.
  3) Consultas BigQuery exigem "pandas-gbq" e credenciais (GOOGLE_APPLICATION_CREDENTIALS).
  4) Se alguma fonte auxiliar não estiver disponível, passe o DataFrame correspondente
     diretamente como argumento das funções (todas aceitam sobreposição via parâmetros).
"""

from __future__ import annotations

import typing as t
from dataclasses import dataclass
from datetime import date
import numpy as np
import pandas as pd


# ===================== Config ===================== #

@dataclass
class Config:
    # ---------- Bases Access (.accdb) ----------
    access_db_razao: str = r"C:\Work\Base\Base_Razao.accdb"
    access_db_caixa: str = r"C:\Work\Base\Base_Caixa.accdb"
    access_db_resultado: str = r"C:\Work\Base\BD_Resultado.accdb"
    access_db_roda_razao: str = r"C:\Work\Base\Roda_Base_Razao.accdb"

    # ---------- CSVs/Excels locais ----------
    csv_projeto_risco: str = r"C:\Work\BI Receita\Projeto_risco.csv"
    csv_meta_vendas_td: str = r"C:\Work\BI Receita\Meta_VendasTD.csv"
    csv_meta_receita: str = r"C:\Work\BI Receita\Meta_Receita.csv"
    csv_meta_vendas: str = r"C:\Work\BI Receita\Meta_Vendas.csv"
    csv_mob: str = r"C:\Work\BI Receita\MOB.csv"
    csv_carteira: str = r"C:\Work\BI Receita\Carteira.csv"
    xlsx_percentual_meta: str = r"C:\Work\BI Receita\PercentualMeta.xlsx"
    xlsx_recebimento: str = r"C:\Work\BI Receita\Relatorio Caixa - BR USA PART_2025.xlsx"
    xlsx_depara_un: str = r"C:\Work\BI Receita\DeParaCarteira (Traduzido).xlsx"

    # ---------- (opcionais) CSVs auxiliares ----------
    csv_aux_estoque_meta: str | None = None
    csv_aux_estoque_safra: str | None = None

    # ---------- BigQuery ----------
    bigquery_project_id: t.Optional[str] = None  # ex.: "seu-projeto-gcp"


# ===================== Utils ===================== #

def start_of_current_month() -> pd.Timestamp:
    today = pd.Timestamp.today().normalize()
    return pd.Timestamp(today.year, today.month, 1)


def _read_access_table(db_path: str, table_name: str, where_sql: str | None = None) -> pd.DataFrame:
    """
    Lê tabela do Access usando pyodbc. Requer o driver do Access instalado.
    """
    try:
        import pyodbc  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "pyodbc não está instalado. Instale `pip install pyodbc` e configure o driver do Access."
        ) from e

    conn_str = (
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
        rf"DBQ={db_path};"
    )
    sql = f"SELECT * FROM [{table_name}]"
    if where_sql:
        sql += f" WHERE {where_sql}"
    with pyodbc.connect(conn_str) as con:
        return pd.read_sql(sql, con)


def _read_bigquery_sql(sql: str, project_id: str | None) -> pd.DataFrame:
    if project_id is None:
        raise RuntimeError("Defina Config.bigquery_project_id para usar BigQuery.")
    try:
        import pandas_gbq  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "pandas-gbq não está instalado. Instale `pip install pandas-gbq` e defina GOOGLE_APPLICATION_CREDENTIALS."
        ) from e
    return pd.read_gbq(sql, project_id=project_id, dialect="standard")


def _read_csv(path: str, **kwargs) -> pd.DataFrame:
    return pd.read_csv(path, **kwargs)


def _read_excel(path: str, sheet: str | int = 0, header: int | None = 0) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=sheet, header=header)


def _normalize_carteira(s: pd.Series) -> pd.Series:
    rep = {
        "Saúde, Educação e Serviços Públicos": "Saúde Educação Segurança e Adm.Pública",
        "América do Norte": "Falconi EUA",
        "Varejo e Bens de Consumo": "Bens Não Duráveis",
        "Indústria de Base e Bens de Capital": "Infraestrutura e Indústria de Base",
    }
    return s.replace(rep)


# ===================== tD_* auxiliares (CSV/Excel) ===================== #

def tD_meta_vendas_td(cfg: Config) -> pd.DataFrame:
    df = _read_csv(cfg.csv_meta_vendas_td, sep=";", encoding="utf-8")
    if "Carteira" not in df.columns:
        df.columns = df.iloc[0]
        df = df.iloc[1:].copy()
    long_df = df.melt(id_vars=["Carteira"], var_name="Atributo", value_name="Valor")
    long_df["mes_calendario"] = pd.to_datetime(long_df["Atributo"], errors="coerce", dayfirst=True).dt.date
    long_df["MetaVendas"] = pd.to_numeric(long_df["Valor"], errors="coerce").fillna(0.0)
    long_df = long_df.rename(columns={"Carteira": "Check"})
    return long_df[["Check", "mes_calendario", "MetaVendas"]]


def tD_metas(cfg: Config) -> pd.DataFrame:
    df = _read_csv(cfg.csv_meta_vendas, sep=";", encoding="utf-8").replace("-", "0")
    if "Carteira" not in df.columns:
        df.columns = df.iloc[0]
        df = df.iloc[1:].copy()
    value_cols = [c for c in df.columns if c not in ["Carteira", "Carteira_Cross", "Classificação venda", "classificacaooportunidade__c"]]
    long_df = df.melt(id_vars=["Carteira"], value_vars=value_cols, var_name="Atributo", value_name="Valor")
    long_df["mes_calendario"] = pd.to_datetime(long_df["Atributo"], errors="coerce", dayfirst=True).dt.date
    long_df["Valor"] = pd.to_numeric(long_df["Valor"], errors="coerce").fillna(0.0)
    agg = long_df.groupby(["Carteira", "mes_calendario"], dropna=False)["Valor"].mean().reset_index()
    agg = agg.rename(columns={"Carteira": "Check", "Valor": "MetaVendas"})
    return agg[["Check", "mes_calendario", "MetaVendas"]]


def tD_meta(cfg: Config) -> pd.DataFrame:
    df = _read_csv(cfg.csv_meta_receita, sep=";", encoding="utf-8")
    if "Carteira" not in df.columns:
        df.columns = df.iloc[0]
        df = df.iloc[1:].copy()
    long_df = df.melt(id_vars=["Carteira"], var_name="Atributo", value_name="Valor")
    long_df["mes_calendario"] = pd.to_datetime(long_df["Atributo"], errors="coerce", dayfirst=True).dt.date
    long_df["ReceitaMeta"] = pd.to_numeric(long_df["Valor"], errors="coerce").fillna(0.0) * 1000.0
    long_df = long_df.rename(columns={"Carteira": "Check"})
    return long_df[["Check", "mes_calendario", "ReceitaMeta"]]


def tD_mob(cfg: Config) -> pd.DataFrame:
    raw = _read_csv(cfg.csv_mob, sep=";", encoding="utf-8", header=None)
    # Transpõe e promove cabeçalhos
    tdf = raw.T.reset_index(drop=False)
    tdf.columns = tdf.iloc[0].tolist()
    tdf = tdf.iloc[1:].copy().reset_index(drop=True)
    tdf.insert(0, "MoB", np.arange(1, len(tdf) + 1))
    if "%" not in tdf.columns and "Percent" in tdf.columns:
        tdf["%"] = tdf["Percent"]
    if "% Ac" not in tdf.columns and "Percent Ac" in tdf.columns:
        tdf["% Ac"] = tdf["Percent Ac"]
    for c in ["%", "% Ac"]:
        if c in tdf.columns:
            tdf[c] = pd.to_numeric(tdf[c], errors="coerce")
    tdf["Chave"] = 1
    return tdf[["MoB", "%", "% Ac", "Chave"]]


def tD_mob_add(cfg: Config) -> pd.DataFrame:
    df = _read_csv(cfg.csv_mob, sep=";", encoding="utf-8")
    if "MOB" in df.columns:
        df = df.drop(columns=["MOB"])
    df = df.iloc[1:].copy()
    return df.reset_index(drop=True)


def percentual_meta(cfg: Config) -> pd.DataFrame:
    """
    PercentualMeta.xlsx -> (Carteira, Status, Mes, Percentual)
    Inclui normalização de carteiras e padronização de Status.
    """
    df = _read_excel(cfg.xlsx_percentual_meta, sheet="Planilha1", header=0)
    df.columns = [str(c) for c in df.columns]
    df = df.rename(columns={"Carteira": "Carteira", "Status": "Status"})
    # Normaliza carteiras
    df["Carteira"] = _normalize_carteira(df["Carteira"])
    # Despivotar meses
    id_cols = ["Carteira", "Status"]
    value_cols = [c for c in df.columns if c not in id_cols]
    long_df = df.melt(id_vars=id_cols, value_vars=value_cols, var_name="Mes", value_name="Percentual")
    # Tipos
    long_df["Mes"] = pd.to_datetime(long_df["Mes"], format="%d/%m/%Y", errors="coerce").dt.date
    # Status -> Novo/Renovação
    st = long_df["Status"].astype(str).str.strip().str.upper()
    st = st.replace({"RENOVACAO": "RENOVAÇÃO", "RENOVAÇAO": "RENOVAÇÃO"})
    long_df["Status"] = np.where(st == "NOVO", "Novo", np.where(st == "RENOVAÇÃO", "Renovação", long_df["Status"]))
    return long_df.rename(columns={"Mes": "mes_calendario"})[["Carteira", "Status", "mes_calendario", "Percentual"]]


def tD_carteira(cfg: Config) -> pd.DataFrame:
    df = _read_csv(cfg.csv_carteira, sep=";", encoding="utf-8", header=0)
    if "" in df.columns:
        df = df.drop(columns=[""])
    return df.rename(columns={"Carteira": "Carteira"})[["Carteira"]]


def depara_un(cfg: Config) -> pd.DataFrame:
    df = _read_excel(cfg.xlsx_depara_un, sheet="Plan1", header=0)

    # normaliza para comparar
    norm = {str(c).strip().lower(): c for c in df.columns}

    # tenta achar nomes alternativos
    def _pick(opts: list[str]) -> str | None:
        for o in opts:
            if o in norm:
                return norm[o]
        return None

    col_un_original = _pick(["un_original", "un original", "un_origem", "un origem", "unorig", "un_org"])
    col_un          = _pick(["un", "un destino", "un_destino"])
    col_un_usa      = _pick(["un_usa", "un usa", "unusa"])

    # renomeia o que encontrou
    rename = {}
    if col_un_original: rename[col_un_original] = "UN_Original"
    if col_un:          rename[col_un]          = "UN"
    if col_un_usa:      rename[col_un_usa]      = "UN_USA"

    if rename:
        df = df.rename(columns=rename)

    # mantém apenas as que existirem; se nenhuma existir, retorna vazio (sem quebrar)
    keep = [c for c in ["UN_Original", "UN", "UN_USA"] if c in df.columns]
    if not keep:
        return pd.DataFrame(columns=["UN_Original", "UN", "UN_USA"])

    return df[keep]


def qry_financeiro_recebimento(cfg: Config) -> pd.DataFrame:
    df = _read_excel(cfg.xlsx_recebimento, sheet="qry_Financeiro_Recebimento", header=0)
    # Tipagem e renomes conforme M
    df = df.rename(columns={
        "data_do_recebimento": "mes_calendario",
        "ID_FRENTE_VAL": "codigo_frente",
        "NOME_CARTEIRA": "Check",
        "Valor_BR": "Valor",
        "cliente": "nome_cliente",
        "Fonte": "Atributo"
    })
    # Limpeza
    df = df[df["empresa"].notna()]
    df = df[df["codigo_frente"] != 0]
    # Campos finais
    df["Recebimento"] = pd.to_numeric(df["Valor"], errors="coerce")
    keep = ["mes_calendario","codigo_frente","Recebimento"]
    out = df[keep].copy()
    out["mes_calendario"] = pd.to_datetime(out["mes_calendario"], errors="coerce").dt.date
    return out


# ===================== Loaders Access/BigQuery auxiliares ===================== #

def tbl_opportunity_vendas_completa(cfg: Config) -> pd.DataFrame:
    return _read_access_table(cfg.access_db_resultado, "tbl_OpportunityVendasCompleta")


def tbl_dimensionamento_equipe_vendida(cfg: Config) -> pd.DataFrame:
    df = _read_access_table(cfg.access_db_resultado, "tbl_Dimensionamento_EquipeVendida")
    if "PER_REF" in df.columns:
        df["PER_REF"] = pd.to_datetime(df["PER_REF"], errors="coerce").dt.date
        df = df[df["PER_REF"] >= date(2025,1,1)]
    if "nomestatus_agenda" in df.columns:
        df = df[df["nomestatus_agenda"] == "Equipe vendida atual"]
    return df


def tbl_cotacoes(cfg: Config) -> pd.DataFrame:
    df = _read_access_table(cfg.access_db_roda_razao, "tbl_Cotacoes")
    if "PER_REF" in df.columns:
        df["PER_REF"] = pd.to_datetime(df["PER_REF"], errors="coerce").dt.date
    for c in ["USD","MXN"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.sort_values("PER_REF").reset_index(drop=True)
    if set(["USD","MXN"]).issubset(df.columns):
        df[["USD","MXN"]] = df[["USD","MXN"]].ffill()
    return df[["PER_REF","USD","MXN"]]


def receita_poc_2025_mes(cfg: Config) -> pd.DataFrame:
    sql = """
        SELECT *
        FROM `data-plataform-prd.cfo_contabilidade.receita_poc`
        WHERE mes_calendario BETWEEN DATE '2025-01-01' AND DATE '2025-12-01'
    """
    return _read_bigquery_sql(sql, cfg.bigquery_project_id)


# ===================== **PRIMEIRA LEVA** ===================== #

def tf_receita_poc(cfg: Config) -> pd.DataFrame:
    df = _read_access_table(cfg.access_db_razao, "tbl_BaseRazao_Acumulada")
    if "PER_REF" in df.columns:
        df["PER_REF"] = pd.to_datetime(df["PER_REF"], errors="coerce").dt.date
    df = df[
        (df.get("PER_REF").notna()) &
        (df["PER_REF"] >= date(2025, 1, 1)) &
        (df.get("Class_DRE") == "ROB") &
        (df.get("Class_DRE_2").isin(["Receita POC"]))
    ].copy()
    cm = start_of_current_month().date()
    df = df[df["PER_REF"] <= cm].copy()
    keep = ["Carteira_Atual", "PER_REF", "Valor_Contabil_Ajustado", "Cliente", "Frente"]
    df = df[keep].rename(columns={
        "Carteira_Atual": "Check",
        "PER_REF": "mes_calendario",
        "Valor_Contabil_Ajustado": "ReceitaPoC",
        "Cliente": "nome_cliente",
        "Frente": "codigo_frente",
    })
    df["ReceitaPoC"] = pd.to_numeric(df["ReceitaPoC"], errors="coerce") * -1
    df["codigo_frente"] = df["codigo_frente"].replace({"Editora": "0", "Frente Ajuste Fiscal": "1", "S/INFORMACAO": "2"}).fillna("3")
    df = df[df.get("Check") != "Editora"].copy()
    df["mes_calendario"] = pd.to_datetime(df["mes_calendario"], errors="coerce").dt.date
    return df


def tf_receita_produto(cfg: Config) -> pd.DataFrame:
    df = _read_access_table(cfg.access_db_razao, "tbl_BaseRazao_Acumulada")
    if "PER_REF" in df.columns:
        df["PER_REF"] = pd.to_datetime(df["PER_REF"], errors="coerce").dt.date
    df = df[(df.get("Class_DRE_2") == "Produtos") & (df["PER_REF"] >= date(2025, 1, 1))].copy()
    keep = ["Carteira_Atual", "PER_REF", "Valor_Contabil_Ajustado", "Cliente", "Frente"]
    df = df[keep].rename(columns={"Carteira_Atual": "Check"})
    df["Valor_Contabil_Ajustado"] = pd.to_numeric(df["Valor_Contabil_Ajustado"], errors="coerce") * -1
    df = df.rename(columns={
        "PER_REF": "mes_calendario",
        "Valor_Contabil_Ajustado": "ReceitaProduto",
        "Cliente": "nome_cliente",
        "Frente": "codigo_frente",
    })
    df = df[(df["ReceitaProduto"].notna()) & (df["ReceitaProduto"] != "")].copy()
    df["codigo_frente"] = df["codigo_frente"].replace({"S/INFORMACAO": "0"})
    df["mes_calendario"] = pd.to_datetime(df["mes_calendario"], errors="coerce").dt.date
    return df


def tf_receita_successfee(cfg: Config) -> pd.DataFrame:
    df = _read_access_table(cfg.access_db_razao, "tbl_BaseRazao_Acumulada")
    if "PER_REF" in df.columns:
        df["PER_REF"] = pd.to_datetime(df["PER_REF"], errors="coerce").dt.date
    df = df[(df.get("Class_DRE_2") == "SUCCESS FEE") & (df["PER_REF"] >= date(2025, 1, 1))].copy()
    keep = ["Carteira_Atual", "PER_REF", "Valor_Contabil_Ajustado", "Cliente", "Frente"]
    df = df[keep].rename(columns={"Carteira_Atual": "Check"})
    df["Valor_Contabil_Ajustado"] = pd.to_numeric(df["Valor_Contabil_Ajustado"], errors="coerce") * -1
    df = df.rename(columns={
        "PER_REF": "mes_calendario",
        "Valor_Contabil_Ajustado": "SuccessFee",
        "Cliente": "nome_cliente",
        "Frente": "codigo_frente",
    })
    df = df[(df["SuccessFee"].fillna(0) != 0)].copy()
    df["mes_calendario"] = pd.to_datetime(df["mes_calendario"], errors="coerce").dt.date
    return df


# -- Estoque / Represado (BQ simplificados, prontos para ajustar com seus SQLs) --

_SQL_FONTE_GERAL = """
SELECT *
FROM `data-plataform-prd.cfo_contabilidade.receita_poc`
"""

def tf_represado(cfg: Config) -> pd.DataFrame:
    df = _read_bigquery_sql(_SQL_FONTE_GERAL, cfg.bigquery_project_id)
    # Placeholders principais (estrutura)
    for col in ["valor_represado_acumulado", "valor_recuperado_acumulado"]:
        if col not in df.columns:
            df[col] = 0.0
    df["mes_calendario"] = pd.to_datetime(df.get("mes_calendario"), errors="coerce")
    df = df.sort_values(["codigo_frente", "mes_calendario"])
    grp = df.groupby("codigo_frente", dropna=False)
    df["valor_represado_mensal"] = grp["valor_represado_acumulado"].diff().fillna(df["valor_represado_acumulado"])
    df = df[(df["mes_calendario"] >= pd.Timestamp(2025, 1, 1))].copy()
    keep = ["nome_cliente", "codigo_frente", "mes_calendario", "valor_represado_acumulado", "valor_represado_mensal"]
    out = df[keep].copy()
    out["mes_calendario"] = out["mes_calendario"].dt.date
    return out


def tf_estoque(cfg: Config, tbl_PendenteAlocacao: pd.DataFrame | None = None) -> pd.DataFrame:
    df = _read_bigquery_sql(_SQL_FONTE_GERAL, cfg.bigquery_project_id)
    df["mes_calendario"] = pd.to_datetime(df.get("mes_calendario"), errors="coerce")
    df = df.sort_values(["codigo_frente", "mes_calendario"])
    grp_cols = ["Check", "nome_cliente", "codigo_frente", "status_frente", "mes_calendario"]
    for c in grp_cols:
        if c not in df.columns:
            df[c] = np.nan
    agg = df.groupby(grp_cols, dropna=False).agg(
        ReceitaRepresadaAc=("valor_represado_acumulado", "sum"),
        ReceitaRecuperadaAc=("valor_recuperado_acumulado", "sum"),
    ).reset_index()
    out_frames = []
    for cod, sub in agg.groupby("codigo_frente", dropna=False):
        sub = sub.sort_values("mes_calendario").copy()
        sub["Estoque.ReceitaRepresadaFinalSaldo"] = sub["ReceitaRepresadaAc"] - sub["ReceitaRecuperadaAc"]
        sub["Estoque.ReceitaRepresadaFinal"] = sub["Estoque.ReceitaRepresadaFinalSaldo"].diff()
        if not sub.empty:
            sub.loc[sub.index[0], "Estoque.ReceitaRepresadaFinal"] = sub.loc[sub.index[0], "Estoque.ReceitaRepresadaFinalSaldo"]
        out_frames.append(sub)
    estoque = pd.concat(out_frames, ignore_index=True) if out_frames else agg
    if tbl_PendenteAlocacao is not None and not tbl_PendenteAlocacao.empty:
        estoque = pd.concat([estoque, tbl_PendenteAlocacao], ignore_index=True, sort=False)
    estoque["mes_calendario"] = pd.to_datetime(estoque["mes_calendario"], errors="coerce").dt.date
    for c in ["Estoque.ReceitaRepresadaFinalSaldo", "Estoque.ReceitaRepresadaFinal"]:
        if c in estoque.columns:
            estoque[c] = pd.to_numeric(estoque[c], errors="coerce").fillna(0.0)
    return estoque


def tf_frente_equipe_formada(cfg: Config) -> pd.DataFrame:
    df = _read_bigquery_sql(_SQL_FONTE_GERAL, cfg.bigquery_project_id)
    return df[["codigo_frente"]].drop_duplicates().copy()


def tf_projeto_risco(cfg: Config) -> pd.DataFrame:
    df = _read_csv(cfg.csv_projeto_risco, sep=";", encoding="utf-8")
    if df.columns.tolist() == ["Column1","Column2"]:
        df.columns = ["Risco","drop"]
        df = df.drop(columns=["drop"])
    if "Risco" not in df.columns:
        df.columns = df.iloc[0].tolist()
        df = df.iloc[1:].copy()
    df["Risco"] = df["Risco"].astype(str)
    df["status_frente"] = "Projeto em Risco"
    return df[["Risco","status_frente"]].copy()


def tf_receita_cancelada(cfg: Config) -> pd.DataFrame:
    # Placeholder: ajuste com sua consulta real (BigQuery/Access) se necessário
    return pd.DataFrame(columns=["Check","mes_calendario","codigo_frente","nome_cliente","status_frente","ReceitaCancelada"])


# ===================== **SEGUNDA LEVA** ===================== #

def tf_carteira_produto(cfg: Config) -> pd.DataFrame:
    df = _read_access_table(cfg.access_db_caixa, "tbl_Carteira_Completa")
    df = df[df.get("Tipo_Item") == "Licenciamento de Sistemas"].copy()
    df["PER_REF"] = pd.to_datetime(df["PER_REF"], errors="coerce").dt.date
    df = df[df["PER_REF"] >= date(2025, 1, 1)].copy()
    df = df.rename(columns={
        "NOME_CARTEIRA": "Check",
        "Valor": "ReceitaProduto",
        "cliente": "nome_cliente",
        "PER_REF": "mes_calendario",
        "ID_FRENTE": "codigo_frente",
    })
    cm = start_of_current_month().date()
    df = df[df["mes_calendario"] >= cm].copy()
    df["nome_cliente"] = df["nome_cliente"].replace({"L4B LOGISTICA LTDA": "LOGGI"})
    df = df[(df["ReceitaProduto"].notna()) & (df["ReceitaProduto"] != "")].copy()
    return df[["Check", "mes_calendario", "codigo_frente", "nome_cliente", "ReceitaProduto"]]


def tf_carteira_successfee(cfg: Config) -> pd.DataFrame:
    df = _read_access_table(cfg.access_db_caixa, "tbl_Carteira_Completa")
    df = df[df.get("Tipo_Item") == "Success Fee"].copy()
    df["PER_REF"] = pd.to_datetime(df["PER_REF"], errors="coerce").dt.date
    df = df[df["PER_REF"] >= date(2025, 1, 1)].copy()
    df = df.rename(columns={
        "NOME_CARTEIRA": "Check",
        "Valor": "SuccessFee",
        "cliente": "nome_cliente",
        "PER_REF": "mes_calendario",
        "ID_FRENTE": "codigo_frente",
    })
    cm = start_of_current_month().date()
    df = df[df["mes_calendario"] >= cm].copy()
    df["nome_cliente"] = df["nome_cliente"].replace({"L4B LOGISTICA LTDA": "LOGGI"})
    return df[["Check", "mes_calendario", "codigo_frente", "nome_cliente", "SuccessFee"]]


def tbl_pendente_alocacao(cfg: Config) -> pd.DataFrame:
    df = _read_access_table(cfg.access_db_resultado, "tbl_OpportunityVendasCompleta").copy()
    mask = (
        df["Empresa"].isin(["Falconi", "Falconi EUA"]) &
        df["Status"].isin(["Oficializado", "Vendido"]) &
        (~df["StatusConsultoria"].isin(["Cancelado","Interrompido","Não encontrado","Substituído"])) &
        (df["Classificacaofrente"] != "Produto") &
        (pd.to_numeric(df["Numero de HDs"], errors="coerce").fillna(0) > 0)
    )
    df = df[mask].copy()
    df["Safra"] = pd.to_datetime(df["Safra"], errors="coerce").dt.date
    agg = (
        df.groupby(["CarteiraAtual","Safra","Frente","name_frente"], dropna=False)["Valor_Frente"]
          .sum()
          .reset_index()
          .rename(columns={
              "CarteiraAtual":"Check","Safra":"mes_calendario","Frente":"codigo_frente","name_frente":"nome_cliente",
              "Valor_Frente":"ReceitaPendenteAlocacao"
          })
    )
    return agg


def tbl_pendente_alocacao_hd_v2(
    cfg: Config,
    aux_pendente_frentes: pd.DataFrame,
    aux_pendente_razao: pd.DataFrame,
    dim_equipes: pd.DataFrame
) -> pd.DataFrame:
    df = _read_access_table(cfg.access_db_resultado, "tbl_OpportunityVendasCompleta")
    cols = ["Frente","Numero de HDs","StatusConsultoria","Classificacaofrente","Valor_Frente","Cliente","CarteiraAtual"]
    df = df[cols].copy()
    df.rename(columns={"Numero de HDs":"Numero_HD"}, inplace=True)
    df["Numero_HD"] = pd.to_numeric(df["Numero_HD"], errors="coerce").fillna(0).astype(int)
    df["Frente"] = pd.to_numeric(df["Frente"], errors="coerce").astype("Int64")

    problem = {"Cancelado","Interrompido","Não encontrado","Substituído"}
    opp = df[(df["Numero_HD"]>0) & (df["Classificacaofrente"]!="Produto") & (~df["StatusConsultoria"].isin(problem))].copy()

    poc = aux_pendente_frentes.rename(columns={"codigo_frente":"Frente"})[["Frente"]].drop_duplicates()
    razao = aux_pendente_razao.rename(columns={"Frente":"Frente"})[["Frente"]].drop_duplicates()
    opp = opp.merge(poc.assign(in_poc=1), on="Frente", how="left")
    opp = opp.merge(razao.assign(in_razao=1), on="Frente", how="left")
    opp = opp[(opp["in_poc"].isna()) & (opp["in_razao"].isna()) & (opp["StatusConsultoria"]=="A iniciar")].copy()

    rec = (opp.groupby(["CarteiraAtual","Frente","Cliente"], dropna=False)["Valor_Frente"]
              .sum()
              .reset_index()
              .rename(columns={"CarteiraAtual":"Check","Frente":"codigo_frente","Cliente":"nome_cliente","Valor_Frente":"ReceitaPendAloc"}))

    dim = dim_equipes.copy()
    dim["codigofrente"] = pd.to_numeric(dim["codigofrente"], errors="coerce").astype("Int64")
    dim["PER_REF"] = pd.to_datetime(dim["PER_REF"], errors="coerce")
    dim["QTD_HD"] = pd.to_numeric(dim["QTD_HD"], errors="coerce").fillna(0).astype(int)

    cur = start_of_current_month()
    dim["PER_REF"] = dim["PER_REF"].fillna(cur)

    jd = rec.merge(dim, left_on="codigo_frente", right_on="codigofrente", how="left")
    tot = jd.groupby("codigo_frente", dropna=False)["QTD_HD"].sum().rename("TotalHD").reset_index()
    jd = jd.merge(tot, on="codigo_frente", how="left")

    mmin = jd.groupby("codigo_frente", dropna=False)["PER_REF"].min().rename("MinPER").reset_index()
    jd = jd.merge(mmin, on="codigo_frente", how="left")
    jd["MesesDeslocar"] = np.where(jd["MinPER"] < cur, (cur.year-jd["MinPER"].dt.year)*12 + (cur.month-jd["MinPER"].dt.month), 0)

    def _calc_row(row):
        if pd.isna(row["TotalHD"]) or row["TotalHD"] == 0:
            return pd.Series({"mes_calendario": pd.NaT, "ReceitaPendenteAlocMes": np.nan})
        valor = row["ReceitaPendAloc"] * (row["QTD_HD"] / row["TotalHD"])
        mes = (row["PER_REF"] + pd.DateOffset(months=int(row["MesesDeslocar"]))).normalize()
        return pd.Series({"mes_calendario": mes, "ReceitaPendenteAlocMes": valor})

    out = jd.join(jd.apply(_calc_row, axis=1))
    out = out[out["mes_calendario"].notna()].copy()
    out = out[out["mes_calendario"] >= cur].copy()
    out["mes_calendario"] = out["mes_calendario"].dt.date
    return out[["Check","mes_calendario","codigo_frente","nome_cliente","ReceitaPendenteAlocMes"]]


def tbl_vendas(cfg: Config) -> pd.DataFrame:
    import pandas as pd

    # Se você estiver rodando temporariamente sem Access (use_access=False), devolve vazio:
    if not getattr(cfg, "use_access", True):
        return pd.DataFrame()

    try:
        df = _read_access_table(cfg.access_db_resultado, "tbl_OpportunityVendasCompleta").copy()
    except Exception:
        # Sem driver/sem tabela: não quebra o pipeline
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    # Normalizações seguras
    # Datas:
    for col in ["Safra", "Data_Entrada_Oport"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Numéricos:
    if "Valor_Frente" in df.columns:
        df["Valor_Frente"] = pd.to_numeric(df["Valor_Frente"], errors="coerce")

    # Classificação (Novo/Renovação)
    def norm_class(c):
        if pd.isna(c) or str(c).strip() == "":
            return None
        return "Novo" if str(c).strip() == "Novo" else "Renovação"

    if "classificacaooportunidade__c" in df.columns:
        df["class_norm"] = df["classificacaooportunidade__c"].apply(norm_class)
    else:
        df["class_norm"] = None

    # codigo_frente pode estar com outro nome; fallback para "Frente"
    if "codigo_frente" in df.columns:
        cod_raw = pd.to_numeric(df["codigo_frente"], errors="coerce")
    elif "Frente" in df.columns:
        cod_raw = pd.to_numeric(df["Frente"], errors="coerce")
    else:
        cod_raw = pd.Series(dtype="float64")  # vazio

    df["codigo_frente_txt"] = cod_raw.astype("Int64").astype(str) if not cod_raw.empty else ""

    # Mapear classificação por frente
    mapa_frente = (
        df.groupby("codigo_frente_txt")["class_norm"]
          .apply(lambda s: [x for x in s.dropna().tolist() if x != ""])
          .reset_index(name="ListaClass")
    )

    def class_por_frente(lc):
        if "Renovação" in lc:
            return "Renovação"
        if "Novo" in lc:
            return "Novo"
        return None

    mapa_frente["class_por_frente"] = mapa_frente["ListaClass"].apply(class_por_frente)
    mapa_frente = mapa_frente.drop(columns=["ListaClass"])

    # Prefixo para fallback adicional
    df["prefixo5"] = df["codigo_frente_txt"].astype(str).str[:5]

    mapa_prefixo = (
        mapa_frente.assign(prefixo5=mapa_frente["codigo_frente_txt"].astype(str).str[:5])
        .groupby("prefixo5")["class_por_frente"]
        .apply(lambda s: [x for x in s.dropna().tolist()])
        .reset_index(name="Classes")
    )

    def class_por_prefixo(cls):
        if "Renovação" in cls:
            return "Renovação"
        return "Novo" if any(x == "Novo" for x in cls) else "Novo"

    mapa_prefixo["class_por_prefixo"] = mapa_prefixo["Classes"].apply(class_por_prefixo)
    mapa_prefixo = mapa_prefixo.drop(columns=["Classes"])

    df = df.merge(mapa_frente, on="codigo_frente_txt", how="left") \
           .merge(mapa_prefixo, on="prefixo5", how="left")

    def class_final(row):
        c1, c2, c3 = row.get("class_norm"), row.get("class_por_frente"), row.get("class_por_prefixo")
        if c1 not in [None, ""]:
            return c1
        if c2 not in [None, ""]:
            return c2
        if c3 not in [None, ""]:
            return c3
        return "Novo"

    df["classificacao_final"] = df.apply(class_final, axis=1)

    # Filtros mínimos esperados
    if "Status" in df.columns:
        df = df[df["Status"].isin(["Oficializado", "Vendido"])].copy()
    if "Safra" in df.columns:
        df = df[df["Safra"] >= pd.Timestamp(2025, 1, 1)]

    # Colunas de saída robustas (só se existirem)
    out = pd.DataFrame()
    out["mes_calendario"] = df["Safra"] if "Safra" in df.columns else pd.NaT
    out["Check"] = df["CarteiraAtual"] if "CarteiraAtual" in df.columns else None
    out["status_frente"] = df["Status"] if "Status" in df.columns else None
    out["classificacaooportunidade__c"] = df["classificacao_final"]
    out["SomaVendas"] = df["Valor_Frente"] if "Valor_Frente" in df.columns else 0

    # Tipos finais
    out["mes_calendario"] = pd.to_datetime(out["mes_calendario"], errors="coerce").dt.date

    return out


def tD_indice() -> pd.DataFrame:
    names_pt = [
        "Receita PoC", "Receita Success Fee", "Receita Produtos",
        "Pendente Formação de Equipe", "Pendente Assinatura", "Receita Potencial",
        "GAP Vendas", "Estouro HD", "GAP Meta", "Vendas", "Meta"
    ]
    names_us = [
        "Element One US", "Element Two US", "Element Three US",
        "Element Four US", "Element Five US", "Element Six US",
        "Element Seven US", "Element Eight US", "Element Nine US",
        "", ""
    ]
    idx = list(range(1, len(names_pt) + 1))
    return pd.DataFrame({"Índice": idx, "Nome_PTBR": names_pt, "Nome_USD": names_us})


# ===================== **QUINTA LEVA – Acessórios** ===================== #

def aux_pendentealocacao_frentes(cfg: Config) -> pd.DataFrame:
    """
    Loader genérico – se existir tabela homônima no Access Resultado, utiliza.
    Caso não exista, devolve apenas as frentes ativas via tf_frente_equipe_formada (fallback).
    """
    try:
        return _read_access_table(cfg.access_db_resultado, "Aux_PendenteAlocacao_Frentes")
    except Exception:
        # Fallback: retorna frentes de BQ
        return tf_frente_equipe_formada(cfg).rename(columns={"codigo_frente":"Frente"})


def aux_pendentealocacao_frentes_hd(cfg: Config) -> pd.DataFrame:
    try:
        return _read_access_table(cfg.access_db_resultado, "Aux_PendenteAlocacao_Frentes_HD")
    except Exception:
        return aux_pendentealocacao_frentes(cfg)


def aux_pendentealocacao_razao(cfg: Config) -> pd.DataFrame:
    try:
        return _read_access_table(cfg.access_db_resultado, "Aux_PendenteAlocacao_Razao")
    except Exception:
        # Placeholder: retorna apenas Frente distinta da Opportunity
        df = tbl_opportunity_vendas_completa(cfg)
        return df[["Frente"]].drop_duplicates()


def aux_pendentealocacao_razao_hd(cfg: Config) -> pd.DataFrame:
    try:
        return _read_access_table(cfg.access_db_resultado, "Aux_PendenteAlocacao_Razao_HD")
    except Exception:
        return aux_pendentealocacao_razao(cfg)


def aux_estoque_meta(cfg: Config) -> pd.DataFrame | None:
    if cfg.csv_aux_estoque_meta:
        return _read_csv(cfg.csv_aux_estoque_meta, sep=";", encoding="utf-8")
    return None


def aux_estoque_safra(cfg: Config) -> pd.DataFrame | None:
    if cfg.csv_aux_estoque_safra:
        return _read_csv(cfg.csv_aux_estoque_safra, sep=";", encoding="utf-8")
    return None


# ===================== Núcleo – tF_Vendas (unificador) ===================== #

def tf_vendas(
    cfg: Config,
    # insumos opcionais (permite sobrepor dados em tests)
    tD_Meta: pd.DataFrame | None = None,
    tbl_PendenteAlocacao_HD: pd.DataFrame | None = None,
    tF_GapVendas_df: pd.DataFrame | None = None,
    tbl_PotencialReceita_Fim: pd.DataFrame | None = None,
    tF_Represado_df: pd.DataFrame | None = None,
    tF_Estoque_df: pd.DataFrame | None = None,
    tbl_Vendas_df: pd.DataFrame | None = None,
    tD_MetaVendas_df: pd.DataFrame | None = None,
    qry_Financeiro_Recebimento_df: pd.DataFrame | None = None,
    tF_CarteiraSuccessFee_df: pd.DataFrame | None = None,
    tF_ReceitaCancelada_df: pd.DataFrame | None = None,
    tF_ReceitaSuccessFee_df: pd.DataFrame | None = None,
    tF_ReceitaProduto_df: pd.DataFrame | None = None,
    tF_ReceitaPoC_df: pd.DataFrame | None = None,
    tbl_Cotacoes_df: pd.DataFrame | None = None,
    DeParaUN_df: pd.DataFrame | None = None,
    tF_Projeto_risco_df: pd.DataFrame | None = None,
) -> pd.DataFrame:

    pieces: list[pd.DataFrame] = []

    def _append(df: pd.DataFrame | None):
        if df is not None and not df.empty:
            pieces.append(df)

    _append(tD_Meta)
    _append(tbl_PendenteAlocacao_HD)
    _append(tF_GapVendas_df)
    _append(tbl_PotencialReceita_Fim)
    _append(tF_Represado_df)
    _append(tF_Estoque_df)
    _append(tbl_Vendas_df)
    _append(tD_MetaVendas_df)
    _append(qry_Financeiro_Recebimento_df)
    _append(tF_CarteiraSuccessFee_df)
    _append(tF_ReceitaCancelada_df)
    _append(tF_ReceitaSuccessFee_df)
    _append(tF_ReceitaProduto_df)
    _append(tF_ReceitaPoC_df)
    # opcionalmente: cotacoes, depara, risco (quando contribuirem com colunas numéricas no combine)

    if not pieces:
        return pd.DataFrame()

    combined = pd.concat(pieces, ignore_index=True, sort=False)

    # Defaults numéricos para colunas-chave
    for c in [
        "ReceitaPoC","ReceitaEstouro","ReceitaMeta","ReceitaPendenteAlocMes","ReceitaGapVendas",
        "ReceitaPotencialPocMes","valor_recuperado_acumulado","valor_represado_acumulado",
        "Estoque.ReceitaRepresadaFinal","Estoque.ReceitaPendenteAlocacao","SomaVendas","MetaVendas",
        "SuccessFee","ReceitaCancelada","ReceitaProduto","Recebimento"
    ]:
        if c in combined.columns:
            combined[c] = pd.to_numeric(combined[c], errors="coerce").fillna(0.0)

    combined["ReceitaTotal"] = combined.get("ReceitaPoC", 0) + combined.get("ReceitaPendenteAlocMes", 0) + \
                               combined.get("Estoque.ReceitaRepresadaFinal", 0) + combined.get("ReceitaPotencialPocMes", 0) + \
                               combined.get("SuccessFee", 0) + combined.get("ReceitaProduto", 0)
    combined["DifMeta"] = combined.get("ReceitaMeta", 0) - combined["ReceitaTotal"]

    id_cols = [c for c in ["Check", "mes_calendario", "codigo_frente", "nome_cliente", "status_frente"] if c in combined.columns]
    value_cols = [c for c in combined.columns if c not in id_cols]
    long_df = combined.melt(id_vars=id_cols, value_vars=value_cols, var_name="Atributo", value_name="Valor")

    if "mes_calendario" in long_df.columns:
        long_df["mes_calendario"] = pd.to_datetime(long_df["mes_calendario"], errors="coerce").dt.date
    if "codigo_frente" in long_df.columns:
        tmp = pd.to_numeric(long_df["codigo_frente"], errors="coerce")
        long_df["codigo_frente"] = tmp.astype("Int64")

    return long_df


# ===================== Orquestração ===================== #

def run_pipeline(cfg: Config) -> dict[str, pd.DataFrame]:
    """
    Executa as etapas principais e retorna um dicionário com os dataframes finais/intermediários.
    Ajuste "cfg" conforme seus caminhos/credenciais.
    """
    # 1) Bases de metas e vendas
    df_meta_rec   = tD_meta(cfg)              # ReceitaMeta
    df_meta_vdt   = tD_meta_vendas_td(cfg)    # MetaVendas (TD)
    df_metas_alt  = tD_metas(cfg)             # MetaVendas (média) – opcional
    df_mob        = tD_mob(cfg)

    # 2) Carteiras / DePara / Percentuais
    df_carteira   = tD_carteira(cfg)
    df_depara     = depara_un(cfg)
    df_pct_meta   = percentual_meta(cfg)

    # 3) Vendas e pendências
    df_vendas     = tbl_vendas(cfg)
    df_dim_eq     = tbl_dimensionamento_equipe_vendida(cfg)
    frentes_poc   = tf_frente_equipe_formada(cfg)
    aux_razao     = aux_pendentealocacao_razao(cfg)
    df_pend_hd    = tbl_pendente_alocacao_hd_v2(cfg, aux_pendente_frentes=frentes_poc, aux_pendente_razao=aux_razao, dim_equipes=df_dim_eq)

    # 4) Carteira (caixa) – Success Fee / Produto futuros
    df_sf_car     = tf_carteira_successfee(cfg)
    df_prod_car   = tf_carteira_produto(cfg)

    # 5) Razão – PoC/Produto/SucessFee histórico
    df_poc        = tf_receita_poc(cfg)
    df_prod       = tf_receita_produto(cfg)
    df_sfee       = tf_receita_successfee(cfg)

    # 6) Estoque e potencial
    df_estoque    = tf_estoque(cfg)
    # (Opcional) potencial por curva e fim – requer BQ cheio
    # pot_curva     = tbl_potencial_receita(cfg, tF_Estoque_df=df_estoque, tD_MoB_df=df_mob)
    # pot_fim       = tbl_potencial_receita_fim(cfg, tF_Estoque_df=df_estoque, dim_equipes=df_dim_eq)

    # 7) Recebimento (caixa)
    df_receb      = qry_financeiro_recebimento(cfg)

    # 8) Unificação estilo tF_Vendas (long)
    # junta produto de Razão (histórico) + Carteira (pipeline de caixa/futuro)
    prod_unificado = pd.concat([df_prod, df_prod_car], ignore_index=True, sort=False)

    long = tf_vendas(
        cfg,
        tD_Meta=df_meta_rec,
        tbl_PendenteAlocacao_HD=df_pend_hd,
        tF_ReceitaSuccessFee_df=df_sfee,
        tF_ReceitaProduto_df=prod_unificado,
        tbl_Vendas_df=df_vendas,
        tD_MetaVendas_df=df_meta_vdt,
        tF_ReceitaPoC_df=df_poc,
        tF_CarteiraSuccessFee_df=df_sf_car,
        qry_Financeiro_Recebimento_df=df_receb,
        tF_Estoque_df=df_estoque,
        # tF_PotencialReceita_df=pot_curva,
        # tbl_PotencialReceita_Fim=pot_fim,
    )

    return {
        "tF_Vendas_long": long,
        "Meta_Receita": df_meta_rec,
        "Meta_Vendas_TD": df_meta_vdt,
        "Vendas": df_vendas,
        "Pendente_Alocacao_HD": df_pend_hd,
        "Receita_PoC": df_poc,
        "Receita_Produto": df_prod,
        "Receita_SuccessFee": df_sfee,
        "Estoque": df_estoque,
        "Recebimento": df_receb,
        "Carteira_SF": df_sf_car,
        "Carteira_Produto": df_prod_car,
        "Carteira": df_carteira,
        "DeParaUN": df_depara,
        "PercentualMeta": df_pct_meta,
    }

# ===================== Execução direta (opcional) ===================== #

if __name__ == "__main__":
    # Exemplo de como rodar localmente (ajuste os caminhos do Config antes):
    cfg = Config(
        access_db_razao=r"C:\Work\Base\Base_Razao.accdb",
        access_db_caixa=r"C:\Work\Base\Base_Caixa.accdb",
        access_db_resultado=r"C:\Work\Base\BD_Resultado.accdb",
        access_db_roda_razao=r"C:\Work\Base\Roda_Base_Razao.accdb",
        csv_projeto_risco=r"C:\Work\BI Receita\Projeto_risco.csv",
        csv_meta_vendas_td=r"C:\Work\BI Receita\Meta_VendasTD.csv",
        csv_meta_receita=r"C:\Work\BI Receita\Meta_Receita.csv",
        csv_meta_vendas=r"C:\Work\BI Receita\Meta_Vendas.csv",
        csv_mob=r"C:\Work\BI Receita\MOB.csv",
        csv_carteira=r"C:\Work\BI Receita\Carteira.csv",
        xlsx_percentual_meta=r"C:\Work\BI Receita\PercentualMeta.xlsx",
        xlsx_recebimento=r"C:\Work\BI Receita\Relatorio Caixa - BR USA PART_2025.xlsx",
        xlsx_depara_un=r"C:\Work\BI Receita\DeParaCarteira (Traduzido).xlsx",
        bigquery_project_id="SEU-PROJETO-GCP",
    )
    dfs = run_pipeline(cfg)
    for name, df in dfs.items():
        print(name, df.shape)