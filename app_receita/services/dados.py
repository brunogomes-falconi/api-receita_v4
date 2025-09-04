from __future__ import annotations
from dataclasses import asdict
import pandas as pd
from datetime import date
from typing import Any, Dict, List, Tuple
import traceback

# importa seu pipeline
# se você escolheu outro nome de arquivo, troque "basecode" abaixo
from basecode import Config, run_pipeline  # type: ignore

# --- nomes oficiais de UI que você definiu ---
CARTEIRAS_UI_OFICIAIS = [
    "Agronegócio",
    "América do Norte",
    "Bens Não Duráveis",
    "Infraestrutura e Indústria de Base",
    "MID",
    "Saúde Educação Segurança e Adm.Pública",
    "Servicos e Tecnologia",
]

# --- mapeamentos de carteira para lidar com normalizações internas (se houver) ---
# Ex.: se internamente algo vier como "Falconi EUA", exibimos como "América do Norte".
DEPARA_EXIBICAO = {
    "Falconi EUA": "América do Norte",
    # adicione outros casos reais se houver
}

# Quando receber da UI "América do Norte", podemos ter de mapear para o valor interno do dado:
DEPARA_INTERNO = {v: k for k, v in DEPARA_EXIBICAO.items()}

def _ajustar_carteira_para_interno(valor_ui: str) -> str:
    return DEPARA_INTERNO.get(valor_ui, valor_ui)

def _ajustar_carteira_para_ui(valor_dado: str) -> str:
    return DEPARA_EXIBICAO.get(valor_dado, valor_dado)

def carregar_pipeline(cfg: Config) -> Dict[str, "pd.DataFrame"]:
    """
    Executa seu pipeline e retorna o dicionário de DataFrames.
    Se der erro (ex.: falta driver do Access), propaga a exceção.
    """
    return run_pipeline(cfg)

# --- nomes oficiais de UI que você definiu ---
CARTEIRAS_UI_OFICIAIS = [
    "Agronegócio",
    "América do Norte",
    "Bens Não Duráveis",
    "Infraestrutura e Indústria de Base",
    "MID",
    "Saúde Educação Segurança e Adm.Pública",
    "Servicos e Tecnologia",
]

# de/para entre base e UI
DEPARA_EXIBICAO = {
    "Falconi EUA": "América do Norte",
    # adicione outros casos reais se houver
}
DEPARA_INTERNO = {v: k for k, v in DEPARA_EXIBICAO.items()}

def _ajustar_carteira_para_interno(valor_ui: str) -> str:
    return DEPARA_INTERNO.get(valor_ui, valor_ui)

def _ajustar_carteira_para_ui(valor_dado: str) -> str:
    return DEPARA_EXIBICAO.get(valor_dado, valor_dado)

def listar_carteiras_ui(cfg: "Config") -> list[str]:
    """
    Lê carteiras do pipeline, aplica de/para para UI e
    GARANTE presença dos nomes oficiais definidos por você.
    """
    try:
        dfs = carregar_pipeline(cfg)
    except Exception:
        dfs = {}

    candidatos = set()
    for key in ["Carteira", "Receita_PoC", "Receita_Produto", "Receita_SuccessFee", "Vendas", "tF_Vendas_long"]:
        df = dfs.get(key)
        if df is not None and not df.empty:
            col = "Carteira" if "Carteira" in df.columns else ("Check" if "Check" in df.columns else None)
            if col:
                for v in df[col].dropna().astype(str):
                    candidatos.add(_ajustar_carteira_para_ui(v))

    # mistura com a lista oficial e ordena
    combinada = set(CARTEIRAS_UI_OFICIAIS) | candidatos
    # devolve em ordem: oficiais primeiro, depois extras em alfabética
    extras = sorted([x for x in combinada if x not in CARTEIRAS_UI_OFICIAIS])
    return CARTEIRAS_UI_OFICIAIS + extras

def _aplicar_filtros_basicos(df, mes: str, status: str, carteira: str):
    """
    Aplica filtros de mês/status/carteira nos DataFrames long ou consolidados.
    - mes: '2025-01'..'2025-12' ou 'tudo'
    - status: 'Novo'|'Renovação'|'todos' (coluna esperada: classificacao... na base Vendas/long)
    - carteira: UI label (mapeado p/ interno)
    """
    import pandas as pd

    if df is None or df.empty:
        return df

    out = df.copy()

    # Mês
    if "mes_calendario" in out.columns and mes != "tudo":
        try:
            y, m = mes.split("-")
            ini = pd.Timestamp(int(y), int(m), 1)
            fim = ini + pd.offsets.MonthEnd(0)
            mask = (pd.to_datetime(out["mes_calendario"], errors="coerce") >= ini) & \
                   (pd.to_datetime(out["mes_calendario"], errors="coerce") <= fim)
            out = out[mask]
        except Exception:
            pass

    # Status
    # Em 'Vendas' e no long consolidado, o status costuma ficar em 'classificacaooportunidade__c' ou 'status_frente'
    if status != "todos":
        cand_cols = [c for c in out.columns if "classificacao" in c or "status" in c]
        if cand_cols:
            mask_any = False
            mask = None
            for c in cand_cols:
                m = out[c].astype(str).str.strip().eq(status)
                mask = m if mask is None else (mask | m)
                mask_any = True
            if mask_any:
                out = out[mask]

    # Carteira
    if carteira != "todas":
        carteira_interno = _ajustar_carteira_para_interno(carteira)
        if "Check" in out.columns:
            out = out[out["Check"].astype(str).str.strip().eq(carteira_interno)]

    return out

def calcular_cascata(cfg: Config, mes: str, status: str, carteira: str) -> List[Dict[str, Any]]:
    """
    Retorna lista com labels/valores para o gráfico em cascata,
    somando colunas de interesse do dataframe long (tF_Vendas_long) OU fontes individuais.
    """
    import numpy as np
    import pandas as pd

    dfs = carregar_pipeline(cfg)

    # preferimos o long unificado quando disponível
    src = dfs.get("tF_Vendas_long")
    if src is None or src.empty:
        # fallback: tenta concatenar fontes individuais
        frames = []
        for k in ["Receita_PoC", "Receita_Produto", "Receita_SuccessFee", "Estoque", "Pendente_Alocacao_HD", "Meta_Receita", "Vendas"]:
            if k in dfs:
                frames.append(dfs[k])
        src = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()

    src = _aplicar_filtros_basicos(src, mes=mes, status=status, carteira=carteira)

    # Se estivermos no formato long (id_vars + Atributo/Valor)
    if "Atributo" in src.columns and "Valor" in src.columns:
        pivot = src.pivot_table(index=None, columns="Atributo", values="Valor", aggfunc="sum")
        # pega valores presentes e default 0 quando não houver
        poc         = float(pivot.get("ReceitaPoC", 0) or 0)
        sfee        = float(pivot.get("SuccessFee", 0) or 0)
        prod        = float(pivot.get("ReceitaProduto", 0) or 0)
        pend_form   = float(pivot.get("ReceitaPendenteAlocMes", 0) or 0)
        pend_ass    = float(pivot.get("ReceitaPendenteAssinatura", 0) or 0)  # se existir depois
        potencial   = float(pivot.get("ReceitaPotencialPocMes", 0) or 0)
        gap_meta    = float(pivot.get("DifMeta", 0) or 0)
        total       = float(pivot.get("ReceitaTotal", 0) or (poc + sfee + prod + pend_form + pend_ass + potencial))

    else:
        # fallback simplificado (quando só temos colunas separadas)
        def soma(col):
            return float(pd.to_numeric(src.get(col), errors="coerce").fillna(0).sum()) if col in src.columns else 0.0

        poc       = soma("ReceitaPoC")
        sfee      = soma("SuccessFee")
        prod      = soma("ReceitaProduto")
        pend_form = soma("ReceitaPendenteAlocMes")
        pend_ass  = soma("ReceitaPendenteAssinatura")
        potencial = soma("ReceitaPotencialPocMes")
        meta      = soma("ReceitaMeta")
        total     = poc + sfee + prod + pend_form + pend_ass + potencial
        gap_meta  = meta - total

    return [
        {"label": "Receita PoC", "valor": round(poc, 2)},
        {"label": "Receita Success Fee", "valor": round(sfee, 2)},
        {"label": "Receita Produtos", "valor": round(prod, 2)},
        {"label": "Pendente Formação de Equipe", "valor": round(pend_form, 2)},
        {"label": "Pendente Assinatura", "valor": round(pend_ass, 2)},
        {"label": "Receita Potencial", "valor": round(potencial, 2)},
        {"label": "GAP Meta", "valor": round(gap_meta, 2)},
        {"label": "Total", "valor": round(total, 2)},
    ]

def _filtrar_ano_2025(df):
    import pandas as pd
    if df is None or df.empty:
        return df
    if "mes_calendario" not in df.columns:
        return df
    s = pd.to_datetime(df["mes_calendario"], errors="coerce")
    return df[(s.dt.year == 2025)]

def _pivot_mensal_2025(df, valor_col: str):
    """
    Espera colunas: Check (carteira), nome_cliente, codigo_frente, mes_calendario, <valor_col>
    Gera pivot com colunas Jan..Dez/2025 + Total por linha e Total por coluna.
    """
    import pandas as pd
    if df is None or df.empty:
        # cria pivot vazio com meses 2025
        cols = [pd.Timestamp(2025, m, 1).strftime("%b/2025") for m in range(1, 13)] + ["Total"]
        return pd.DataFrame(columns=["Carteira","Cliente","Frente"] + cols)

    df = df.copy()
    df["mes_cal"] = pd.to_datetime(df["mes_calendario"], errors="coerce")
    df["ym"] = df["mes_cal"].dt.to_period("M")
    df["mes_label"] = df["mes_cal"].dt.strftime("%b/2025")  # ex.: Jan/2025

    # normaliza campos de ID
    df["Carteira"] = df.get("Check", "")
    df["Cliente"]  = df.get("nome_cliente", "")
    df["Frente"]   = df.get("codigo_frente", "")

    # só meses de 2025
    df = df[df["mes_cal"].dt.year == 2025]

    # agrega por linha de ID + mês
    grp = df.groupby(["Carteira","Cliente","Frente","mes_label"], dropna=False)[valor_col].sum().reset_index()

    # pivot
    pivot = grp.pivot_table(index=["Carteira","Cliente","Frente"], columns="mes_label", values=valor_col, aggfunc="sum").fillna(0.0)

    # garante ordem dos meses
    meses = [pd.Timestamp(2025, m, 1).strftime("%b/2025") for m in range(1, 13)]
    for ml in meses:
        if ml not in pivot.columns:
            pivot[ml] = 0.0
    pivot = pivot[meses]

    # total por linha
    pivot["Total"] = pivot.sum(axis=1)

    # reordena index como colunas
    pivot = pivot.reset_index()

    return pivot

def tabela_poc(cfg: "Config", mes: str, status: str, carteira: str):
    """
    Tabela de Receita PoC (linhas: Carteira/Cliente/Frente, colunas: meses 2025).
    """
    import pandas as pd
    try:
        dfs = carregar_pipeline(cfg)
        df = dfs.get("Receita_PoC")
        if df is None:
            return pd.DataFrame()
        df = _aplicar_filtros_basicos(df, mes, status, carteira)
        df = _filtrar_ano_2025(df)
        return _pivot_mensal_2025(df, "ReceitaPoC")  # valor
    except Exception:
        import traceback; traceback.print_exc()
        return pd.DataFrame()

def tabela_success_fee(cfg: "Config", mes: str, status: str, carteira: str):
    import pandas as pd
    try:
        dfs = carregar_pipeline(cfg)
        df = dfs.get("Receita_SuccessFee")
        if df is None:
            return pd.DataFrame()
        df = _aplicar_filtros_basicos(df, mes, status, carteira)
        df = _filtrar_ano_2025(df)
        return _pivot_mensal_2025(df, "SuccessFee")
    except Exception:
        import traceback; traceback.print_exc()
        return pd.DataFrame()

def tabela_produtos(cfg: "Config", mes: str, status: str, carteira: str):
    import pandas as pd
    try:
        dfs = carregar_pipeline(cfg)
        # juntar Razão (histórico) + Carteira (futuro) como no run_pipeline
        df = dfs.get("Receita_Produto")
        car = dfs.get("Carteira_Produto")
        import pandas as pd
        base = pd.concat([df, car], ignore_index=True, sort=False) if df is not None or car is not None else pd.DataFrame()
        if base is None or base.empty:
            return pd.DataFrame()
        base = _aplicar_filtros_basicos(base, mes, status, carteira)
        base = _filtrar_ano_2025(base)
        return _pivot_mensal_2025(base, "ReceitaProduto")
    except Exception:
        import traceback; traceback.print_exc()
        return pd.DataFrame()


def tabela_pendente_formacao(cfg: "Config", mes: str, status: str, carteira: str):
    """
    Tabela de Receita Pendente por Formação de Equipe.
    Entrada esperada: dfs["Pendente_Alocacao_HD"] ou qualquer fonte que já traga a coluna
    "ReceitaPendenteAlocMes" (caso contrário, retorna pivot vazio com a mesma estrutura de meses/Total).
    """
    import pandas as pd
    VALOR_COL = "ReceitaPendenteAlocMes"
    try:
        dfs = carregar_pipeline(cfg)

        # Procura uma fonte que já tenha a coluna "ReceitaPendenteAlocMes" pronta
        candidatos = ["Pendente_Alocacao_HD", "tF_Vendas_long", "Vendas"]
        src = None
        for k in candidatos:
            d = dfs.get(k)
            if d is not None and (VALOR_COL in d.columns):
                src = d
                break

        if src is None or src.empty:
            return _pivot_mensal_2025(pd.DataFrame(), VALOR_COL)

        src = _aplicar_filtros_basicos(src, mes, status, carteira)
        src = _filtrar_ano_2025(src)

        if VALOR_COL not in src.columns:
            return _pivot_mensal_2025(pd.DataFrame(), VALOR_COL)

        return _pivot_mensal_2025(src, VALOR_COL)
    except Exception:
        traceback.print_exc()
        return _pivot_mensal_2025(pd.DataFrame(), VALOR_COL)

def tabela_pendente_assinatura(cfg: "Config", mes: str, status: str, carteira: str):
    """
    Tabela de Receita Pendente de Assinatura.
    Entrada esperada: uma fonte que já contenha a coluna "ReceitaPendenteAssinatura"
    (ex.: dfs["Pendente_Assinatura"] ou consolidado explodido). Fallback: pivot vazio.
    """
    import pandas as pd
    VALOR_COL = "ReceitaPendenteAssinatura"
    try:
        dfs = carregar_pipeline(cfg)

        candidatos = ["Pendente_Assinatura", "tF_Vendas_long", "Vendas"]
        src = None
        for k in candidatos:
            d = dfs.get(k)
            if d is not None and (VALOR_COL in d.columns):
                src = d
                break

        if src is None or src.empty:
            return _pivot_mensal_2025(pd.DataFrame(), VALOR_COL)

        src = _aplicar_filtros_basicos(src, mes, status, carteira)
        src = _filtrar_ano_2025(src)

        if VALOR_COL not in src.columns:
            return _pivot_mensal_2025(pd.DataFrame(), VALOR_COL)

        return _pivot_mensal_2025(src, VALOR_COL)
    except Exception:
        traceback.print_exc()
        return _pivot_mensal_2025(pd.DataFrame(), VALOR_COL)

def tabela_receita_potencial(cfg: "Config", mes: str, status: str, carteira: str):
    """
    Tabela de Receita Potencial (PoC).
    Entrada esperada: uma fonte com a coluna "ReceitaPotencialPocMes" (ex.: dfs["tF_Vendas_long"]
    explodido ou dfs["Receita_Potencial"], se existir). Fallback: pivot vazio.
    """
    import pandas as pd
    VALOR_COL = "ReceitaPotencialPocMes"
    try:
        dfs = carregar_pipeline(cfg)

        candidatos = ["Receita_Potencial", "tF_Vendas_long", "Vendas"]
        src = None
        for k in candidatos:
            d = dfs.get(k)
            if d is not None and (VALOR_COL in d.columns):
                src = d
                break

        if src is None or src.empty:
            return _pivot_mensal_2025(pd.DataFrame(), VALOR_COL)

        src = _aplicar_filtros_basicos(src, mes, status, carteira)
        src = _filtrar_ano_2025(src)

        if VALOR_COL not in src.columns:
            return _pivot_mensal_2025(pd.DataFrame(), VALOR_COL)

        return _pivot_mensal_2025(src, VALOR_COL)
    except Exception:
        traceback.print_exc()
        return _pivot_mensal_2025(pd.DataFrame(), VALOR_COL)
