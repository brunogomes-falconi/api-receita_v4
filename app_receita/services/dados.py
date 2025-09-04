from __future__ import annotations
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, List, Tuple
from datetime import date
import hashlib, json, traceback

import pandas as pd

# pipeline
from basecode import Config, run_pipeline  # type: ignore

# cache
from django.core.cache import cache
from django.conf import settings

# =========================
#  Config de cache (TTL)
# =========================
CACHE_TTL = int(getattr(settings, "CACHE_TTL_SECONDS", 300))
_CACHE_VERSION = "v1"  # bump se mudar estrutura de chaves

# ---------- helpers de cache ----------
def _cfg_key(cfg: Config) -> str:
    """Gera chave estável a partir de Config (dataclass ou objeto comum)."""
    try:
        if is_dataclass(cfg):
            base = asdict(cfg)
        else:
            base = {k: v for k, v in vars(cfg).items() if not k.startswith("_")}
    except Exception:
        base = repr(cfg)
    payload = json.dumps(base, default=str, sort_keys=True)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()

def _mk_key(name: str, **kwargs) -> str:
    payload = json.dumps(kwargs, default=str, sort_keys=True)
    h = hashlib.md5(payload.encode("utf-8")).hexdigest()
    return f"{_CACHE_VERSION}:{name}:{h}"

def _cached_call(key: str, builder, ttl: int | None = None, nocache: bool = False):
    """Pega do cache ou executa e salva; respeita nocache."""
    if nocache:
        return builder()
    obj = cache.get(key)
    if obj is not None:
        return obj
    obj = builder()
    cache.set(key, obj, timeout=ttl if ttl is not None else CACHE_TTL)
    return obj

# ---------------- nomes oficiais UI / de-para ----------------
CARTEIRAS_UI_OFICIAIS = [
    "Agronegócio",
    "América do Norte",
    "Bens Não Duráveis",
    "Infraestrutura e Indústria de Base",
    "MID",
    "Saúde Educação Segurança e Adm.Pública",
    "Servicos e Tecnologia",
]

DEPARA_EXIBICAO = {
    "Falconi EUA": "América do Norte",
}
DEPARA_INTERNO = {v: k for k, v in DEPARA_EXIBICAO.items()}

def _ajustar_carteira_para_interno(valor_ui: str) -> str:
    return DEPARA_INTERNO.get(valor_ui, valor_ui)

def _ajustar_carteira_para_ui(valor_dado: str) -> str:
    return DEPARA_EXIBICAO.get(valor_dado, valor_dado)

# =========================
#  Pipeline (CACHEADO)
# =========================
def carregar_pipeline(cfg: Config, nocache: bool = False) -> Dict[str, pd.DataFrame]:
    """
    Executa seu pipeline e retorna o dicionário de DataFrames.
    Agora cacheado em memória (LocMem) por TTL.
    'nocache=True' força bypass (útil p/ debug export=atualizado).
    """
    key = _mk_key("pipeline", cfg=_cfg_key(cfg))
    return _cached_call(key, lambda: run_pipeline(cfg), ttl=CACHE_TTL, nocache=nocache)

# =========================
#  UI: carteiras (CACHE)
# =========================
def listar_carteiras_ui(cfg: Config, nocache: bool = False) -> list[str]:
    key = _mk_key("listar_carteiras_ui", cfg=_cfg_key(cfg))
    def _build():
        try:
            dfs = carregar_pipeline(cfg, nocache=nocache)
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
        combinada = set(CARTEIRAS_UI_OFICIAIS) | candidatos
        extras = sorted([x for x in combinada if x not in CARTEIRAS_UI_OFICIAIS])
        return CARTEIRAS_UI_OFICIAIS + extras
    return _cached_call(key, _build, ttl=CACHE_TTL, nocache=nocache)

# =========================
#  Filtros básicos
# =========================
def _aplicar_filtros_basicos(df, mes: str, status: str, carteira: str):
    """
    Aplica filtros de mês/status/carteira nos DataFrames long ou consolidados.
    - mes: '2025-01'..'2025-12' ou 'tudo'
    - status: 'Novo'|'Renovação'|'todos' (coluna com 'classificacao' ou 'status')
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
    if status != "todos":
        cand_cols = [c for c in out.columns if "classificacao" in c or "status" in c]
        if cand_cols:
            mask = None
            for c in cand_cols:
                m = out[c].astype(str).str.strip().eq(status)
                mask = m if mask is None else (mask | m)
            if mask is not None:
                out = out[mask]

    # Carteira
    if carteira != "todas":
        carteira_interno = _ajustar_carteira_para_interno(carteira)
        if "Check" in out.columns:
            out = out[out["Check"].astype(str).str.strip().eq(carteira_interno)]

    return out

# =========================
#  Cascata (CACHE)
# =========================
def calcular_cascata(cfg: Config, mes: str, status: str, carteira: str, nocache: bool = False) -> List[Dict[str, Any]]:
    """
    Lista de dicts p/ gráfico em cascata somando métricas do long (tF_Vendas_long) ou fontes específicas.
    Cacheado por (cfg, mes, status, carteira).
    """
    import numpy as np
    import pandas as pd

    key = _mk_key("calcular_cascata", cfg=_cfg_key(cfg), mes=mes, status=status, carteira=carteira)
    def _build():
        dfs = carregar_pipeline(cfg, nocache=nocache)
        src = dfs.get("tF_Vendas_long")
        if src is None or src.empty:
            frames = []
            for k in ["Receita_PoC", "Receita_Produto", "Receita_SuccessFee", "Estoque", "Pendente_Alocacao_HD", "Meta_Receita", "Vendas"]:
                if k in dfs and dfs[k] is not None:
                    frames.append(dfs[k])
            src = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()

        src = _aplicar_filtros_basicos(src, mes=mes, status=status, carteira=carteira)

        if "Atributo" in src.columns and "Valor" in src.columns:
            pivot = src.pivot_table(index=None, columns="Atributo", values="Valor", aggfunc="sum")
            poc         = float(pivot.get("ReceitaPoC", 0) or 0)
            sfee        = float(pivot.get("SuccessFee", 0) or 0)
            prod        = float(pivot.get("ReceitaProduto", 0) or 0)
            pend_form   = float(pivot.get("ReceitaPendenteAlocMes", 0) or 0)
            pend_ass    = float(pivot.get("ReceitaPendenteAssinatura", 0) or 0)
            potencial   = float(pivot.get("ReceitaPotencialPocMes", 0) or 0)
            gap_meta    = float(pivot.get("DifMeta", 0) or 0)
            total       = float(pivot.get("ReceitaTotal", 0) or (poc + sfee + prod + pend_form + pend_ass + potencial))
        else:
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
    return _cached_call(key, _build, ttl=CACHE_TTL, nocache=nocache)

# =========================
#  Helpers 2025 (pivot)
# =========================
def _filtrar_ano_2025(df):
    import pandas as pd
    if df is None or df.empty: return df
    if "mes_calendario" not in df.columns: return df
    s = pd.to_datetime(df["mes_calendario"], errors="coerce")
    return df[(s.dt.year == 2025)]

def _pivot_mensal_2025(df, valor_col: str):
    """
    Espera colunas: Check (carteira), nome_cliente, codigo_frente, mes_calendario, <valor_col>
    Gera pivot com colunas Jan..Dez/2025 + Total por linha.
    """
    import pandas as pd
    if df is None or df.empty:
        cols = [pd.Timestamp(2025, m, 1).strftime("%b/2025") for m in range(1, 13)] + ["Total"]
        return pd.DataFrame(columns=["Carteira","Cliente","Frente"] + cols)

    df = df.copy()
    df["mes_cal"] = pd.to_datetime(df["mes_calendario"], errors="coerce")
    df["mes_label"] = df["mes_cal"].dt.strftime("%b/2025")
    df["Carteira"] = df.get("Check", "")
    df["Cliente"]  = df.get("nome_cliente", "")
    df["Frente"]   = df.get("codigo_frente", "")
    df = df[df["mes_cal"].dt.year == 2025]
    grp = df.groupby(["Carteira","Cliente","Frente","mes_label"], dropna=False)[valor_col].sum().reset_index()
    pivot = grp.pivot_table(index=["Carteira","Cliente","Frente"], columns="mes_label", values=valor_col, aggfunc="sum").fillna(0.0)
    meses = [pd.Timestamp(2025, m, 1).strftime("%b/2025") for m in range(1, 13)]
    for ml in meses:
        if ml not in pivot.columns:
            pivot[ml] = 0.0
    pivot = pivot[meses]
    pivot["Total"] = pivot.sum(axis=1)
    return pivot.reset_index()

# =========================
#  Limite de linhas (UI)
# =========================
def limit_rows(df: pd.DataFrame | None, max_rows: int = 200) -> pd.DataFrame:
    """Limita linhas apenas para renderização em tela (export usa DF completo)."""
    if df is None: return pd.DataFrame()
    if not isinstance(df, pd.DataFrame): return pd.DataFrame()
    if df.shape[0] <= max_rows: return df
    return df.head(max_rows).copy()

# =========================
#  Tabelas (CACHE)
# =========================
def _tabela_cache_key(nome: str, cfg: Config, mes: str, status: str, carteira: str) -> str:
    return _mk_key(nome, cfg=_cfg_key(cfg), mes=mes, status=status, carteira=carteira)

def tabela_poc(cfg: Config, mes: str, status: str, carteira: str, nocache: bool = False):
    import pandas as pd
    key = _tabela_cache_key("tabela_poc", cfg, mes, status, carteira)
    def _build():
        dfs = carregar_pipeline(cfg, nocache=nocache)
        df = dfs.get("Receita_PoC")
        if df is None: return pd.DataFrame()
        df = _aplicar_filtros_basicos(df, mes, status, carteira)
        df = _filtrar_ano_2025(df)
        return _pivot_mensal_2025(df, "ReceitaPoC")
    return _cached_call(key, _build, ttl=CACHE_TTL, nocache=nocache)

def tabela_success_fee(cfg: Config, mes: str, status: str, carteira: str, nocache: bool = False):
    import pandas as pd
    key = _tabela_cache_key("tabela_success_fee", cfg, mes, status, carteira)
    def _build():
        dfs = carregar_pipeline(cfg, nocache=nocache)
        df = dfs.get("Receita_SuccessFee")
        if df is None: return pd.DataFrame()
        df = _aplicar_filtros_basicos(df, mes, status, carteira)
        df = _filtrar_ano_2025(df)
        return _pivot_mensal_2025(df, "SuccessFee")
    return _cached_call(key, _build, ttl=CACHE_TTL, nocache=nocache)

def tabela_produtos(cfg: Config, mes: str, status: str, carteira: str, nocache: bool = False):
    import pandas as pd
    key = _tabela_cache_key("tabela_produtos", cfg, mes, status, carteira)
    def _build():
        dfs = carregar_pipeline(cfg, nocache=nocache)
        df = dfs.get("Receita_Produto")
        car = dfs.get("Carteira_Produto")
        base = pd.concat([d for d in [df, car] if d is not None], ignore_index=True, sort=False) if (df is not None or car is not None) else pd.DataFrame()
        if base is None or base.empty: return pd.DataFrame()
        base = _aplicar_filtros_basicos(base, mes, status, carteira)
        base = _filtrar_ano_2025(base)
        return _pivot_mensal_2025(base, "ReceitaProduto")
    return _cached_call(key, _build, ttl=CACHE_TTL, nocache=nocache)

def tabela_pendente_formacao(cfg: Config, mes: str, status: str, carteira: str, nocache: bool = False):
    import pandas as pd
    VALOR_COL = "ReceitaPendenteAlocMes"
    key = _tabela_cache_key("tabela_pendente_formacao", cfg, mes, status, carteira)
    def _build():
        dfs = carregar_pipeline(cfg, nocache=nocache)
        candidatos = ["Pendente_Alocacao_HD", "tF_Vendas_long", "Vendas"]
        src = None
        for k in candidatos:
            d = dfs.get(k)
            if d is not None and (VALOR_COL in d.columns):
                src = d; break
        if src is None or src.empty: return _pivot_mensal_2025(pd.DataFrame(), VALOR_COL)
        src = _aplicar_filtros_basicos(src, mes, status, carteira)
        src = _filtrar_ano_2025(src)
        if VALOR_COL not in src.columns: return _pivot_mensal_2025(pd.DataFrame(), VALOR_COL)
        return _pivot_mensal_2025(src, VALOR_COL)
    return _cached_call(key, _build, ttl=CACHE_TTL, nocache=nocache)

def tabela_pendente_assinatura(cfg: Config, mes: str, status: str, carteira: str, nocache: bool = False):
    import pandas as pd
    VALOR_COL = "ReceitaPendenteAssinatura"
    key = _tabela_cache_key("tabela_pendente_assinatura", cfg, mes, status, carteira)
    def _build():
        dfs = carregar_pipeline(cfg, nocache=nocache)
        candidatos = ["Pendente_Assinatura", "tF_Vendas_long", "Vendas"]
        src = None
        for k in candidatos:
            d = dfs.get(k)
            if d is not None and (VALOR_COL in d.columns):
                src = d; break
        if src is None or src.empty: return _pivot_mensal_2025(pd.DataFrame(), VALOR_COL)
        src = _aplicar_filtros_basicos(src, mes, status, carteira)
        src = _filtrar_ano_2025(src)
        if VALOR_COL not in src.columns: return _pivot_mensal_2025(pd.DataFrame(), VALOR_COL)
        return _pivot_mensal_2025(src, VALOR_COL)
    return _cached_call(key, _build, ttl=CACHE_TTL, nocache=nocache)

def tabela_receita_potencial(cfg: Config, mes: str, status: str, carteira: str, nocache: bool = False):
    import pandas as pd
    VALOR_COL = "ReceitaPotencialPocMes"
    key = _tabela_cache_key("tabela_receita_potencial", cfg, mes, status, carteira)
    def _build():
        dfs = carregar_pipeline(cfg, nocache=nocache)
        candidatos = ["Receita_Potencial", "tF_Vendas_long", "Vendas"]
        src = None
        for k in candidatos:
            d = dfs.get(k)
            if d is not None and (VALOR_COL in d.columns):
                src = d; break
        if src is None or src.empty: return _pivot_mensal_2025(pd.DataFrame(), VALOR_COL)
        src = _aplicar_filtros_basicos(src, mes, status, carteira)
        src = _filtrar_ano_2025(src)
        if VALOR_COL not in src.columns: return _pivot_mensal_2025(pd.DataFrame(), VALOR_COL)
        return _pivot_mensal_2025(src, VALOR_COL)
    return _cached_call(key, _build, ttl=CACHE_TTL, nocache=nocache)
