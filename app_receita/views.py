from django.shortcuts import render
from django.http import HttpResponse
import pandas as pd
from io import BytesIO

from django.views.decorators.cache import cache_page
from django.utils.decorators import method_decorator  # se quiser usar em CBVs no futuro

from app_receita.services.dados import (
    Config,
    calcular_cascata,
    listar_carteiras_ui,
    tabela_poc,
    tabela_success_fee,
    tabela_produtos,
    tabela_pendente_formacao,
    tabela_pendente_assinatura,
    tabela_receita_potencial,
    calcular_cascata_estoque,
    tabela_estoque,
)

# ---------- Helpers de filtros ----------
MESES_2025 = [
    {"value": "2025-01", "label": "Jan/2025"},
    {"value": "2025-02", "label": "Fev/2025"},
    {"value": "2025-03", "label": "Mar/2025"},
    {"value": "2025-04", "label": "Abr/2025"},
    {"value": "2025-05", "label": "Mai/2025"},
    {"value": "2025-06", "label": "Jun/2025"},
    {"value": "2025-07", "label": "Jul/2025"},
    {"value": "2025-08", "label": "Ago/2025"},
    {"value": "2025-09", "label": "Set/2025"},
    {"value": "2025-10", "label": "Out/2025"},
    {"value": "2025-11", "label": "Nov/2025"},
    {"value": "2025-12", "label": "Dez/2025"},
]

STATUS_OPCOES = [
    {"value": "todos", "label": "Selecionar Todos"},
    {"value": "Novo", "label": "Novo"},
    {"value": "Renovação", "label": "Renovação"},
]

ANOS = [{"value": str(a), "label": str(a)} for a in [2025, 2026, 2027, 2028, 2029]]

TIPOS_ESTOQUE = [
    {"value": "Pendente Formação", "label": "Pendente Formação de Equipe"},
    {"value": "Receita", "label": "Receita (PoC)"},
    {"value": "Receita Produto", "label": "Receita Produto"},
    {"value": "Success Fee", "label": "Success Fee"},
]


def _get_filtros(request):
    mes = request.GET.get("mes", "tudo")
    status = request.GET.get("status", "todos")
    carteira = request.GET.get("carteira", "todas")

    if mes != "tudo" and mes not in [m["value"] for m in MESES_2025]:
        mes = "tudo"
    if status not in [s["value"] for s in STATUS_OPCOES]:
        status = "todos"
    if not carteira:
        carteira = "todas"

    return {"mes": mes, "status": status, "carteira": carteira}


def _get_filtros_estoque(request):
    ano = request.GET.get("ano", "2025")
    tipo = request.GET.get("tipo", "Receita")  # só usado na tela de detalhes
    status = request.GET.get("status", "todos")
    carteira = request.GET.get("carteira", "todas")

    if ano not in [x["value"] for x in ANOS]:
        ano = "2025"
    if status not in [s["value"] for s in STATUS_OPCOES]:
        status = "todos"
    if tipo not in [t["value"] for t in TIPOS_ESTOQUE]:
        tipo = "Receita"
    if not carteira:
        carteira = "todas"

    return {"ano": ano, "tipo": tipo, "status": status, "carteira": carteira}


def _contexto_comum(request, titulo_pagina):
    filtros = _get_filtros(request)
    cfg = Config()  # sem use_access

    try:
        carteiras_ui = listar_carteiras_ui(cfg)
    except Exception:
        carteiras_ui = [
            "Agronegócio", "América do Norte", "Bens Não Duráveis",
            "Infraestrutura e Indústria de Base", "MID",
            "Saúde Educação Segurança e Adm.Pública", "Servicos e Tecnologia",
        ]

    carteiras_options = [{"value": "todas", "label": "Selecionar Todos"}] + [
        {"value": c, "label": c} for c in carteiras_ui
    ]

    return {
        "titulo_pagina": titulo_pagina,
        "filtros": filtros,
        "MESES_2025": MESES_2025,
        "STATUS_OPCOES": STATUS_OPCOES,
        "CARTEIRAS": carteiras_options,
    }

def _is_nocache(request):
    return request.GET.get("nocache") in ("1", "true", "yes")


# ---------- Views existentes ----------
def resumo(request):
    ctx = _contexto_comum(request, "Início · Falconi")
    return render(request, "home.html", ctx)


def receita(request):
    ctx = _contexto_comum(request, "Receita (Cascata) · Falconi")

    cfg = Config(
        # Ajuste caminhos/credenciais se desejar
        # bigquery_project_id="seu-projeto-gcp",
    )

    f = ctx["filtros"]
    try:
        dados = calcular_cascata(cfg, f["mes"], f["status"], f["carteira"])
        if not isinstance(dados, list) or not all(isinstance(x, dict) for x in dados):
            raise ValueError("Formato inesperado retornado por calcular_cascata.")
        ctx["waterfall_data"] = dados
    except Exception as e:
        print("[receita] Erro ao calcular cascata:", e)
        import traceback; traceback.print_exc()
        ctx["waterfall_data"] = [
            {"label": "Receita PoC", "valor": 0},
            {"label": "Receita Success Fee", "valor": 0},
            {"label": "Receita Produtos", "valor": 0},
            {"label": "Pendente Formação de Equipe", "valor": 0},
            {"label": "Pendente Assinatura", "valor": 0},
            {"label": "Receita Potencial", "valor": 0},
            {"label": "GAP Meta", "valor": 0},
            {"label": "Total", "valor": 0},
        ]

    return render(request, "receita/receita.html", ctx)


def poc(request):
    ctx = _contexto_comum(request, "PoC · Falconi")
    cfg = Config()
    f = ctx["filtros"]
    df = tabela_poc(cfg, f["mes"], f["status"], f["carteira"])
    ctx["table"] = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    return render(request, "receita/poc.html", ctx)


def success_fee(request):
    ctx = _contexto_comum(request, "Success Fee · Falconi")
    cfg = Config()
    f = ctx["filtros"]
    df = tabela_success_fee(cfg, f["mes"], f["status"], f["carteira"])
    ctx["table"] = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    return render(request, "receita/success_fee.html", ctx)


def produtos(request):
    ctx = _contexto_comum(request, "Produtos · Falconi")
    cfg = Config()
    f = ctx["filtros"]
    df = tabela_produtos(cfg, f["mes"], f["status"], f["carteira"])
    ctx["table"] = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    return render(request, "receita/produtos.html", ctx)


def pendente_formacao(request):
    ctx = _contexto_comum(request, "Pendente Formação · Falconi")
    cfg = Config()
    f = ctx["filtros"]
    df = tabela_pendente_formacao(cfg, f["mes"], f["status"], f["carteira"])
    ctx["table"] = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    return render(request, "receita/pendente_formacao.html", ctx)


def pendente_assinatura(request):
    ctx = _contexto_comum(request, "Pendente Assinatura · Falconi")
    cfg = Config()
    f = ctx["filtros"]
    df = tabela_pendente_assinatura(cfg, f["mes"], f["status"], f["carteira"])
    ctx["table"] = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    return render(request, "receita/pendente_assinatura.html", ctx)


def receita_potencial(request):
    ctx = _contexto_comum(request, "Receita Potencial · Falconi")
    cfg = Config()
    f = ctx["filtros"]
    df = tabela_receita_potencial(cfg, f["mes"], f["status"], f["carteira"])
    ctx["table"] = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    return render(request, "receita/receita_potencial.html", ctx)


# ---------- NOVAS VIEWS: ESTOQUE ----------
def estoque(request):
    """
    Página do gráfico em cascata de Estoque (ano 2025..2029), sem 'GAP Meta'.
    """
    # contexto base + adiciona listas de ANOS/CARTEIRAS/STATUS
    ctx = _contexto_comum(request, "Estoque (Cascata) · Falconi")
    filtros_e = _get_filtros_estoque(request)
    ctx["ANOS"] = ANOS
    ctx["filtros_estoque"] = filtros_e

    cfg = Config()

    try:
        dados = calcular_cascata_estoque(cfg, int(filtros_e["ano"]), filtros_e["status"], filtros_e["carteira"])
        ctx["waterfall_data"] = dados
    except Exception as e:
        print("[estoque] Erro ao calcular cascata:", e)
        import traceback; traceback.print_exc()
        ctx["waterfall_data"] = [
            {"label": "Receita PoC", "valor": 0},
            {"label": "Receita Success Fee", "valor": 0},
            {"label": "Receita Produtos", "valor": 0},
            {"label": "Pendente Formação de Equipe", "valor": 0},
            {"label": "Pendente Assinatura", "valor": 0},
            {"label": "Receita Potencial", "valor": 0},
            {"label": "Total", "valor": 0},
        ]

    return render(request, "estoque/estoque.html", ctx)


def estoque_detalhes(request):
    """
    Tabela anual de Estoque (Carteira/Cliente/Frente × 2025..2029), filtrando por tipo de receita.
    """
    ctx = _contexto_comum(request, "Detalhes de Estoque · Falconi")
    filtros_e = _get_filtros_estoque(request)
    ctx["ANOS"] = ANOS
    ctx["TIPOS_ESTOQUE"] = TIPOS_ESTOQUE
    ctx["filtros_estoque"] = filtros_e

    cfg = Config()

    try:
        df = tabela_estoque(cfg, int(filtros_e["ano"]), filtros_e["tipo"], filtros_e["status"], filtros_e["carteira"])
    except Exception as e:
        print("[estoque_detalhes] Erro na tabela:", e)
        import traceback; traceback.print_exc()
        df = pd.DataFrame()

    ctx["table"] = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    return render(request, "estoque/detalhes.html", ctx)


# ---------- EXPORTAÇÃO ----------
def exportar_excel(request, tipo: str):
    """
    tipos suportados:
      - 'poc' | 'success_fee' | 'produtos'
      - 'pend_formacao' | 'pend_assinatura' | 'potencial'
      - 'estoque' (detalhes de estoque por tipo)
    Gera um .xlsx com uma aba "dados".
    """
    f = _get_filtros(request)
    cfg = Config()

    if tipo == "poc":
        df = tabela_poc(cfg, f["mes"], f["status"], f["carteira"]); fname = "poc_2025.xlsx"
    elif tipo == "success_fee":
        df = tabela_success_fee(cfg, f["mes"], f["status"], f["carteira"]); fname = "success_fee_2025.xlsx"
    elif tipo == "produtos":
        df = tabela_produtos(cfg, f["mes"], f["status"], f["carteira"]); fname = "produtos_2025.xlsx"
    elif tipo == "pend_formacao":
        df = tabela_pendente_formacao(cfg, f["mes"], f["status"], f["carteira"]); fname = "pendente_formacao_2025.xlsx"
    elif tipo == "pend_assinatura":
        df = tabela_pendente_assinatura(cfg, f["mes"], f["status"], f["carteira"]); fname = "pendente_assinatura_2025.xlsx"
    elif tipo == "potencial":
        df = tabela_receita_potencial(cfg, f["mes"], f["status"], f["carteira"]); fname = "receita_potencial_2025.xlsx"
    elif tipo == "estoque":
        # usa filtros específicos de estoque
        fe = _get_filtros_estoque(request)
        df = tabela_estoque(cfg, int(fe["ano"]), fe["tipo"], fe["status"], fe["carteira"])
        fname = f"estoque_{fe['tipo'].replace(' ', '_').lower()}_{fe['ano']}.xlsx"
    else:
        df = pd.DataFrame(); fname = "export.xlsx"

    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        (df if isinstance(df, pd.DataFrame) else pd.DataFrame()).to_excel(writer, index=False, sheet_name="dados")
    bio.seek(0)

    resp = HttpResponse(
        bio.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    resp["Content-Disposition"] = f'attachment; filename="{fname}"'
    return resp
