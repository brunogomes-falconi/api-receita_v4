from django.shortcuts import render
from django.http import HttpResponse
from datetime import date
import pandas as pd
from app_receita.services.dados import (
    Config, calcular_cascata, listar_carteiras_ui,
    tabela_poc, tabela_success_fee, tabela_produtos
)

# Página inicial (resumo)
def resumo(request):
    return render(request, "home.html")

# Abas principais
def receita(request):
    return render(request, "receita/receita.html")

def poc(request):
    ctx = _contexto_comum(request, "PoC · Falconi")
    cfg = Config(use_access=False)  # mude para True quando tiver o driver
    filtros = ctx["filtros"]
    df = tabela_poc(cfg, filtros["mes"], filtros["status"], filtros["carteira"])
    ctx["table"] = df  # usaremos no template
    return render(request, "receita/poc.html", ctx)

def success_fee(request):
    ctx = _contexto_comum(request, "Success Fee · Falconi")
    cfg = Config(use_access=False)
    filtros = ctx["filtros"]
    df = tabela_success_fee(cfg, filtros["mes"], filtros["status"], filtros["carteira"])
    ctx["table"] = df
    return render(request, "receita/success_fee.html", ctx)

def produtos(request):
    ctx = _contexto_comum(request, "Produtos · Falconi")
    cfg = Config(use_access=False)
    filtros = ctx["filtros"]
    df = tabela_produtos(cfg, filtros["mes"], filtros["status"], filtros["carteira"])
    ctx["table"] = df
    return render(request, "receita/produtos.html", ctx)

def pendente_formacao(request):
    return render(request, "receita/pendente_formacao.html")

def pendente_assinatura(request):
    return render(request, "receita/pendente_assinatura.html")

def receita_potencial(request):
    return render(request, "receita/receita_potencial.html")

def exportar_excel(request, tipo: str):
    """
    tipo: 'poc' | 'success_fee' | 'produtos'
    Usa os filtros atuais da querystring.
    """
    filtros = _get_filtros(request)
    cfg = Config(use_access=False)
    if tipo == "poc":
        df = tabela_poc(cfg, filtros["mes"], filtros["status"], filtros["carteira"])
        fname = "poc_2025.xlsx"
    elif tipo == "success_fee":
        df = tabela_success_fee(cfg, filtros["mes"], filtros["status"], filtros["carteira"])
        fname = "success_fee_2025.xlsx"
    elif tipo == "produtos":
        df = tabela_produtos(cfg, filtros["mes"], filtros["status"], filtros["carteira"])
        fname = "produtos_2025.xlsx"
    else:
        df = pd.DataFrame()
        fname = "export.xlsx"

    # resposta Excel simples (dados crus)
    from io import BytesIO
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        (df if df is not None else pd.DataFrame()).to_excel(writer, index=False, sheet_name="dados")
    bio.seek(0)

    resp = HttpResponse(
        bio.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    resp["Content-Disposition"] = f'attachment; filename="{fname}"'
    return resp

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

# OBS: por enquanto, carteiras de exemplo; vamos trocar para vir dos dados (distinct)
CARTEIRAS_FIXAS = [
    {"value": "todas", "label": "Selecionar Todos"},
    {"value": "Agronegócio", "label": "Agronegócio"},
    {"value": "América do Norte", "label": "América do Norte"},
    {"value": "Bens Não Duráveis", "label": "Bens Não Duráveis"},
    {"value": "Infraestrutura e Indústria de Base", "label": "Infraestrutura e Indústria de Base"},
    {"value": "MID", "label": "MID"},
    {"value": "Saúde Educação Segurança e Adm.Pública", "label": "Saúde Educação Segurança e Adm.Pública"},
    {"value": "Servicos e Tecnologia", "label": "Servicos e Tecnologia"},
]

def _get_filtros(request):
    """
    Extrai filtros da querystring:
      - mes (string: '2025-01' ... '2025-12' ou 'tudo')
      - status (string: 'Novo' | 'Renovação' | 'todos')
      - carteira (string: 'todas' | <nome>)
    """
    mes = request.GET.get("mes", "tudo")
    status = request.GET.get("status", "todos")
    carteira = request.GET.get("carteira", "todas")

    # defaults defensivos
    if mes != "tudo" and mes not in [m["value"] for m in MESES_2025]:
        mes = "tudo"
    if status not in [s["value"] for s in STATUS_OPCOES]:
        status = "todos"
    if carteira not in [c["value"] for c in CARTEIRAS_FIXAS]:
        carteira = "todas"

    return {
        "mes": mes,
        "status": status,
        "carteira": carteira,
    }

def _contexto_comum(request, titulo_pagina):
    filtros = _get_filtros(request)

    # Config local (se estiver sem Access por enquanto, mantenha use_access=False)
    cfg = Config(
        # preencha caminhos reais quando quiser usar as fontes
        # access_db_razao=..., access_db_resultado=..., etc.
        use_access=False,  # troque para True quando tiver o driver do Access ok
        # bigquery_project_id="SEU-PROJETO-GCP",
    )

    try:
        carteiras_ui = listar_carteiras_ui(cfg)
    except Exception:
        carteiras_ui = [
            "Agronegócio","América do Norte","Bens Não Duráveis","Infraestrutura e Indústria de Base",
            "MID","Saúde Educação Segurança e Adm.Pública","Servicos e Tecnologia"
        ]

    # monta a lista para o select (com “Selecionar Todos”)
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

# ---------- Views ----------

def resumo(request):
    ctx = _contexto_comum(request, "Início · Falconi")
    return render(request, "home.html", ctx)

def receita(request):
    ctx = _contexto_comum(request, "Receita (Cascata) · Falconi")

    # Monte a Config com seus caminhos reais (ajuste depois; por ora podemos testar com o que já tiver local)
    cfg = Config(
        # Preencha aqui os caminhos reais se quiser evitar o default do módulo:
        access_db_razao=r"C:\Work\Base\Base_Razao.accdb",
        access_db_caixa=r"C:\Work\Base\Base_Caixa.accdb",
        access_db_resultado=r"C:\Work\Base\BD_Resultado.accdb",
        access_db_roda_razao=r"C:\Work\Base\Roda_Base_Razao.accdb",
        # csv_meta_receita=... etc
        # bigquery_project_id="SEU-PROJETO-GCP",
    )

    filtros = ctx["filtros"]
    try:
        ctx["waterfall_data"] = calcular_cascata(cfg, filtros["mes"], filtros["status"], filtros["carteira"])
    except Exception as e:
        # Se der qualquer problema de conector, mantém placeholder e mostra um aviso básico no console
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