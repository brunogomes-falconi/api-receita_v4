from django.shortcuts import render
from django.http import HttpResponse
import pandas as pd
from io import BytesIO

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
    if not carteira:
        carteira = "todas"

    return {"mes": mes, "status": status, "carteira": carteira}


def _contexto_comum(request, titulo_pagina):
    filtros = _get_filtros(request)

    # A Config do basecode.py não aceita 'use_access'; instancie sem argumentos.
    cfg = Config()

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


# ---------- Views ----------
def resumo(request):
    ctx = _contexto_comum(request, "Início · Falconi")
    return render(request, "home.html", ctx)


def receita(request):
    """
    Página do gráfico em cascata (Receita 2025).
    Agora usa dados reais do pipeline via calcular_cascata(...).
    Mantém fallback seguro em caso de erro de conector/coluna.
    """
    ctx = _contexto_comum(request, "Receita (Cascata) · Falconi")

    # Instancie a Config do seu basecode.py sem 'use_access' (esse campo não existe).
    # Quando o driver do Access (ACE/pyodbc) estiver instalado e os caminhos corretos,
    # basta garantir que os paths em Config apontem para suas bases. Opcionalmente,
    # preencha explicitamente algum caminho/campo abaixo.
    cfg = Config(
        # Exemplo (descomente e ajuste se quiser forçar caminhos locais):
        # access_db_resultado=r"C:\caminho\BD_Resultado.accdb",
        # access_db_razao=r"C:\caminho\Base_Razao.accdb",
        # access_db_caixa=r"C:\caminho\Base_Caixa.accdb",
        # access_db_roda_razao=r"C:\caminho\Roda_Base_Razao.accdb",
        # xlsx_depara_un=r"C:\caminho\DePara_UN.xlsx",
        # csv_meta_receita=r"C:\caminho\meta_receita.csv",
        # bigquery_project_id="seu-projeto-gcp",
    )

    filtros = ctx["filtros"]
    try:
        dados = calcular_cascata(cfg, filtros["mes"], filtros["status"], filtros["carteira"])
        # sanity-check básico: precisa ser lista de dicts com label/valor
        if not isinstance(dados, list) or not all(isinstance(x, dict) for x in dados):
            raise ValueError("Formato inesperado retornado por calcular_cascata.")
        ctx["waterfall_data"] = dados
    except Exception as e:
        # Loga o erro e mantém o placeholder seguro
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
    ctx["table"] = tabela_poc(cfg, f["mes"], f["status"], f["carteira"]) or pd.DataFrame()
    return render(request, "receita/poc.html", ctx)


def success_fee(request):
    ctx = _contexto_comum(request, "Success Fee · Falconi")
    cfg = Config()
    f = ctx["filtros"]
    ctx["table"] = tabela_success_fee(cfg, f["mes"], f["status"], f["carteira"]) or pd.DataFrame()
    return render(request, "receita/success_fee.html", ctx)


def produtos(request):
    ctx = _contexto_comum(request, "Produtos · Falconi")
    cfg = Config()
    f = ctx["filtros"]
    ctx["table"] = tabela_produtos(cfg, f["mes"], f["status"], f["carteira"]) or pd.DataFrame()
    return render(request, "receita/produtos.html", ctx)


# NOVAS ABAS
def pendente_formacao(request):
    ctx = _contexto_comum(request, "Pendente Formação · Falconi")
    cfg = Config()
    f = ctx["filtros"]
    ctx["table"] = tabela_pendente_formacao(cfg, f["mes"], f["status"], f["carteira"]) or pd.DataFrame()
    return render(request, "receita/pendente_formacao.html", ctx)


def pendente_assinatura(request):
    ctx = _contexto_comum(request, "Pendente Assinatura · Falconi")
    cfg = Config()
    f = ctx["filtros"]
    ctx["table"] = tabela_pendente_assinatura(cfg, f["mes"], f["status"], f["carteira"]) or pd.DataFrame()
    return render(request, "receita/pendente_assinatura.html", ctx)


def receita_potencial(request):
    ctx = _contexto_comum(request, "Receita Potencial · Falconi")
    cfg = Config()
    f = ctx["filtros"]
    ctx["table"] = tabela_receita_potencial(cfg, f["mes"], f["status"], f["carteira"]) or pd.DataFrame()
    return render(request, "receita/receita_potencial.html", ctx)


# EXPORTAÇÃO GENÉRICA
def exportar_excel(request, tipo: str):
    """
    tipos suportados:
      - 'poc' | 'success_fee' | 'produtos'
      - 'pend_formacao' | 'pend_assinatura' | 'potencial'
    Gera um .xlsx com uma aba "dados" contendo o DataFrame cru (após filtros).
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
    else:
        df = pd.DataFrame(); fname = "export.xlsx"

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
