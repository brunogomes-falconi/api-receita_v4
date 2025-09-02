from django.shortcuts import render
from datetime import date

# Página inicial (resumo)
def resumo(request):
    return render(request, "home.html")

# Abas principais
def receita(request):
    return render(request, "receita/receita.html")

def poc(request):
    return render(request, "receita/poc.html")

def success_fee(request):
    return render(request, "receita/success_fee.html")

def produtos(request):
    return render(request, "receita/produtos.html")

def pendente_formacao(request):
    return render(request, "receita/pendente_formacao.html")

def pendente_assinatura(request):
    return render(request, "receita/pendente_assinatura.html")

def receita_potencial(request):
    return render(request, "receita/receita_potencial.html")

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
CARTEIRAS_EXEMPLO = [
    {"value": "todas", "label": "Selecionar Todos"},
    {"value": "Falconi EUA", "label": "Falconi EUA"},
    {"value": "Infraestrutura e Indústria de Base", "label": "Infraestrutura e Indústria de Base"},
    {"value": "Bens Não Duráveis", "label": "Bens Não Duráveis"},
    {"value": "Saúde Educação Segurança e Adm.Pública", "label": "Saúde Educação Segurança e Adm.Pública"},
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
    if carteira not in [c["value"] for c in CARTEIRAS_EXEMPLO]:
        carteira = "todas"

    return {
        "mes": mes,
        "status": status,
        "carteira": carteira,
    }

def _contexto_comum(request, titulo_pagina):
    filtros = _get_filtros(request)
    return {
        "titulo_pagina": titulo_pagina,
        "filtros": filtros,
        "MESES_2025": MESES_2025,
        "STATUS_OPCOES": STATUS_OPCOES,
        "CARTEIRAS": CARTEIRAS_EXEMPLO,
    }

# ---------- Views ----------

def resumo(request):
    ctx = _contexto_comum(request, "Início · Falconi")
    return render(request, "home.html", ctx)

def receita(request):
    ctx = _contexto_comum(request, "Receita (Cascata) · Falconi")
    # placeholder de dados agregados (vamos carregar do pipeline na próxima etapa)
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
    return render(request, "receita/poc.html", ctx)

def success_fee(request):
    ctx = _contexto_comum(request, "Success Fee · Falconi")
    return render(request, "receita/success_fee.html", ctx)

def produtos(request):
    ctx = _contexto_comum(request, "Produtos · Falconi")
    return render(request, "receita/produtos.html", ctx)

def pendente_formacao(request):
    ctx = _contexto_comum(request, "Pendente Formação de Equipe · Falconi")
    return render(request, "receita/pendente_formacao.html", ctx)

def pendente_assinatura(request):
    ctx = _contexto_comum(request, "Pendente Assinatura · Falconi")
    return render(request, "receita/pendente_assinatura.html", ctx)

def receita_potencial(request):
    ctx = _contexto_comum(request, "Receita Potencial · Falconi")
    return render(request, "receita/receita_potencial.html", ctx)