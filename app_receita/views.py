from django.shortcuts import render, redirect
from django.http import HttpResponse
from django.views.decorators.cache import cache_page

from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.contrib.auth.models import User

from .models import UserProfile, ViewingPermission
from .forms import ProfileForm, PermissionGrantForm, PermissionRevokeForm

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
    limit_rows,  # novo: truncar renderização
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

CARTEIRAS_UI_OFICIAIS = [
    "Agronegócio",
    "América do Norte",
    "Bens Não Duráveis",
    "Infraestrutura e Indústria de Base",
    "MID",
    "Saúde Educação Segurança e Adm.Pública",
    "Servicos e Tecnologia",
]

def _carteiras_permitidas_por_role(role: str) -> list[str]:
    """Mapa simples: ADMIN/CEO/CFO/CTRL enxergam todas; VP/DIR enxergam as suas (a definir no futuro) + concedidas; USER só concedidas."""
    if role in ("ADMIN", "CEO", "CFO", "CTRL"):
        return CARTEIRAS_UI_OFICIAIS[:]  # todas
    # Para VP/DIR, poderíamos associar "carteira própria" ao perfil; por ora, fica vazio e confiamos nas concessões.
    return []

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

    # Config do basecode.py (sem 'use_access')
    cfg = Config()

    try:
        carteiras_ui = listar_carteiras_ui(cfg)
    except Exception:
        # fallback estável
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

@cache_page(60)  # cache leve de página
def receita(request):
    """
    Página do gráfico em cascata (Receita 2025) usando calcular_cascata(...).
    Mantém fallback seguro em caso de erro.
    """
    ctx = _contexto_comum(request, "Receita (Cascata) · Falconi")
    cfg = Config()
    filtros = ctx["filtros"]
    nocache = request.GET.get("nocache") == "1"  # bypass apenas do cache de serviços

    try:
        dados = calcular_cascata(cfg, filtros["mes"], filtros["status"], filtros["carteira"], nocache=nocache)
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

@cache_page(60)
def poc(request):
    ctx = _contexto_comum(request, "PoC · Falconi")
    cfg = Config()
    f = ctx["filtros"]
    nocache = request.GET.get("nocache") == "1"
    df = tabela_poc(cfg, f["mes"], f["status"], f["carteira"], nocache=nocache)
    total = int(df.shape[0]) if isinstance(df, pd.DataFrame) else 0
    ctx["table"] = limit_rows(df, 200)
    ctx["table_total_rows"] = total
    ctx["table_limited_rows"] = ctx["table"].shape[0] < total
    return render(request, "receita/poc.html", ctx)

@cache_page(60)
def success_fee(request):
    ctx = _contexto_comum(request, "Success Fee · Falconi")
    cfg = Config()
    f = ctx["filtros"]
    nocache = request.GET.get("nocache") == "1"
    df = tabela_success_fee(cfg, f["mes"], f["status"], f["carteira"], nocache=nocache)
    total = int(df.shape[0]) if isinstance(df, pd.DataFrame) else 0
    ctx["table"] = limit_rows(df, 200)
    ctx["table_total_rows"] = total
    ctx["table_limited_rows"] = ctx["table"].shape[0] < total
    return render(request, "receita/success_fee.html", ctx)

@cache_page(60)
def produtos(request):
    ctx = _contexto_comum(request, "Produtos · Falconi")
    cfg = Config()
    f = ctx["filtros"]
    nocache = request.GET.get("nocache") == "1"
    df = tabela_produtos(cfg, f["mes"], f["status"], f["carteira"], nocache=nocache)
    total = int(df.shape[0]) if isinstance(df, pd.DataFrame) else 0
    ctx["table"] = limit_rows(df, 200)
    ctx["table_total_rows"] = total
    ctx["table_limited_rows"] = ctx["table"].shape[0] < total
    return render(request, "receita/produtos.html", ctx)

@cache_page(60)
def pendente_formacao(request):
    ctx = _contexto_comum(request, "Pendente Formação · Falconi")
    cfg = Config()
    f = ctx["filtros"]
    nocache = request.GET.get("nocache") == "1"
    df = tabela_pendente_formacao(cfg, f["mes"], f["status"], f["carteira"], nocache=nocache)
    total = int(df.shape[0]) if isinstance(df, pd.DataFrame) else 0
    ctx["table"] = limit_rows(df, 200)
    ctx["table_total_rows"] = total
    ctx["table_limited_rows"] = ctx["table"].shape[0] < total
    return render(request, "receita/pendente_formacao.html", ctx)

@cache_page(60)
def pendente_assinatura(request):
    ctx = _contexto_comum(request, "Pendente Assinatura · Falconi")
    cfg = Config()
    f = ctx["filtros"]
    nocache = request.GET.get("nocache") == "1"
    df = tabela_pendente_assinatura(cfg, f["mes"], f["status"], f["carteira"], nocache=nocache)
    total = int(df.shape[0]) if isinstance(df, pd.DataFrame) else 0
    ctx["table"] = limit_rows(df, 200)
    ctx["table_total_rows"] = total
    ctx["table_limited_rows"] = ctx["table"].shape[0] < total
    return render(request, "receita/pendente_assinatura.html", ctx)

@cache_page(60)
def receita_potencial(request):
    ctx = _contexto_comum(request, "Receita Potencial · Falconi")
    cfg = Config()
    f = ctx["filtros"]
    nocache = request.GET.get("nocache") == "1"
    df = tabela_receita_potencial(cfg, f["mes"], f["status"], f["carteira"], nocache=nocache)
    total = int(df.shape[0]) if isinstance(df, pd.DataFrame) else 0
    ctx["table"] = limit_rows(df, 200)
    ctx["table_total_rows"] = total
    ctx["table_limited_rows"] = ctx["table"].shape[0] < total
    return render(request, "receita/receita_potencial.html", ctx)

@login_required
def opcoes_usuario(request):
    user = request.user
    profile, _ = UserProfile.objects.get_or_create(user=user)

    # Carteiras nativas pelo papel + concessões recebidas
    carteiras_papel = set(_carteiras_permitidas_por_role(profile.role))
    concedidas = set(ViewingPermission.objects.filter(grantee=user).values_list("portfolio", flat=True))
    carteiras_efetivas = sorted(carteiras_papel | concedidas)

    if request.method == "POST":
        # Atualiza preferências do perfil
        if "save_prefs" in request.POST:
            form = ProfileForm(request.POST, instance=profile)
            if form.is_valid():
                form.save()
                messages.success(request, "Preferências salvas com sucesso.")
                return redirect("app_receita:opcoes_usuario")
            else:
                messages.error(request, "Verifique os campos de preferências.")
        # Concede permissão (apenas VP/DIR/ADMIN)
        elif "grant_perm" in request.POST:
            if profile.role in ("VP", "DIR", "ADMIN"):
                pform = PermissionGrantForm(request.POST)
                if pform.is_valid():
                    target = pform.cleaned_data["user"]
                    portfolio = pform.cleaned_data["portfolio"]
                    if target == user:
                        messages.error(request, "Você não pode conceder permissão para si mesmo.")
                    else:
                        ViewingPermission.objects.get_or_create(
                            grantee=target, portfolio=portfolio, defaults={"granted_by": user}
                        )
                        messages.success(request, f"Permissão de '{portfolio}' concedida a {target.username}.")
                    return redirect("app_receita:opcoes_usuario")
                else:
                    messages.error(request, "Selecione um usuário e uma carteira válidos.")
            else:
                messages.error(request, "Você não tem permissão para conceder acessos.")
                return redirect("app_receita:opcoes_usuario")
        # Revoga permissão
        elif "revoke_perm" in request.POST:
            if profile.role in ("VP", "DIR", "ADMIN"):
                rform = PermissionRevokeForm(request.POST)
                if rform.is_valid():
                    perm_id = rform.cleaned_data["perm_id"]
                    ViewingPermission.objects.filter(id=perm_id).delete()
                    messages.success(request, "Permissão revogada.")
                else:
                    messages.error(request, "Não foi possível identificar a permissão a revogar.")
                return redirect("app_receita:opcoes_usuario")

    # GET: prepara forms
    form = ProfileForm(instance=profile)
    grant_form = PermissionGrantForm() if profile.role in ("VP", "DIR", "ADMIN") else None
    minhas_concessoes = ViewingPermission.objects.filter(granted_by=user).select_related("grantee") if grant_form else []

    ctx = {
        "titulo_pagina": "Opções do Usuário · Falconi",
        "user_obj": user,
        "profile": profile,
        "form": form,
        "grant_form": grant_form,
        "minhas_concessoes": minhas_concessoes,
        "carteiras_efetivas": carteiras_efetivas,
        "ROLE_LABEL": dict((k, v) for k, v in [("ADMIN","Administrador"),("CEO","CEO"),("CFO","CFO"),
                                               ("CTRL","Controladoria"),("VP","VP"),("DIR","Diretor"),("USER","Usuário")]),
    }
    return render(request, "user/opcoes.html", ctx)

# EXPORTAÇÃO (sem cache p/ garantir arquivo atualizado)
def exportar_excel(request, tipo: str):
    """
    tipos suportados:
      - 'poc' | 'success_fee' | 'produtos'
      - 'pend_formacao' | 'pend_assinatura' | 'potencial'
    Gera um .xlsx com uma aba "dados" contendo o DataFrame cru (após filtros).
    """
    f = _get_filtros(request)
    cfg = Config()

    # bypass total de cache em export
    if tipo == "poc":
        df = tabela_poc(cfg, f["mes"], f["status"], f["carteira"], nocache=True); fname = "poc_2025.xlsx"
    elif tipo == "success_fee":
        df = tabela_success_fee(cfg, f["mes"], f["status"], f["carteira"], nocache=True); fname = "success_fee_2025.xlsx"
    elif tipo == "produtos":
        df = tabela_produtos(cfg, f["mes"], f["status"], f["carteira"], nocache=True); fname = "produtos_2025.xlsx"
    elif tipo == "pend_formacao":
        df = tabela_pendente_formacao(cfg, f["mes"], f["status"], f["carteira"], nocache=True); fname = "pendente_formacao_2025.xlsx"
    elif tipo == "pend_assinatura":
        df = tabela_pendente_assinatura(cfg, f["mes"], f["status"], f["carteira"], nocache=True); fname = "pendente_assinatura_2025.xlsx"
    elif tipo == "potencial":
        df = tabela_receita_potencial(cfg, f["mes"], f["status"], f["carteira"], nocache=True); fname = "receita_potencial_2025.xlsx"
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
