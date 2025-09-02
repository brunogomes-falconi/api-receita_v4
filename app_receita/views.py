from django.shortcuts import render

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