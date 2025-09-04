from django.urls import path
from . import views

app_name = "app_receita"

urlpatterns = [
    path("", views.resumo, name="resumo"),
    path("receita/", views.receita, name="receita"),
    path("poc/", views.poc, name="poc"),
    path("success-fee/", views.success_fee, name="success_fee"),
    path("produtos/", views.produtos, name="produtos"),
    path("pendente-formacao/", views.pendente_formacao, name="pendente_formacao"),
    path("pendente-assinatura/", views.pendente_assinatura, name="pendente_assinatura"),
    path("receita-potencial/", views.receita_potencial, name="receita_potencial"),

    # NOVAS PÁGINAS DE ESTOQUE
    path("estoque/", views.estoque, name="estoque"),
    path("estoque/detalhes/", views.estoque_detalhes, name="estoque_detalhes"),

    # exportações (inclui novos tipos pend_formacao, pend_assinatura, potencial e estoque)
    path("exportar/<str:tipo>/", views.exportar_excel, name="exportar_excel"),
]
