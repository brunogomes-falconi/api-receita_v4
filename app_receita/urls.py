from django.urls import path
from . import views

app_name = "app_receita"

urlpatterns = [
    path("", views.resumo, name="resumo"),

    # Abas principais (2025)
    path("receita/", views.receita, name="receita"),  # gráfico cascata (placeholder)
    path("poc/", views.poc, name="poc"),
    path("success-fee/", views.success_fee, name="success_fee"),
    path("produtos/", views.produtos, name="produtos"),
    path("pendente-formacao/", views.pendente_formacao, name="pendente_formacao"),
    path("pendente-assinatura/", views.pendente_assinatura, name="pendente_assinatura"),
    path("receita-potencial/", views.receita_potencial, name="receita_potencial"),
]
