from django import forms
from django.contrib.auth.models import User
from .models import UserProfile, ViewingPermission

# Mesma lista oficial de carteiras que usamos na UI
CARTEIRAS_UI_OFICIAIS = [
    "Agronegócio",
    "América do Norte",
    "Bens Não Duráveis",
    "Infraestrutura e Indústria de Base",
    "MID",
    "Saúde Educação Segurança e Adm.Pública",
    "Servicos e Tecnologia",
]

class ProfileForm(forms.ModelForm):
    class Meta:
        model = UserProfile
        fields = ["email_frequency", "email_report_type", "send_time_brt"]
        widgets = {
            "send_time_brt": forms.TimeInput(format="%H:%M", attrs={"type": "time"}),
        }

class PermissionGrantForm(forms.Form):
    user = forms.ModelChoiceField(
        queryset=User.objects.all().order_by("username"),
        label="Conceder para o usuário",
    )
    portfolio = forms.ChoiceField(
        choices=[(c, c) for c in CARTEIRAS_UI_OFICIAIS],
        label="Carteira",
    )

class PermissionRevokeForm(forms.Form):
    """Form simples para revogar por ID."""
    perm_id = forms.IntegerField(widget=forms.HiddenInput())
