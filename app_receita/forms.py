from __future__ import annotations

from django import forms
from django.contrib.auth import get_user_model
from .models import UserProfile, ViewingPermission

User = get_user_model()


class ProfileForm(forms.ModelForm):
    """
    Edição de preferências de e-mail e horário.
    Usamos fields='__all__' + exclude para evitar FieldError em import-time,
    caso os campos novos ainda não tenham sido migrados.
    """
    class Meta:
        model = UserProfile
        fields = "__all__"
        exclude = ["user", "role"]  # usuário não edita estes dois

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Aplica widget de hora se o campo existir
        if "email_send_time" in self.fields:
            self.fields["email_send_time"].widget = forms.TimeInput(
                format="%H:%M",
                attrs={"type": "time"}
            )


class PermissionGrantForm(forms.Form):
    """
    VP/Diretor: conceder visualização de uma carteira a outro usuário.
    'carteiras_choices' são injetadas no __init__ (lista oficial).
    """
    grantee = forms.ModelChoiceField(
        queryset=User.objects.all().order_by("username"),
        label="Usuário",
        help_text="Usuário que receberá permissão de visualização.",
    )
    carteira = forms.ChoiceField(
        choices=[],
        label="Carteira",
        help_text="Carteira a ser concedida.",
    )

    def __init__(self, *args, **kwargs):
        carteiras_choices = kwargs.pop("carteiras_choices", [])
        super().__init__(*args, **kwargs)
        self.fields["carteira"].choices = [(c, c) for c in carteiras_choices]


class PermissionRevokeForm(forms.Form):
    """
    Formulário simples para revogar uma ou mais permissões concedidas.
    O template renderiza uma checkbox por permissão e envia os IDs selecionados.
    """
    revoke_ids = forms.CharField(widget=forms.HiddenInput, required=False)
