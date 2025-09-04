from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone

# Papéis previstos no escopo
ROLE_CHOICES = [
    ("ADMIN", "Administrador"),
    ("CEO", "CEO"),
    ("CFO", "CFO"),
    ("CTRL", "Controladoria"),
    ("VP", "VP"),
    ("DIR", "Diretor"),
    ("USER", "Usuário"),
]

EMAIL_FREQ_CHOICES = [
    ("none", "Não receber"),
    ("daily", "Diário"),
    ("weekly", "Semanal"),
    ("monthly", "Mensal"),
]

EMAIL_TYPE_CHOICES = [
    ("exec", "Resumo executivo"),
    ("detail", "Análises detalhadas"),
]

class UserProfile(models.Model):
    ROLE_CHOICES = [
        ("FUNC", "Funcionário"),
        ("VP", "VP"),
        ("DIR", "Diretor"),
    ]
    EMAIL_SUMMARY_TYPE_CHOICES = [
        ("poc", "Resumo PoC"),
        ("sf", "Resumo Success Fee"),
        ("prod", "Resumo Produtos"),
        ("geral", "Resumo Geral"),
        ("off", "Não enviar"),
    ]
    EMAIL_FREQ_CHOICES = [
        ("daily", "Diário"),
        ("weekly", "Semanal"),
        ("monthly", "Mensal"),
    ]

    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name="profile")
    role = models.CharField(max_length=8, choices=ROLE_CHOICES, default="FUNC")

    # Estes são os campos que o form usa:
    email_summary_type = models.CharField(max_length=16, choices=EMAIL_SUMMARY_TYPE_CHOICES, default="geral")
    email_frequency = models.CharField(max_length=16, choices=EMAIL_FREQ_CHOICES, default="daily")
    from datetime import time
    email_send_time = models.TimeField(default=time(hour=8, minute=0))

class ViewingPermission(models.Model):
    """
    Permissão de visualização por carteira, concedida a um usuário (grantee) por outro (granted_by).
    A carteira é guardada como string (label de UI), seguindo a taxonomia oficial do projeto.
    """
    grantee = models.ForeignKey(User, on_delete=models.CASCADE, related_name="view_perms")
    portfolio = models.CharField(max_length=120)  # ex.: "América do Norte"
    granted_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="granted_perms")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ("grantee", "portfolio")

    def __str__(self):
        return f"{self.grantee.username} -> {self.portfolio}"

# Sinal para criar o profile automaticamente
from django.db.models.signals import post_save
from django.dispatch import receiver

@receiver(post_save, sender=User)
def create_or_update_profile(sender, instance, created, **kwargs):
    if created:
        UserProfile.objects.create(user=instance)
    else:
        # garante que exista
        UserProfile.objects.get_or_create(user=instance)
