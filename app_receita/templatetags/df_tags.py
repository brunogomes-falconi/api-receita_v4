from django import template
register = template.Library()

@register.filter
def get_item(row, colname):
    try:
        return row[colname]
    except Exception:
        return ""