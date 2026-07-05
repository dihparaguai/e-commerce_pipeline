from datetime import datetime
from zoneinfo import ZoneInfo

def get_current_date_str() -> str:
    """
    Retorna a data atual no formato 'YYYY-MM-DD' sob o fuso horário 'America/Sao_Paulo'.
    """
    return datetime.now(ZoneInfo("America/Sao_Paulo")).date().isoformat()
