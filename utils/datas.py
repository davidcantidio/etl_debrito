# utils/datas.py

from datetime import datetime, date

def transformar_para_date(valor):
    """
    Transforma um valor de data no formato 'YYYY-MM-DD HH:MM:SS' ou
    um objeto datetime em um objeto date (YYYY-MM-DD).

    Parâmetros:
        valor (str ou datetime ou date): Data no formato "YYYY-MM-DD HH:MM:SS"
                                         ou já um objeto datetime ou date.

    Retorna:
        date: objeto da classe date (ex: 2024-04-15)

    Exceções:
        ValueError: Se o valor não puder ser interpretado como data.
    """
    if not valor:
        return None

    # Se já for um objeto date, retorna diretamente
    if isinstance(valor, date) and not isinstance(valor, datetime):
        return valor

    # Se for um objeto datetime, extrai a parte de data
    if isinstance(valor, datetime):
        return valor.date()

    # Se for uma string, tenta converter para datetime com o formato esperado
    if isinstance(valor, str):
        try:
            dt = datetime.strptime(valor, "%Y-%m-%d %H:%M:%S")
            return dt.date()
        except ValueError:
            # Caso a string já esteja no formato "YYYY-MM-DD"
            try:
                dt = datetime.strptime(valor, "%Y-%m-%d")
                return dt.date()
            except ValueError:
                raise ValueError(f"Formato de data não reconhecido: {valor}")
    
    raise ValueError(f"Tipo de valor não suportado: {type(valor)}")
