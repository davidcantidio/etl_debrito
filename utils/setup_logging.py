import logging

def setup_logging(level=logging.INFO):
    """
    Configura o log do projeto com o nível especificado e um formato padrão.
    
    Parâmetros:
        level (int, opcional): Nível de log (ex: logging.INFO, logging.DEBUG). Padrão: logging.INFO.
    """
    logging.basicConfig(level=level, format='%(levelname)s: %(message)s')
