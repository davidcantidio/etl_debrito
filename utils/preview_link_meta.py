# utils/preview_link_utils.py

def ajustar_preview_link(url_anuncio, preview_link_fb):
    """
    Ajusta o preview link para Meta Ads.
    
    Se 'url_anuncio' estiver vazio, retorna 'preview_link_fb'. Caso contrário, retorna 'url_anuncio'.
    
    Args:
        url_anuncio (str): Valor atual da coluna URL_do_Anuncio.
        preview_link_fb (str): Valor da coluna Preview Link FB.
    
    Returns:
        str: O link adequado.
    """
    if not url_anuncio or str(url_anuncio).strip() == "":
        return preview_link_fb if preview_link_fb else ""
    return url_anuncio
