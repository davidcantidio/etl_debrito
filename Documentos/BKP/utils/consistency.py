# utils/consistency.py
import logging

def verify_impressions_match(df_ref, df_cmp, ref_name, cmp_name):
    """
    Compara a soma da coluna 'Impressoes' entre dois DataFrames e emite
    um log com 🟢 se baterem ou 🔴 se divergirem.
    """
    tot_ref = df_ref['Impressoes'].sum()
    tot_cmp = df_cmp['Impressoes'].sum()
    if tot_ref == tot_cmp:
        logging.info(f"🟢 IMPRESSÕES IGUAIS: {ref_name} ({tot_ref}) == {cmp_name} ({tot_cmp})")
    else:
        logging.warning(f"🔴 IMPRESSÕES DIFERENTES: {ref_name} ({tot_ref}) != {cmp_name} ({tot_cmp})")
