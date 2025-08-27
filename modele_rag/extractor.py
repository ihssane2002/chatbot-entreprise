import pdfplumber
import camelot
import pandas as pd
import os
import gc  # À mettre en haut du fichier si pas encore importé
import shutil


def fix_vertical_block_cell(df):
    """
    Corrige les cellules fusionnées verticalement (ex: Heures\nMarée\nautomatique…)
    """
    flat_text = str(df.iloc[0, 0])
    lines = [l.strip() for l in flat_text.split("\n") if l.strip()]

    if len(lines) >= 6 and len(lines) % 3 == 0:
        print(" Reconstruction d’un tableau verticalement fusionné...")
        n_cols = len(lines) // 3
        col1, col2, col3 = lines[:n_cols], lines[n_cols:2 * n_cols], lines[2 * n_cols:]
        df = pd.DataFrame({col1[i]: [col2[i], col3[i]] for i in range(n_cols)})
        df = df.T.reset_index()
        df.columns = ['Header', 'Valeur 1', 'Valeur 2']
    return df


def extract_pdf_content(pdf_path):
    all_content = []
    filename = os.path.basename(pdf_path)

    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        for i in range(total_pages):
            page_num = i + 1
            print(f"\n PAGE {page_num} — {filename}")

            page = pdf.pages[i]

            # ➤ Texte brut
            text = page.extract_text()
            if text:
                print(" Texte extrait.")
                all_content.append({
                    "page": page_num,
                    "type": "text",
                    "content": text
                })
            else:
                print(" Aucun texte détecté.")
                all_content.append({
                    "page": page_num,
                    "type": "text",
                    "content": "Aucun texte détecté"
                })

            # ➤ Tableaux avec Camelot (dans un try-except-finally pour nettoyage)
            for flavor in ['lattice', 'stream']:
                tables = None
                try:
                    tables = camelot.read_pdf(pdf_path, pages=str(page_num), flavor=flavor)
                    if tables and len(tables) > 0:
                        for j, table in enumerate(tables):
                            df = table.df
                            if df.shape == (1, 1) and df.iloc[0, 0].count('\n') >= 6:
                                df = fix_vertical_block_cell(df)

                            print(f" Tableau {j + 1} trouvé (flavor={flavor})")
                            all_content.append({
                                "page": page_num,
                                "type": "table",
                                "flavor": flavor,
                                "content": df.to_dict()
                            })
                        break  # Pas besoin d'essayer l'autre flavor si tableau détecté
                except Exception as e:
                    print(f" Erreur tableau page {page_num} (flavor={flavor}): {e}")
                finally:
                    if tables:
                        for t in tables:
                            if hasattr(t, 'tempdir') and t.tempdir and os.path.exists(t.tempdir):
                                try:
                                    gc.collect()  # Forcer Python à libérer les ressources
                                    shutil.rmtree(t.tempdir, ignore_errors=True)
                                except Exception:
                                    pass  # Ignorer l'earreur WinError 32

    return {
        "rapport": filename,
        "contenu": all_content
    }
