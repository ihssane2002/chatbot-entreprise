import re
import json

def convert_table_to_text(table_input) -> str:
    """
    Transforme un tableau au format dict (ou str JSON) en texte lisible.
    Le tableau est un dict : { "0": {"0": "val", "1": "val"}, "1": {...}, ... }
    Cette fonction gère aussi le cas où input est une chaîne JSON.
    """
    # Si entrée est une chaîne JSON, la parser
    if isinstance(table_input, str):
        try:
            table_dict = json.loads(table_input)
        except json.JSONDecodeError:
            print("convert_table_to_text: erreur de décodage JSON")
            return ""
    elif isinstance(table_input, dict):
        table_dict = table_input
    else:
        print("convert_table_to_text: format inattendu, ni dict ni str JSON")
        return ""

    # Vérifier que table_dict n'est pas vide
    if not table_dict:
        return ""

    # Nombre de colonnes = nombre de clés
    num_cols = len(table_dict)
    try:
        # Nombre de lignes = nombre d'éléments dans la première colonne
        num_rows = len(next(iter(table_dict.values())))
    except Exception:
        print("convert_table_to_text: impossible de déterminer le nombre de lignes")
        return ""

    rows = []
    for i in range(num_rows):
        row = []
        for col in range(num_cols):
            col_str = str(col)
            row_str = str(i)
            col_data = table_dict.get(col_str, {})
            if isinstance(col_data, dict):
                cell = col_data.get(row_str, "")
            else:
                cell = ""
            if isinstance(cell, str):
                cell = cell.replace("\n", " ").strip()
            else:
                cell = str(cell)
            row.append(cell)

        # Supprimer colonnes vides à droite
        while row and row[-1] == "":
            row.pop()

        if any(cell.strip() for cell in row):
            rows.append(" | ".join(row))

    return "\n".join(rows)


def chunk_text(text: str, max_length: int = 500) -> list:
    """
    Découpe un texte en chunks ≤ max_length, en respectant les phrases.
    """
    sentences = re.split(r'(?<=[.?!])\s+', text)
    chunks = []
    current_chunk = ""

    for sentence in sentences:
        if len(current_chunk) + len(sentence) + 1 <= max_length:
            current_chunk += " " + sentence
        else:
            if current_chunk:
                chunks.append(current_chunk.strip())
            current_chunk = sentence
    if current_chunk:
        chunks.append(current_chunk.strip())

    return chunks


def flatten_content(knowledge_base: list) -> list:
    """
    Transforme la base de connaissance en liste de chunks.
    Supporte les textes et les tables (avec la nouvelle structure).
    """
    final_chunks = []

    for doc in knowledge_base:
        report_name = doc.get("rapport", "unknown")
        contenu = doc.get("contenu", [])
        for entry in contenu:
            page = entry.get("page", None)
            content_type = entry.get("type", "")
            content = entry.get("content", "")

            if content_type == "text":
                # Chunker texte
                if not isinstance(content, str):
                    content = json.dumps(content, ensure_ascii=False)
                chunks = chunk_text(content)
                for idx, chunk in enumerate(chunks):
                    final_chunks.append({
                        "rapport": report_name,
                        "page": page,
                        "type": "text",
                        "chunk_id": f"{report_name}_p{page}_text_{idx}",
                        "content": chunk
                    })

            elif content_type == "table":
                try:
                    table_text = convert_table_to_text(content)
                    if table_text.strip():
                        if len(table_text) > 1000:
                            sub_chunks = chunk_text(table_text)
                            for idx, chunk in enumerate(sub_chunks):
                                final_chunks.append({
                                    "rapport": report_name,
                                    "page": page,
                                    "type": "table",
                                    "chunk_id": f"{report_name}_p{page}_table_{idx}",
                                    "content": chunk
                                })
                        else:
                            final_chunks.append({
                                "rapport": report_name,
                                "page": page,
                                "type": "table",
                                "chunk_id": f"{report_name}_p{page}_table",
                                "content": table_text
                            })
                except Exception as e:
                    print(f"Erreur conversion table page {page} : {e}")

    return final_chunks
