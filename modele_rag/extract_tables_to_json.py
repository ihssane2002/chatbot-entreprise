from pymongo import MongoClient
import json  # important ici

def extract_tables_from_reports(mongo_uri="mongodb://localhost:27017/", db_name="rag_db",
                                 input_collection="knowledge", output_collection="tables"):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    knowledge = db[input_collection]
    tables = db[output_collection]

    # Nettoyer l'ancienne collection de tableaux
    tables.delete_many({})

    table_data = []

    for doc in knowledge.find():  #  Fix ici
        rapport = doc.get("rapport", "unknown")
        for entry in doc.get("contenu", []):
            if entry.get("type") == "table":
                page = entry.get("page", entry.get("pages", [0])[0])

                raw_table = entry.get("content", "")
                if isinstance(raw_table, str):
                    try:
                        table = json.loads(raw_table)
                    except json.JSONDecodeError:
                        print(f"Table invalide pour {rapport}, page {page}")
                        continue
                else:
                    table = raw_table

                rows = []
                header = []
                try:
                    for i in range(len(next(iter(table.values())))):
                        row = []
                        for j in range(len(table)):
                            val = table[str(j)].get(str(i), "").strip()
                            row.append(val)
                        if i == 0:
                            header = row
                        else:
                            rows.append(row)
                except Exception as e:
                    print(f"Erreur dans {rapport} page {page} :", e)
                    continue

                table_data.append({
                    "table_id": f"{rapport}_p{page}_table{len(table_data)}",
                    "rapport": rapport,
                    "page": page,
                    "header": header,
                    "rows": rows
                })

    if table_data:
        tables.insert_many(table_data)
        print(f"{len(table_data)} tableaux extraits et sauvegardés dans MongoDB (collection '{output_collection}').")
    else:
        print("Aucun tableau trouvé.")
