import os
import json
import hashlib
from pymongo import MongoClient
import gridfs
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct
from sentence_transformers import SentenceTransformer
from modele_rag.extractor import extract_pdf_content
from modele_rag.chunker import flatten_content
from modele_rag.extract_tables_to_json import extract_tables_from_reports
import sys
print("Script Python démarré", file=sys.stderr)


# === Configuration ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
pdf_dir = os.path.join(BASE_DIR, "modele_rag", "pdfs")
print("Contenu du dossier 'pdfs' :", os.listdir(pdf_dir))

collection_name = "rag_chunks"

# === Connexion MongoDB ===
client = MongoClient("mongodb://localhost:27017/")
db = client["rag_db"]
knowledge_col = db["knowledge"]
chunks_col = db["chunks"]
tables_col = db["tables"]
fs = gridfs.GridFS(db)

# === Connexion Qdrant ===
qdrant = QdrantClient(
    url="https://b8c7f3c1-27cc-4915-bcc9-39dd4a4dae1f.eu-west-1-0.aws.cloud.qdrant.io",
    api_key="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOiJtIn0.F59-t7r7BRWp_SpOU9IIGKVrRz-mzeFSBQIV7ulCGVY"
)

# === Modèle d'encodage ===
encoder = SentenceTransformer("all-MiniLM-L6-v2")

def report_already_extracted(pdf_name, current_hash):
    existing = knowledge_col.find_one({"rapport": pdf_name})
    return existing and existing.get("file_hash") == current_hash

def compute_file_hash(path, algo="md5"):
    h = hashlib.new(algo)
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def store_pdf_in_gridfs(pdf_path, pdf_name):
    with open(pdf_path, "rb") as f:
        # Supprimer ancienne version dans GridFS si existe
        if fs.exists({"filename": pdf_name}):
            fs.delete(fs.find_one({"filename": pdf_name})._id)
        fs.put(f, filename=pdf_name)


def validate_and_clean_report(report_data, pdf_name):
    if not isinstance(report_data, dict):
        raise ValueError("Le rapport n'est pas un dictionnaire.")
    report_data["rapport"] = report_data.get("rapport", pdf_name)
    cleaned_content = []
    for entry in report_data.get("contenu", []):
        if isinstance(entry, dict):
            if not isinstance(entry.get("content"), str):
                entry["content"] = json.dumps(entry["content"], ensure_ascii=False)
            cleaned_content.append(entry)
    report_data["contenu"] = cleaned_content
    return report_data


def main():
    print("Démarrage du pipeline MongoDB + GridFS + Qdrant...")

    existing_docs = list(knowledge_col.find({}))
    print(f"{len(existing_docs)} document(s) trouvés dans MongoDB")

    pdf_files = [f for f in os.listdir(pdf_dir) if f.endswith(".pdf")]
    existing_names = {doc.get("rapport", "") for doc in existing_docs}

    # On crée un dict pour retrouver le hash stocké en base par fichier
    hash_in_db = {doc.get("rapport", ""): doc.get("file_hash", "") for doc in existing_docs}

    updated = False

    # Suppression des documents absents du dossier local
    missing_files = existing_names - set(pdf_files)
    if missing_files:
        knowledge_col.delete_many({"rapport": {"$in": list(missing_files)}})
        for name in missing_files:
            f = fs.find_one({"filename": name})
            if f:
                fs.delete(f._id)
        print(f"{len(missing_files)} rapport(s) supprimé(s) car absents du dossier.")
        updated = True

    for pdf_name in pdf_files:
        full_path = os.path.join(pdf_dir, pdf_name)
        current_hash = compute_file_hash(full_path)

        # Vérifier si le fichier est déjà en base avec même hash
        if pdf_name in existing_names and hash_in_db.get(pdf_name, "") == current_hash:
            print(f"{pdf_name} déjà présent et inchangé, aucun traitement.")
            continue  # passer au suivant sans retraiter

        print(f"Traitement : {pdf_name}")

        try:
            store_pdf_in_gridfs(full_path, pdf_name)
            if not report_already_extracted(pdf_name, current_hash):
                report = extract_pdf_content(full_path)
                report = validate_and_clean_report(report, pdf_name)
                report["file_hash"] = current_hash
                knowledge_col.delete_many({"rapport": pdf_name})
                knowledge_col.insert_one(report)
                updated = True
            else:
                print(f"{pdf_name} déjà traité, aucune mise à jour.")

            report = validate_and_clean_report(report, pdf_name)
            report["file_hash"] = current_hash  # stocker le hash dans le document
            knowledge_col.delete_many({"rapport": pdf_name})
            knowledge_col.insert_one(report)
            updated = True
        except Exception as e:
            print(f"Erreur pour {pdf_name} : {e}")

    if not updated:
        print("Aucun fichier nouveau ou modifié. La base de connaissance est à jour.")
        return

    print("Insertion des documents terminée dans MongoDB")

    print("Extraction des tableaux...")
    extract_tables_from_reports(
        mongo_uri="mongodb://localhost:27017/",
        db_name="rag_db",
        input_collection="knowledge",
        output_collection="tables"
    )

    print("Génération des chunks...")
    full_knowledge = list(knowledge_col.find({}))
    chunks = flatten_content(full_knowledge)

    for chunk in chunks:
        if not isinstance(chunk.get("content"), str):
            chunk["content"] = json.dumps(chunk["content"], ensure_ascii=False)

    chunks_col.delete_many({})
    chunks_col.insert_many(chunks)
    print(f"{len(chunks)} chunk(s) inséré(s) dans 'chunks'")

    print("Nettoyage et recréation de la collection Qdrant...")
    if qdrant.collection_exists(collection_name=collection_name):
        qdrant.delete_collection(collection_name=collection_name)

    qdrant.create_collection(
        collection_name=collection_name,
        vectors_config=VectorParams(size=384, distance=Distance.COSINE)
    )

    print("Insertion des vecteurs dans Qdrant...")
    points = []
    for i, chunk in enumerate(chunks):
        try:
            content = chunk["content"]
            embedding = encoder.encode(content).tolist()
            points.append(PointStruct(
                id=i,
                vector=embedding,
                payload={
                    "chunk_id": chunk.get("id", i),
                    "rapport": chunk.get("rapport", ""),
                    "page": chunk.get("page", None),
                    "content": content
                }
            ))
        except Exception as e:
            print(f"Chunk {i} : échec d'encodage : {e}")

    # Pour éviter timeout, insertion par batch (exemple batch=100)
    BATCH_SIZE = 100
    for start in range(0, len(points), BATCH_SIZE):
        batch = points[start:start+BATCH_SIZE]
        qdrant.upsert(collection_name=collection_name, points=batch)
        print(f"Batch {start} à {start+len(batch)} inséré dans Qdrant.")

    print(f"{len(points)} vecteurs insérés dans Qdrant")

    print("\nAperçu des premiers chunks insérés dans Qdrant :")
    for i, pt in enumerate(points[:10]):
        print(f"\nChunk {i + 1}")
        print(f"Rapport : {pt.payload['rapport']}")
        print(f"Page : {pt.payload.get('page', '-')}")
        print(f"Longueur contenu : {len(pt.payload['content'])} caractères")
        print(f"Contenu :\n{pt.payload['content'][:500]}...")
        print("-" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Erreur fatale : {e}")
