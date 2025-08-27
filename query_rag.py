import sys
import json
import os
import requests
import io
import re
import urllib.parse
import time
from sentence_transformers import SentenceTransformer, CrossEncoder  # Ajout du reranker
from pymongo import MongoClient
from qdrant_client import QdrantClient
from qdrant_client.http.models import SearchParams
import os
from dotenv import load_dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# === Config Qdrant Cloud ===


# Charger les variables depuis .env
load_dotenv()

QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
QDRANT_URL = os.getenv("QDRANT_URL")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL")


# === Connexions ===
model = SentenceTransformer("all-MiniLM-L6-v2")
reranker = CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2')

qdrant_client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
client = MongoClient("mongodb://localhost:27017/")
db = client["rag_db"]
tables_col = db["tables"]
all_tables = list(tables_col.find({}))

def search_chunks(question, k=50):
    vector = model.encode(question).tolist()
    hits = qdrant_client.search(
        collection_name=COLLECTION_NAME,
        query_vector=vector,
        limit=k,
        with_payload=True,
        search_params=SearchParams(hnsw_ef=128)
    )
    candidates = [hit.payload for hit in hits]
    texts = [(question, c['content']) for c in candidates if 'content' in c]
    scores = reranker.predict(texts)
    reranked = sorted(zip(scores, candidates), key=lambda x: x[0], reverse=True)
    return [c for _, c in reranked[:10]]

def search_tables(question, limit=5):
    keywords = set(question.lower().split())
    table_scores = []
    for table in all_tables:
        text = " ".join(table["header"] + [cell for row in table["rows"] for cell in row]).lower()
        score = sum(1 for word in keywords if word in text)
        if score > 0:
            table_scores.append((score, table))
    table_scores.sort(key=lambda x: x[0], reverse=True)
    merged_results = []
    seen = set()
    for _, table in table_scores[:limit]:
        key = (table["rapport"], tuple(table["header"]))
        if key in seen:
            continue
        seen.add(key)
        full_rows = []
        for t in all_tables:
            if (
                t["rapport"] == table["rapport"]
                and t["header"] == table["header"]
                and abs(t["page"] - table["page"]) <= 2
            ):
                full_rows.extend(t["rows"])
        merged_results.append({
            "rapport": table["rapport"],
            "page": table["page"],
            "header": table["header"],
            "rows": full_rows
        })
    return merged_results

def format_markdown_table(header, rows):
    md = "| " + " | ".join(header) + " |\n"
    md += "| " + " | ".join(["---"] * len(header)) + " |\n"
    for row in rows[:10]:
        md += "| " + " | ".join(row) + " |\n"
    return md

def question_est_comparative(question):
    mots = ["compar", "différence", "différencier", "vs", "contre", "meilleur", "par rapport", "différences"]
    return any(mot in question.lower() for mot in mots)

def build_prompt(text_chunks, table_matches, question, history):
    grouped_contexts = {}
    for chunk in text_chunks:
        rapport = chunk.get("rapport", "Rapport inconnu")
        grouped_contexts.setdefault(rapport, []).append(chunk.get("content", ""))

    prompt = (
        "Tu es un expert en analyse technique des rapports de l’ANP (Agence Nationale des Ports).\n"
        "Tu dois répondre de façon claire, complète, bien structurée, uniquement en **langue française**.\n"
        "- N’utilise que les extraits fournis (textes + tableaux), sans rien inventer.\n"
        "- Les liens PDF apparaissent à la fin uniquement.\n\n"
    )

    if history:
        prompt += "### Historique de la conversation :\n"
        for item in history:
            prompt += f"- Q: {item['question']}\n  A: {item['answer']}\n"
        prompt += "\n"

    for rapport, contents in grouped_contexts.items():
        prompt += f"### Extraits du rapport : {rapport}\n"
        prompt += "\n\n".join(contents) + "\n\n"

    if table_matches:
        prompt += "### Tableaux extraits :\n"
        for table in table_matches:
            nom_rapport = table.get("rapport", "?")
            page = table.get("page", "?")
            prompt += f"**Rapport : {nom_rapport} | Page : {page}**\n"
            prompt += format_markdown_table(table["header"], table["rows"]) + "\n\n"

    prompt += (
        "Tu es un expert assistant en analyse de documents techniques extraits de PDF.\n\n"
        "---\n"
        f"**Question posée :**\n{question}\n"
        "---\n"
        "**Consignes strictes pour générer ta réponse :**\n"
        "- Réponds de manière complète, rigoureuse et bien structurée (utilise des paragraphes, titres, listes ou tableaux si nécessaire).\n"
        "- Ne copie pas la question dans la réponse.\n"
        "- N’utilise que les extraits fournis (textes + tableaux), sans rien inventer.\n"
        "- Si aucune information pertinente n’est trouvée, indique-le clairement.\n"
        "- N’inclus aucun lien ni référence extérieure.\n"
        "- Utilise un langage professionnel clair, précis, sans ambiguïté.\n"
        "- La réponse doit comporter un **minimum de 900 mots** si la question le justifie.\n"
        "- Sépare les différentes parties avec des lignes vides pour la lisibilité.\n"
    )
    if question_est_comparative(question):
        prompt += (
            "\n### Instructions supplémentaires pour une question comparative :\n\n"
            "- Identifie les **éléments comparables** dans les textes ou tableaux fournis.\n"
            "- Présente un **tableau comparatif clair** :\n"
            "   - Lignes = critères de comparaison\n"
            "   - Colonnes = noms des rapports\n"
            "- Rédige ensuite une **analyse des différences** : explique chaque point notable.\n"
            "- Termine par une **synthèse comparative** ou une **recommandation finale**, basée sur l’interprétation des données.\n"
            "- Utilise un titre comme ## Comparaison suivi de ## Conclusion comparative pour séparer les sections.\n"
        )
    return prompt

def call_groq(prompt, retries=3, backoff=2):
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": "Tu es un expert analyste technique."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.4,
        "max_tokens": 2000
    }
    for i in range(retries):
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            return response.json()["choices"][0]["message"]["content"]
        elif response.status_code in [502, 503, 504]:
            time.sleep(backoff * (2 ** i))
        else:
            raise Exception(f"Erreur Groq: {response.status_code} - {response.text}")
    return "Le service Groq est temporairement indisponible."

def ajouter_section_rapports_utilises(answer_text, chunks, tables):
    base_url = "http://localhost:5000/static/rapports/"
    rapports_chunks = {chunk.get("rapport") for chunk in chunks if "rapport" in chunk}
    rapports_tables = {table.get("rapport") for table in tables if "rapport" in table}
    rapports_utilises = rapports_chunks.union(rapports_tables)
    rapports_utilises = {nom.strip() for nom in rapports_utilises if nom}
    if not rapports_utilises:
        return answer_text.strip()
    liens = [f"[{nom}]({base_url}{urllib.parse.quote(nom)})" for nom in sorted(rapports_utilises)]
    section = "\n\n---\n**Rapports utilisés :**\n" + "\n".join(f"- {l}" for l in liens)
    return answer_text.strip() + section

def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Aucune question fournie"}, ensure_ascii=False))
        sys.exit(0)

    question = sys.argv[1].strip()
    history_json = sys.argv[2] if len(sys.argv) > 2 else "[]"
    try:
        history = json.loads(history_json)
    except:
        history = []
    try:
        chunks = search_chunks(question)
        tables = search_tables(question)
        prompt = build_prompt(chunks, tables, question, history)
        answer = call_groq(prompt)
        answer = ajouter_section_rapports_utilises(answer, chunks, tables)
        print(json.dumps({"answer": answer}, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({"error": f"Erreur interne : {str(e)}"}, ensure_ascii=False))
        sys.exit(0)

if __name__ == "__main__":
    main()