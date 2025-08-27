from sentence_transformers import SentenceTransformer

def encode_chunks(chunks):
    """
    Encode la liste de chunks en vecteurs embeddings.
    Retourne une liste de vecteurs (list de listes).
    """
    model = SentenceTransformer("all-MiniLM-L6-v2")
    embeddings = [model.encode(chunk["content"]).tolist() for chunk in chunks]
    return embeddings
