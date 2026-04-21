from __future__ import annotations

from pathlib import Path
from typing import Optional

import chromadb
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction

COLLECTION_NAME = "flights_schema"
_CHROMA_PATH = Path(__file__).parent / "chroma_db"
_EMBEDDING_MODEL = "all-MiniLM-L6-v2"

_client: Optional[chromadb.PersistentClient] = None
_collection: Optional[chromadb.Collection] = None


def _get_collection() -> chromadb.Collection:
    global _client, _collection
    if _collection is None:
        _client = chromadb.PersistentClient(path=str(_CHROMA_PATH))
        embedding_fn = SentenceTransformerEmbeddingFunction(model_name=_EMBEDDING_MODEL)
        _collection = _client.get_or_create_collection(
            name=COLLECTION_NAME,
            embedding_function=embedding_fn,
            metadata={"hnsw:space": "cosine"},
        )
    return _collection


def add_documents(docs: list[dict]) -> None:
    """Upsert documents into the collection. Each dict needs id, text, metadata."""
    collection = _get_collection()
    collection.upsert(
        ids=[d["id"] for d in docs],
        documents=[d["text"] for d in docs],
        metadatas=[d.get("metadata", {}) for d in docs],
    )


def retrieve(query: str, n_results: int = 4) -> list[str]:
    """Return top-n most similar document texts for query."""
    collection = _get_collection()
    count = collection.count()
    if count == 0:
        return []
    n = min(n_results, count)
    results = collection.query(query_texts=[query], n_results=n)
    return results.get("documents", [[]])[0]


def collection_count() -> int:
    return _get_collection().count()


def reset_collection() -> None:
    global _client, _collection
    client = _client or chromadb.PersistentClient(path=str(_CHROMA_PATH))
    try:
        client.delete_collection(COLLECTION_NAME)
    except Exception:
        pass
    _collection = None
    _client = client
