"""
vectorstore.py
--------------
ChromaDB wrapper with persistent local storage.

Collection name : flights_schema
Persist path    : ./chroma_db/  (relative to this file's directory)
Embedding model : all-MiniLM-L6-v2 via sentence-transformers (ChromaDB default)
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import chromadb
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

COLLECTION_NAME = "flights_schema"
_CHROMA_PATH = Path(__file__).parent / "chroma_db"
_EMBEDDING_MODEL = "all-MiniLM-L6-v2"

# ---------------------------------------------------------------------------
# Singleton client / collection
# ---------------------------------------------------------------------------

_client: Optional[chromadb.PersistentClient] = None
_collection: Optional[chromadb.Collection] = None


def _get_collection() -> chromadb.Collection:
    global _client, _collection
    if _collection is None:
        _client = chromadb.PersistentClient(path=str(_CHROMA_PATH))
        embedding_fn = SentenceTransformerEmbeddingFunction(
            model_name=_EMBEDDING_MODEL
        )
        _collection = _client.get_or_create_collection(
            name=COLLECTION_NAME,
            embedding_function=embedding_fn,
            metadata={"hnsw:space": "cosine"},
        )
    return _collection


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def add_documents(docs: list[dict]) -> None:
    """
    Upsert documents into the ChromaDB collection.

    Each dict must contain:
        id       (str)  – unique identifier
        text     (str)  – document text to embed
        metadata (dict) – arbitrary key/value pairs stored alongside the vector

    Upsert semantics mean this is safe to call multiple times without
    creating duplicates.
    """
    collection = _get_collection()

    ids = [d["id"] for d in docs]
    texts = [d["text"] for d in docs]
    metadatas = [d.get("metadata", {}) for d in docs]

    collection.upsert(ids=ids, documents=texts, metadatas=metadatas)


def retrieve(query: str, n_results: int = 4) -> list[str]:
    """
    Return the top-n most semantically similar document texts for *query*.

    Args:
        query:     Natural language question from the user.
        n_results: Number of documents to retrieve (default 4).

    Returns:
        List of document text strings ordered by descending similarity.
    """
    collection = _get_collection()

    # Guard against asking for more results than documents stored
    count = collection.count()
    if count == 0:
        return []
    n = min(n_results, count)

    results = collection.query(query_texts=[query], n_results=n)
    documents = results.get("documents", [[]])[0]
    return documents


def collection_count() -> int:
    """Return the number of documents currently stored in the collection."""
    return _get_collection().count()


def reset_collection() -> None:
    """Delete and recreate the collection (used by setup script for idempotency)."""
    global _client, _collection
    client = _client or chromadb.PersistentClient(path=str(_CHROMA_PATH))
    try:
        client.delete_collection(COLLECTION_NAME)
    except Exception:
        pass
    _collection = None
    _client = client
