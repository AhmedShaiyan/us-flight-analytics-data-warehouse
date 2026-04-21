

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv

load_dotenv()

import embedder
import schema_loader
import vectorstore


def main() -> None:
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "")
    dataset = os.getenv("BQ_DATASET", "marts")

    if not project_id:
        print(
            "WARNING: GOOGLE_CLOUD_PROJECT is not set in .env.  "
            "Table references in embedded documents will use an empty project ID.  "
            "Set the variable and re-run this script before using the app."
        )

    print("=" * 60)
    print("Flights Data Assistant — Vector Store Setup")
    print("=" * 60)
    print(f"  GCP Project : {project_id or '(not set)'}")
    print(f"  BQ Dataset  : {dataset}")
    print()

    print("Step 1/3  Loading mart schemas from dbt models…")
    models_dir = Path(__file__).parent.parent / "flight_analytics_dbt" / "models"
    schemas = schema_loader.load_mart_schemas(
        models_dir=models_dir,
        project_id=project_id,
        dataset=dataset,
    )
    print(f"          Found {len(schemas)} mart model(s):")
    for s in schemas:
        print(f"            • {s.table_name}  ({len(s.columns)} columns)")
    print()

    print("Step 2/3  Building documents (schema + example Q&A pairs)…")
    docs = embedder.build_documents(
        schemas=schemas,
        project_id=project_id,
        dataset=dataset,
    )
    schema_docs = [d for d in docs if d["metadata"].get("type") == "table_schema"]
    example_docs = [d for d in docs if d["metadata"].get("type") == "example_qa"]
    print(f"          {len(schema_docs)} schema document(s)")
    print(f"          {len(example_docs)} example Q&A document(s)")
    print(f"          {len(docs)} total documents to embed")
    print()

    print("Step 3/3  Embedding documents into ChromaDB…")
    print("          (downloading all-MiniLM-L6-v2 on first run — this may take a minute)")
    vectorstore.add_documents(docs)
    final_count = vectorstore.collection_count()
    print(f"          Done.  Collection now contains {final_count} document(s).")
    print()
    print("Setup complete.  Start the app with:")
    print("    streamlit run app.py")
    print()


if __name__ == "__main__":
    main()
