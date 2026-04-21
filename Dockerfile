FROM python:3.10-slim

WORKDIR /app

# Install system deps needed by torch/onnxruntime
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY flights_agent/requirements.txt .

# Install CPU-only torch first so requirements.txt doesn't pull in the GPU version
RUN pip install --no-cache-dir torch==2.2.2+cpu --index-url https://download.pytorch.org/whl/cpu

RUN pip install --no-cache-dir -r requirements.txt

# Copy the agent code
COPY flights_agent/ .

# Copy the dbt models so setup_vectorstore.py can read the schema
COPY flight_analytics_dbt/models/marts/ /app/flight_analytics_dbt/models/marts/

# Pin HuggingFace cache inside /app so appuser can access it after chown
ENV HF_HOME=/app/.cache/huggingface

# Pre-download the embedding model so cold starts don't fetch from HuggingFace
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"

# Bake the ChromaDB vector store into the image at build time — eliminates cold-start delay
RUN python setup_vectorstore.py

# Drop root privileges — chown covers the baked-in chroma_db too
RUN useradd --no-create-home --shell /bin/false appuser && chown -R appuser /app
USER appuser

EXPOSE 8080

CMD streamlit run app.py \
    --server.port=8080 \
    --server.address=0.0.0.0 \
    --server.headless=true
