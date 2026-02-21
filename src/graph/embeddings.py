import os
from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer
from src.graph.db import db
from dotenv import load_dotenv

load_dotenv()

# Load the model
model = SentenceTransformer('all-MiniLM-L6-v2')

def create_vector_index():
    print("Creating Vector Index in Neo4j...")
    index_query = """
    CREATE VECTOR INDEX node_embeddings IF NOT EXISTS
    FOR (n:CloudEntity)
    ON (n.embedding)
    OPTIONS {indexConfig: {
      `vector.dimensions`: 384,
      `vector.similarity_function`: 'cosine'
    }}
    """
    # Note: CloudEntity is a parent label for all our nodes per ontology design
    # If we don't have it on all nodes, we should add it or create multiple indexes.
    # In our ingest, we didn't add CloudEntity label. Let's add it now to all nodes.
    db.query("MATCH (n) SET n:CloudEntity")
    
    try:
        db.query(index_query)
        print("Vector index created successfully.")
    except Exception as e:
        print(f"Error creating vector index: {e}")

def generate_and_store_embeddings():
    print("Generating embeddings for nodes...")
    
    targets = [
        {"label": "Service", "prop": "ServiceName"},
        {"label": "Resource", "prop": "ResourceName"},
        {"label": "Account", "prop": "BillingAccountName"},
        {"label": "SubAccount", "prop": "SubAccountName"},
        {"label": "Location", "prop": "RegionName"},
        {"label": "Charge", "prop": "chargeDescription"},
        {"label": "Knowledge", "prop": "content"},
        {"label": "Standard", "prop": "name"},
        {"label": "FOCUSColumn", "prop": "name"},
        {"label": "Application", "prop": "name"},
        {"label": "Environment", "prop": "name"},
        {"label": "CostCentre", "prop": "name"},
        {"label": "CostAllocation", "prop": "allocationRuleName"}
    ]
    
    for target in targets:
        label = target["label"]
        prop = target["prop"]
        
        print(f"Processing label: {label}")
        # Use elementId as it is safer for newer Neo4j versions
        nodes = db.query(f"MATCH (n:{label}) WHERE n.{prop} IS NOT NULL RETURN elementId(n) as id, n.{prop} as text")
        
        if not nodes:
            continue
            
        ids = [node["id"] for node in nodes]
        texts = [node["text"] for node in nodes]
        
        # Batch encode
        embeddings = model.encode(texts, batch_size=32, show_progress_bar=True).tolist()
        
        # Batch update using UNWIND
        update_query = """
        UNWIND $data as item
        MATCH (n) WHERE elementId(n) = item.id
        SET n.embedding = item.embedding
        """
        data = [{"id": id_, "embedding": emb} for id_, emb in zip(ids, embeddings)]
        db.query(update_query, {"data": data})
            
    print("Finished generating and storing embeddings.")

if __name__ == "__main__":
    create_vector_index()
    generate_and_store_embeddings()
