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
    
    # Target nodes and their descriptive properties
    targets = [
        {"label": "Service", "prop": "ServiceName"},
        {"label": "Resource", "prop": "ResourceName"},
        {"label": "Account", "prop": "BillingAccountName"},
        {"label": "SubAccount", "prop": "SubAccountName"},
        {"label": "Location", "prop": "RegionName"},
        {"label": "Charge", "prop": "chargeDescription"}
    ]
    
    for target in targets:
        label = target["label"]
        prop = target["prop"]
        
        nodes = db.query(f"MATCH (n:{label}) WHERE n.{prop} IS NOT NULL RETURN id(n) as id, n.{prop} as text")
        
        for node in nodes:
            node_id = node["id"]
            text = node["text"]
            
            embedding = model.encode(text).tolist()
            
            db.query(f"MATCH (n) WHERE id(n) = $id SET n.embedding = $embedding", 
                     {"id": node_id, "embedding": embedding})
            
    print("Finished generating and storing embeddings.")

if __name__ == "__main__":
    create_vector_index()
    generate_and_store_embeddings()
