from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import sys
import os
# Ensure the root directory is in the path so 'src' can be found
if os.getcwd() not in sys.path:
    sys.path.append(os.getcwd())

from src.rag.pipeline import run_rag_pipeline
from src.graph.db import db
import uvicorn

from neo4j.graph import Node, Relationship

app = FastAPI(title="Cloud Cost Knowledge Base API", version="1.0.0")

def serialize_neo4j(obj):
    if isinstance(obj, list):
        return [serialize_neo4j(i) for i in obj]
    if isinstance(obj, dict):
        return {k: serialize_neo4j(v) for k, v in obj.items()}
    # Check for Record type safely
    if hasattr(obj, "data") and callable(obj.data):
        return serialize_neo4j(obj.data())
    if isinstance(obj, Node):
        return dict(obj)
    if isinstance(obj, Relationship):
        return dict(obj)
    return obj

class QueryRequest(BaseModel):
    question: str

class QueryResponse(BaseModel):
    answer: str
    concepts: list
    paths: list
    confidence: float

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/stats")
def get_stats():
    try:
        node_count = db.query("MATCH (n) RETURN count(n) as count")[0]["count"]
        rel_count = db.query("MATCH ()-[r]->() RETURN count(r) as count")[0]["count"]
        # Check index status
        indices = db.query("SHOW INDEXES")
        index_status = "active" if indices else "none"
        return {
            "total_nodes": node_count,
            "total_relationships": rel_count,
            "index_status": index_status
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/concept/{name}")
def get_concept(name: str):
    try:
        # Search for knowledge or columns by name
        cypher = """
        MATCH (n)
        WHERE (n:Knowledge AND n.title CONTAINS $name) 
           OR (n:FOCUSColumn AND n.name CONTAINS $name)
           OR (n:Standard AND n.name CONTAINS $name)
        RETURN labels(n) as labels, properties(n) as props, id(n) as id, 1.0 as score
        LIMIT 5
        """
        results = db.query(cypher, {"name": name})
        
        if not results:
            # Fallback to vector search if exact match fails
            from src.rag.retriever import retriever
            vector_results = retriever.vector_search(name, limit=3)
            return serialize_neo4j(vector_results)
            
        return serialize_neo4j(results)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query", response_model=QueryResponse)
def query_kb(request: QueryRequest):
    try:
        print(f"API received question: {request.question}")
        result = run_rag_pipeline(request.question)
        return {
            "answer": result["answer"],
            "concepts": serialize_neo4j(result["concepts"]),
            "paths": result["paths"],
            "confidence": result["confidence"]
        }
    except Exception as e:
        print(f"Error in API /query: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
