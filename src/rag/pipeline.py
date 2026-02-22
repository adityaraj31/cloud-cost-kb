from src.rag.retriever import retriever
from src.rag.generator import generator

def run_rag_pipeline(query):
    print(f"Pipeline: Processing query -> {query}")
    
    understand_prompt = f"""
    Analyze this cloud cost query: "{query}"
    Extract:
    1. Intent (e.g., Cost Breakdown, Comparison, Optimization, Definition)
    2. Entities (e.g., AWS, Azure, S3, ec2-instance-123)
    3. Timeframe (e.g., July 2024, last month)
    
    Return as a concise JSON string.
    """
    try:
        response = generator.client.chat.completions.create(
            model=generator.model,
            messages=[{"role": "user", "content": understand_prompt}]
        )
        understanding = response.choices[0].message.content
        print(f"Understanding: {understanding}")
    except Exception as e:
        print(f"Failed to understand query: {e}")
        understanding = "{}"

    # 2. Retrieve hybrid context
    findings = retriever.hybrid_retrieve(query)
    
    # 3. Extract concepts and paths for API compliance
    concepts = [f["semantic_match"] for f in findings]
    paths = []
    for f in findings:
        if "graph_context" in f:
            paths.extend([p["path"] for p in f["graph_context"]])
    
    # 4. Generate answer with source-backed explanation
    answer = generator.generate_answer(query, str(findings))
    
    # 5. Calculate a simple confidence score (based on top semantic score)
    top_score = findings[0]["score"] if findings else 0.0
    confidence = round(float(top_score), 2)
    
    return {
        "answer": answer,
        "concepts": concepts,
        "paths": list(set(paths)), # Deduplicate paths
        "confidence": confidence
    }

if __name__ == "__main__":
    test_query = "What is the most expensive AWS service?"
    result = run_rag_pipeline(test_query)
    print("\n--- TEST RESULT ---")
    print(f"Query: {result['query']}")
    print(f"Answer: {result['answer']}")
