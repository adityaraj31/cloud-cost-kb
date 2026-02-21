from sentence_transformers import SentenceTransformer
from src.graph.db import db
import numpy as np

model = SentenceTransformer('all-MiniLM-L6-v2')

class CloudRetriever:
    def __init__(self):
        self.model = model

    def vector_search(self, query_text, limit=5):
        print(f"Performing vector search for: {query_text}")
        query_embedding = self.model.encode(query_text).tolist()
        
        # Cypher query for vector similarity
        # We search across all CloudEntity nodes
        cypher = """
        CALL db.index.vector.queryNodes('node_embeddings', $limit, $embedding)
        YIELD node, score
        RETURN labels(node) as labels, properties(node) as props, score
        """
        results = db.query(cypher, {"limit": limit, "embedding": query_embedding})
        return results

    def get_structured_context(self, node_props, node_label):
        """Get related nodes and relationship paths for provenance."""
        
        if node_label == "Service":
            cypher = """
            MATCH path = (s:Service {ServiceName: $name})<-[:HAS_SERVICE]-(cr:CostRecord)-[:BELONGS_TO_BILLING_ACCOUNT]->(ba:Account)
            RETURN nodes(path) as nodes, [r in relationships(path) | type(r)] as rels LIMIT 5
            """
            params = {"name": node_props.get("ServiceName")}
        elif node_label == "Resource":
            cypher = """
            MATCH path = (r:Resource {ResourceId: $id})<-[:INCURRED_BY]-(cr:CostRecord)-[:HAS_CHARGE]->(ch:Charge)
            RETURN nodes(path) as nodes, [r in relationships(path) | type(r)] as rels LIMIT 5
            """
            params = {"id": node_props.get("ResourceId")}
        elif node_label == "Knowledge":
            cypher = """
            MATCH path = (k:Knowledge {title: $title})
            RETURN nodes(path) as nodes, [] as rels LIMIT 1
            """
            params = {"title": node_props.get("title")}
        elif node_label == "FOCUSColumn":
            cypher = """
            MATCH path = (fc:FOCUSColumn {name: $name})-[r:PART_OF]->(s:Standard)
            RETURN nodes(path) as nodes, [type(r)] as rels LIMIT 1
            """
            params = {"name": node_props.get("name")}
        else:
            cypher = """
            MATCH path = (n)-[r]-(m)
            WHERE id(n) = $id
            RETURN nodes(path) as nodes, [type(r)] as rels LIMIT 5
            """
            params = {"id": node_props.get("id")}
            
        results = db.query(cypher, params)
        formatted_paths = []
        for res in results:
            nodes = res["nodes"]
            rels = res["rels"]
            path_str = ""
            for i in range(len(rels)):
                node_name = nodes[i].get('ServiceName') or nodes[i].get('ResourceId') or nodes[i].get('title') or "Node"
                path_str += f"({node_name}) -[{rels[i]}]-> "
            
            last_node_name = nodes[-1].get('BillingAccountName') or nodes[-1].get('chargeCategory') or nodes[-1].get('name') or "EndNode"
            path_str += f"({last_node_name})"
            formatted_paths.append({"path": path_str, "data": [dict(n) for n in nodes]})
            
        return formatted_paths

    def hybrid_retrieve(self, query):
        # 1. Semantic search
        findings = self.vector_search(query)
        
        context = []
        for finding in findings:
            labels = finding["labels"]
            props = finding["props"]
            score = finding["score"]
            
            # Simple heuristic: if it's a very good match, get its graph context
            if score > 0.6:
                graph_context = self.get_structured_context(props, labels[0] if labels else None)
                context.append({
                    "semantic_match": props,
                    "score": score,
                    "graph_context": graph_context
                })
            else:
                context.append({
                    "semantic_match": props,
                    "score": score
                })
                
        return context

retriever = CloudRetriever()
