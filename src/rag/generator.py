import google.generativeai as genai
import os
from dotenv import load_dotenv

load_dotenv()

genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

class CloudGenerator:
    def __init__(self):
        self.model = genai.GenerativeModel('gemini-2.5-flash-lite')

    def generate_answer(self, query, context):
        prompt = f"""
        You are a Cloud Cost Expert (FinOps Practitioner).
        Your task is to answer user questions about cloud costs based on the provided Knowledge Graph context.
        
        The context includes "semantic_match" data and "graph_context" paths which provide provenance (the chain of relationships).
        
        Rules:
        1. Use ONLY the provided context. If the context is missing info, say you don't know.
        2. Provide source-backed explanations. Mention the relationship paths found in the graph (e.g., "Service X is linked to CostRecord Y via relationship Z").
        3. Break down costs and quantities clearly in natural language.
        4. If the user asks for definitions (e.g., FOCUS columns), use the 'Knowledge' or 'Standard' nodes in the context.
        
        Question: {query}
        
        Retrieved Context (from Neo4j with Provenance):
        {context}
        
        Final Answer:
        """
        
        try:
            response = self.model.generate_content(prompt)
            return response.text
        except Exception as e:
            return f"Error in LLM generation: {e}"

generator = CloudGenerator()
