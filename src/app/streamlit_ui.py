import streamlit as st
import sys
import os
# Ensure the root directory is in the path so 'src' can be found
if os.getcwd() not in sys.path:
    sys.path.append(os.getcwd())

from src.rag.pipeline import run_rag_pipeline
import pandas as pd
import time

st.set_page_config(page_title="Cloud Cost Knowledge Base", page_icon="☁️", layout="wide")

# Custom CSS for Premium Design
st.markdown("""
<style>
    .main {
        background-color: #0e1117;
    }
    .stTextInput > div > div > input {
        background-color: #262730;
        color: white;
        border-radius: 10px;
    }
    .stButton > button {
        border-radius: 20px;
        background: linear-gradient(45deg, #00c6ff, #0072ff);
        color: white;
        border: none;
        padding: 10px 24px;
        transition: 0.3s;
    }
    .stButton > button:hover {
        transform: scale(1.05);
        box-shadow: 0 0 15px rgba(0, 114, 255, 0.5);
    }
    .context-box {
        background-color: #1e2130;
        padding: 20px;
        border-radius: 15px;
        border-left: 5px solid #00c6ff;
        margin-bottom: 20px;
    }
    .answer-box {
        background-color: #161b22;
        padding: 25px;
        border-radius: 15px;
        border: 1px solid #30363d;
        line-height: 1.6;
    }
</style>
""", unsafe_allow_html=True)

st.title("☁️ Cloud Cost Knowledge Base")
st.markdown("### AI-Powered RAG for FinOps (AWS & Azure)")

query = st.text_input("Ask a question about your cloud costs:", placeholder="e.g., Which are the core FOCUS columns?")

if st.button("Query Knowledge Graph"):
    if query:
        with st.spinner("Analyzing Graph & Generating Answer..."):
            try:
                result = run_rag_pipeline(query)
                
                st.markdown("---")
                
                # Layout
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    st.markdown("### 🤖 AI Response")
                    st.markdown(f'<div class="answer-box">{result["answer"]}</div>', unsafe_allow_html=True)
                    st.metric("Confidence Score", f"{result['confidence']:.2f}")
                
                with col2:
                    st.markdown("### 🔍 Semantic Concepts")
                    for finding in result["concepts"]:
                        name = finding.get("ServiceName") or finding.get("ResourceName") or finding.get("title") or "Entity"
                        st.markdown(f"""
                        <div class="context-box">
                            <strong>{name}</strong><br>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    if result["paths"]:
                        st.markdown("### 🗺️ Provenance Paths")
                        for path in result["paths"]:
                            st.caption(path)
                
                # Show full context in expander
                with st.expander("View RAW Graph Data & Concepts"):
                    st.json(result["concepts"])
                    
            except Exception as e:
                st.error(f"Error: {e}")
    else:
        st.warning("Please enter a question.")

st.sidebar.title("System Info")
st.sidebar.info("""
- **Ontology**: FOCUS 1.0
- **Graph**: Neo4j
- **Embeddings**: all-MiniLM-L6-v2
- **LLM**: Google Gemini Pro
""")

if st.sidebar.button("Refresh Graph Stats"):
    from src.graph.db import db
    node_count = db.query("MATCH (n) RETURN count(n) as count")[0]["count"]
    rel_count = db.query("MATCH ()-[r]->() RETURN count(r) as count")[0]["count"]
    st.sidebar.write(f"Nodes: {node_count}")
    st.sidebar.write(f"Relationships: {rel_count}")
