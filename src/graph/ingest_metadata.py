import sys
import os
sys.path.append(os.getcwd())

from src.graph.db import db

def ingest_metadata():
    print("Ingesting FOCUS Metadata and Business Rules...")
    
    # 1. Standard and Columns
    db.query("""
        MERGE (s:Standard {name: 'FOCUS', version: '1.0'})
        MERGE (fc1:FOCUSColumn {name: 'EffectiveCost', description: 'Total cost including amortized commitment discounts.'})
        MERGE (fc2:FOCUSColumn {name: 'BilledCost', description: 'The amount appearing on the physical invoice.'})
        MERGE (fc3:FOCUSColumn {name: 'CommitmentDiscountQuantity', description: 'Quantity of commitment discount used.'})
        
        MERGE (fc1)-[:PART_OF]->(s)
        MERGE (fc2)-[:PART_OF]->(s)
        MERGE (fc3)-[:PART_OF]->(s)
    """)
    
    # 2. FAQ / Knowledge Nodes (to answer Part E prompts)
    knowledge_nodes = [
        {
            "title": "FOCUS vs Vendor Columns",
            "content": "FOCUS columns are standardized across providers (e.g., BillingAccountId), while vendor-specific columns (x_*) store unique attributes like AWS UsageType or Azure MeterCategory."
        },
        {
            "title": "Commitment Double Counting",
            "content": "To avoid double counting when using CommitmentDiscountQuantity, exclude 'Purchase' charge categories and focus on 'Usage' and 'Credit' to see the net effect."
        },
        {
            "title": "Usage and Commitment Totals",
            "content": "Total increases when including commitment purchases and usage together because 'Purchase' represents the upfront commitment cost, while 'Usage' reflects the consumption. In FOCUS, EffectiveCost should be used to see the amortized view."
        },
        {
            "title": "Cloud Spend Analysis Type",
            "content": "EffectiveCost is the recommended cost type for analyzing cloud spend as it accounts for amortized commitments and reflects true economic consumption."
        }
    ]
    
    for kn in knowledge_nodes:
        db.query("""
            MERGE (k:Knowledge {title: $title})
            SET k.content = $content
        """, kn)

    print("Metadata ingestion complete.")

if __name__ == "__main__":
    ingest_metadata()
