from src.graph.db import db

def setup_schema():
    print("Setting up Neo4j Schema Constraints...")
    
    constraints = [
        # Uniqueness constraints based on FOCUS spec
        "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Account) REQUIRE a.BillingAccountId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (s:SubAccount) REQUIRE s.SubAccountId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (r:Resource) REQUIRE r.ResourceId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (sv:Service) REQUIRE sv.ServiceName IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (loc:Location) REQUIRE loc.RegionId IS UNIQUE",
        
        # Performance indexes
        "CREATE INDEX IF NOT EXISTS FOR (cr:CostRecord) ON (cr.ChargePeriodStart)",
        "CREATE INDEX IF NOT EXISTS FOR (ch:Charge) ON (ch.chargeCategory)"
    ]
    
    for cmd in constraints:
        try:
            db.query(cmd)
            print(f"Applied: {cmd}")
        except Exception as e:
            print(f"Failed to apply {cmd}: {e}")

if __name__ == "__main__":
    setup_schema()
