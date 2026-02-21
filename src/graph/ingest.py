import pandas as pd
import numpy as np
from src.graph.db import db
from datetime import datetime

def clean_value(val):
    if pd.isna(val) or val == 'nan':
        return None
    return val

def get_col(row, col_name):
    """Case-insensitive column getter."""
    for actual_col in row.index:
        if str(actual_col).lower() == str(col_name).lower():
            return row[actual_col]
    return None

def ingest_data(file_path, cloud_provider):
    print(f"Ingesting {cloud_provider} data from {file_path}...")
    df = pd.read_excel(file_path)
    
    for _, row in df.iterrows():
        # 1. Create/Update Account Nodes
        billing_account_id = clean_value(get_col(row, 'BillingAccountId'))
        billing_account_name = clean_value(get_col(row, 'BillingAccountName'))
        
        if billing_account_id:
            db.query("""
                MERGE (a:Account {BillingAccountId: $id})
                SET a.BillingAccountName = $name, a.Type = 'BillingAccount'
            """, {"id": str(billing_account_id), "name": billing_account_name})

        sub_account_id = clean_value(get_col(row, 'SubAccountId'))
        sub_account_name = clean_value(get_col(row, 'SubAccountName'))
        
        if sub_account_id:
            db.query("""
                MERGE (s:SubAccount {SubAccountId: $id})
                SET s.SubAccountName = $name
            """, {"id": str(sub_account_id), "name": sub_account_name})

        # 2. Create Service Node
        service_name = clean_value(get_col(row, 'ServiceName'))
        service_category = clean_value(get_col(row, 'ServiceCategory'))
        if service_name:
            db.query("""
                MERGE (s:Service {ServiceName: $name})
                SET s.ServiceCategory = $category
            """, {"name": service_name, "category": service_category})

        # 3. Create Resource Node
        resource_id = clean_value(get_col(row, 'ResourceId'))
        resource_name = clean_value(get_col(row, 'ResourceName'))
        resource_type = clean_value(get_col(row, 'ResourceType'))
        if resource_id:
            db.query("""
                MERGE (r:Resource {ResourceId: $id})
                SET r.ResourceName = $name, r.ResourceType = $type
            """, {"id": str(resource_id), "name": resource_name, "type": resource_type})

        # 4. Create Location Node
        region_id = clean_value(get_col(row, 'RegionId'))
        region_name = clean_value(get_col(row, 'RegionName'))
        if region_id:
            db.query("""
                MERGE (l:Location {RegionId: $id})
                SET l.RegionName = $name
            """, {"id": str(region_id), "name": region_name})

        # 5. Create Charge Node 
        charge_category = clean_value(get_col(row, 'ChargeCategory'))
        charge_class = clean_value(get_col(row, 'ChargeClass'))
        charge_desc = clean_value(get_col(row, 'ChargeDescription'))
        if charge_desc:
            db.query("""
                MERGE (ch:Charge {chargeDescription: $desc})
                SET ch.chargeCategory = $category, ch.chargeClass = $class
            """, {"desc": charge_desc, "category": charge_category, "class": charge_class})
        
        # 6. Create CostRecord Node and Relationships
        billed_cost = float(get_col(row, 'BilledCost') or 0)
        effective_cost = float(get_col(row, 'EffectiveCost') or billed_cost)
        currency = clean_value(get_col(row, 'Currency'))
        
        record_id = f"{cloud_provider}_{_}" 
        
        db.query("""
            CREATE (cr:CostRecord {
                RecordId: $record_id,
                BilledCost: $billed_cost,
                EffectiveCost: $effective_cost,
                Currency: $currency,
                Provider: $provider
            })
            WITH cr
            MATCH (a:Account {BillingAccountId: $billing_id})
            MERGE (cr)-[:BELONGS_TO_BILLING_ACCOUNT]->(a)
            
            WITH cr
            MATCH (s:Service {ServiceName: $service_name})
            MERGE (cr)-[:HAS_SERVICE]->(s)
            
            WITH cr
            MATCH (r:Resource {ResourceId: $resource_id})
            MERGE (cr)-[:INCURRED_BY]->(r)
            
            WITH cr
            MATCH (l:Location {RegionId: $region_id})
            MERGE (cr)-[:DEPLOYED_IN]->(l)

            WITH cr
            MATCH (ch:Charge {chargeDescription: $charge_desc})
            MERGE (cr)-[:HAS_CHARGE]->(ch)
        """, {
            "record_id": record_id,
            "billed_cost": billed_cost,
            "effective_cost": effective_cost,
            "currency": currency,
            "provider": cloud_provider,
            "billing_id": str(billing_account_id) if billing_account_id else None,
            "service_name": service_name,
            "resource_id": str(resource_id) if resource_id else None,
            "region_id": str(region_id) if region_id else None,
            "charge_desc": charge_desc
        })

    print(f"Finished ingesting {cloud_provider} data.")

if __name__ == "__main__":
    # Clear existing data for fresh start to match new relationships
    print("Clearing database...")
    db.query("MATCH (n) DETACH DELETE n")
    
    aws_path = 'data/aws_test-focus-00001.snappy_transformed.xls'
    azure_path = 'data/focusazure_anon_transformed.xls'
    
    ingest_data(aws_path, 'AWS')
    ingest_data(azure_path, 'Azure')
