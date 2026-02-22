import sys
import os
sys.path.append(os.getcwd())

import pandas as pd
import numpy as np
import json
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

def parse_tags(tags_str):
    if not tags_str:
        return {}
    try:
        return json.loads(tags_str)
    except:
        return {}

def ingest_data(file_path, cloud_provider):
    print(f"Ingesting {cloud_provider} data from {file_path}...")
    df = pd.read_excel(file_path)
    
    for _, row in df.iterrows():
        # 1. Account Nodes
        billing_id = clean_value(get_col(row, 'BillingAccountId'))
        billing_name = clean_value(get_col(row, 'BillingAccountName'))
        sub_id = clean_value(get_col(row, 'SubAccountId'))
        sub_name = clean_value(get_col(row, 'SubAccountName'))
        
        if billing_id:
            db.query("""
                MERGE (a:BillingAccount {BillingAccountId: $id})
                SET a.BillingAccountName = $name
            """, {"id": str(billing_id), "name": billing_name})

        if sub_id:
            db.query("""
                MERGE (s:SubAccount {SubAccountId: $id})
                SET s.SubAccountName = $name
            """, {"id": str(sub_id), "name": sub_name})

        # 2. Service & Resource
        service_name = clean_value(get_col(row, 'ServiceName'))
        service_cat = clean_value(get_col(row, 'ServiceCategory'))
        resource_id = clean_value(get_col(row, 'ResourceId'))
        resource_name = clean_value(get_col(row, 'ResourceName'))
        resource_type = clean_value(get_col(row, 'ResourceType'))

        if service_name:
            db.query("MERGE (s:Service {ServiceName: $name}) SET s.ServiceCategory = $cat", 
                     {"name": service_name, "cat": service_cat})
        
        if resource_id:
            db.query("MERGE (r:Resource {ResourceId: $id}) SET r.ResourceName = $name, r.ResourceType = $type",
                     {"id": str(resource_id), "name": resource_name, "type": resource_type})
            if service_name:
                db.query("""
                    MATCH (r:Resource {ResourceId: $rid}), (s:Service {ServiceName: $sname})
                    MERGE (r)-[:USES_SERVICE]->(s)
                """, {"rid": str(resource_id), "sname": service_name})

        # 3. Location
        region_id = clean_value(get_col(row, 'RegionId'))
        region_name = clean_value(get_col(row, 'RegionName'))
        if region_id:
            db.query("MERGE (l:Location {RegionId: $id}) SET l.RegionName = $name", {"id": str(region_id), "name": region_name})
            if resource_id:
                db.query("""
                    MATCH (r:Resource {ResourceId: $rid}), (l:Location {RegionId: $lid})
                    MERGE (r)-[:DEPLOYED_IN]->(l)
                """, {"rid": str(resource_id), "lid": str(region_id)})

        # 4. TimeFrame
        cp_s = clean_value(get_col(row, 'ChargePeriodStart'))
        cp_e = clean_value(get_col(row, 'ChargePeriodEnd'))
        bp_s = clean_value(get_col(row, 'BillingPeriodStart'))
        bp_e = clean_value(get_col(row, 'BillingPeriodEnd'))
        
        time_frame_id = f"{cp_s}_{cp_e}"
        if cp_s:
            db.query("""
                MERGE (t:TimeFrame {TimeFrameId: $id})
                SET t.ChargePeriodStart = $cp_s, t.ChargePeriodEnd = $cp_e,
                    t.BillingPeriodStart = $bp_s, t.BillingPeriodEnd = $bp_e
            """, {"id": time_frame_id, "cp_s": cp_s, "cp_e": cp_e, "bp_s": bp_s, "bp_e": bp_e})

        # 5. Charge
        ch_cat = clean_value(get_col(row, 'ChargeCategory'))
        ch_freq = clean_value(get_col(row, 'ChargeFrequency'))
        ch_desc = clean_value(get_col(row, 'ChargeDescription'))
        ch_class = clean_value(get_col(row, 'ChargeClass'))
        if ch_desc:
            db.query("""
                MERGE (ch:Charge {chargeDescription: $desc})
                SET ch.chargeCategory = $cat, ch.chargeFrequency = $freq, ch.chargeClass = $class
            """, {"desc": ch_desc, "cat": ch_cat, "freq": ch_freq, "class": ch_class})

        # 6. Vendor Specific Attributes
        v_attr_id = f"v_attr_{cloud_provider}_{_}"
        if cloud_provider == 'AWS':
            x_sc = clean_value(get_col(row, 'x_ServiceCode'))
            x_ut = clean_value(get_col(row, 'x_UsageType'))
            db.query("MERGE (v:AWSAttributes {id: $id}) SET v.x_ServiceCode = $sc, v.x_usageType = $ut", 
                     {"id": v_attr_id, "sc": x_sc, "ut": x_ut})
        else:
            x_smc = clean_value(get_col(row, 'x_skumetercategory'))
            x_sd = clean_value(get_col(row, 'x_skudescription'))
            db.query("MERGE (v:AzureAttributes {id: $id}) SET v.x_skumetercategory = $smc, v.x_skudescription = $sd", 
                     {"id": v_attr_id, "smc": x_smc, "sd": x_sd})

        # 7. Tags & Allocation Targets
        tags_raw = get_col(row, 'Tags') if cloud_provider == 'AWS' else get_col(row, 'tags')
        tags = parse_tags(tags_raw)
        app = tags.get('application') or tags.get('Application')
        env = tags.get('environment') or tags.get('Environment')
        cc = tags.get('cost_center') or tags.get('CostCentre') or tags.get('x_costcenter')

        # 8. CostRecord Creation
        record_id = f"{cloud_provider}_rec_{_}"
        billed = float(get_col(row, 'BilledCost') or 0)
        eff = float(get_col(row, 'EffectiveCost') or billed)
        list_c = float(get_col(row, 'ListCost') or 0)
        contract = float(get_col(row, 'ContractedCost') or 0)
        qty = float(get_col(row, 'ConsumedQuantity') or 0)
        unit = clean_value(get_col(row, 'ConsumedUnit'))
        curr = clean_value(get_col(row, 'Currency'))

        db.query("""
            CREATE (cr:CostRecord {
                RecordId: $rid, BilledCost: $billed, EffectiveCost: $eff,
                ListCost: $list, ContractedCost: $contract,
                ConsumedQuantity: $qty, ConsumedUnit: $unit, Currency: $curr,
                TagsKV: $tags, Provider: $provider,
                application: $app, environment: $env, CostCentre: $cc
            })
            WITH cr
            MATCH (ba:BillingAccount {BillingAccountId: $bid}) MERGE (cr)-[:BELONGS_TO_BILLING_ACCOUNT]->(ba)
            WITH cr
            MATCH (sa:SubAccount {SubAccountId: $sid}) MERGE (cr)-[:BELONGS_TO_SUBACCOUNT]->(sa)
            WITH cr
            MATCH (t:TimeFrame {TimeFrameId: $tfid}) MERGE (cr)-[:IN_TIME_FRAME]->(t)
            WITH cr
            MATCH (ch:Charge {chargeDescription: $ch_desc}) MERGE (cr)-[:HAS_CHARGE]->(ch)
            WITH cr
            MATCH (r:Resource {ResourceId: $resid}) MERGE (cr)-[:INCURRED_BY]->(r)
            WITH cr
            MATCH (v {id: $vid}) MERGE (cr)-[:HAS_VENDOR_ATTRS]->(v)
        """, {
            "rid": record_id, "billed": billed, "eff": eff, "list": list_c, "contract": contract,
            "qty": qty, "unit": unit, "curr": curr, "tags": json.dumps(tags), "provider": cloud_provider,
            "app": app, "env": env, "cc": cc, "bid": str(billing_id), "sid": str(sub_id),
            "tfid": time_frame_id, "ch_desc": ch_desc, "resid": str(resource_id), "vid": v_attr_id
        })

        # 9. Direct Tag Relationships (Strategic for retrieval)
        if app:
            db.query("""
                MATCH (cr:CostRecord {RecordId: $rid})
                MERGE (target:Application {name: $app})
                MERGE (cr)-[:PART_OF_APP]->(target)
            """, {"rid": record_id, "app": app})
        if env:
            db.query("""
                MATCH (cr:CostRecord {RecordId: $rid})
                MERGE (target:Environment {name: $env})
                MERGE (cr)-[:PART_OF_ENV]->(target)
            """, {"rid": record_id, "env": env})
        if cc:
            db.query("""
                MATCH (cr:CostRecord {RecordId: $rid})
                MERGE (target:CostCentre {name: $cc})
                MERGE (cr)-[:PART_OF_CC]->(target)
            """, {"rid": record_id, "cc": cc})

        # 10. Allocation (If applicable)
        alloc_rule = clean_value(get_col(row, 'x_costallocationrulename'))
        if alloc_rule:
            db.query("""
                MATCH (cr:CostRecord {RecordId: $rid})
                MERGE (ca:CostAllocation {allocationRuleName: $rule})
                SET ca.allocationMethod = 'Proportional' 
                MERGE (cr)-[:ALLOCATED_VIA]->(ca)
            """, {"rid": record_id, "rule": alloc_rule})
            
            if app:
                db.query("MERGE (target:Application {name: $app}) WITH target MATCH (ca:CostAllocation {allocationRuleName: $rule}) MERGE (ca)-[:ALLOCATED_TO]->(target)", {"app": app, "rule": alloc_rule})
            if env:
                db.query("MERGE (target:Environment {name: $env}) WITH target MATCH (ca:CostAllocation {allocationRuleName: $rule}) MERGE (ca)-[:ALLOCATED_TO]->(target)", {"env": env, "rule": alloc_rule})
            if cc:
                db.query("MERGE (target:CostCentre {name: $cc}) WITH target MATCH (ca:CostAllocation {allocationRuleName: $rule}) MERGE (ca)-[:ALLOCATED_TO]->(target)", {"cc": cc, "rule": alloc_rule})

    print(f"Finished ingesting {cloud_provider} data.")

if __name__ == "__main__":
    print("Clearing database...")
    db.query("MATCH (n) DETACH DELETE n")
    
    aws_path = 'data/aws_test-focus-00001.snappy_transformed.xls'
    azure_path = 'data/focusazure_anon_transformed.xls'
    
    ingest_data(aws_path, 'AWS')
    ingest_data(azure_path, 'Azure')
