import sys
import oci
import csv
import os
import datetime

profile = "int03"

# -------- Resource Stop Functions --------

def stop_compute_instances(compute_client, compartment_id):
    """Stops all RUNNING compute instances."""
    successes, failures = [], []
    try:
        instances = compute_client.list_instances(compartment_id=compartment_id, lifecycle_state="RUNNING").data
    except oci.exceptions.ServiceError as se:
        failures.append({
            "resource_name": "", "resource_id": "", "status": "failed",
            "message": f"[ServiceError {se.status} {se.code}] {se.message}"})
        return {"success": successes, "failed": failures}
    except Exception as e:
        failures.append({"resource_name": "", "resource_id": "", "status": "failed",
                         "message": f"Unexpected error: {e}"})
        return {"success": successes, "failed": failures}
    for instance in instances:
        print(f"  Stopping Compute Instance {instance.display_name} ({instance.id}) ...")
        try:
            compute_client.instance_action(instance.id, "STOP")
            successes.append({"resource_name": instance.display_name, "resource_id": instance.id,
                              "status": "success", "message": ""})
        except Exception as e:
            failures.append({"resource_name": instance.display_name, "resource_id": instance.id,
                             "status": "failed", "message": f"Failed to stop Compute Instance: {e}"})
    return {"success": successes, "failed": failures}

def stop_autonomous_databases(db_client, compartment_id):
    """Stops all AVAILABLE Autonomous Databases."""
    successes, failures = [], []
    try:
        dbs = db_client.list_autonomous_databases(compartment_id=compartment_id, lifecycle_state="AVAILABLE").data
    except oci.exceptions.ServiceError as se:
        failures.append({
            "resource_name": "", "resource_id": "", "status": "failed",
            "message": f"[ServiceError {se.status} {se.code}] {se.message}"})
        return {"success": successes, "failed": failures}
    except Exception as e:
        failures.append({"resource_name": "", "resource_id": "", "status": "failed",
                         "message": f"Unexpected error: {e}"})
        return {"success": successes, "failed": failures}
    for db in dbs:
        print(f"  Stopping Autonomous Database {db.db_name} ({db.id}) ...")
        try:
            db_client.stop_autonomous_database(db.id)
            successes.append({"resource_name": db.db_name, "resource_id": db.id,
                              "status": "success", "message": ""})
        except Exception as e:
            failures.append({"resource_name": db.db_name, "resource_id": db.id,
                             "status": "failed", "message": f"Failed to stop Autonomous Database: {e}"})
    return {"success": successes, "failed": failures}

def stop_generative_ai_endpoints(genai_client, compartment_id):
    """Stops all ACTIVE Generative AI Endpoints."""
    successes, failures = [], []
    try:
        endpoints = genai_client.list_endpoints(compartment_id=compartment_id, lifecycle_state="ACTIVE").data
    except oci.exceptions.ServiceError as se:
        failures.append({
            "resource_name": "", "resource_id": "", "status": "failed",
            "message": f"[ServiceError {se.status} {se.code}] {se.message}"})
        return {"success": successes, "failed": failures}
    except Exception as e:
        failures.append({"resource_name": "", "resource_id": "", "status": "failed",
                         "message": f"Unexpected error: {e}"})
        return {"success": successes, "failed": failures}
    for ep in endpoints:
        print(f"  Stopping Generative AI Endpoint {ep.display_name} ({ep.id}) ...")
        try:
            genai_client.deactivate_endpoint(ep.id)
            successes.append({"resource_name": ep.display_name, "resource_id": ep.id,
                              "status": "success", "message": ""})
        except Exception as e:
            failures.append({"resource_name": ep.display_name, "resource_id": ep.id,
                             "status": "failed", "message": f"Failed to deactivate endpoint: {e}"})
    return {"success": successes, "failed": failures}

def stop_visual_builder_instances(vb_client, compartment_id):
    """Stops all ACTIVE Oracle Visual Builder instances."""
    successes, failures = [], []
    try:
        vbs = vb_client.list_vb_instances(compartment_id=compartment_id, lifecycle_state="ACTIVE").data
    except Exception as e:
        failures.append({"resource_name": "", "resource_id": "", "status": "failed",
                         "message": f"Failed to list Visual Builder Instances: {e}"})
        return {"success": successes, "failed": failures}
    for vb in vbs:
        print(f"  Stopping Visual Builder Instance {vb.display_name} ({vb.id}) ...")
        try:
            vb_client.stop_vb_instance(vb.id)
            successes.append({"resource_name": vb.display_name, "resource_id": vb.id,
                              "status": "success", "message": ""})
        except Exception as e:
            failures.append({"resource_name": vb.display_name, "resource_id": vb.id,
                             "status": "failed", "message": f"Failed to stop Visual Builder Instance: {e}"})
    return {"success": successes, "failed": failures}

def stop_ai_language_endpoints(lang_client, compartment_id):
    """Stops all ACTIVE AI Language endpoints."""
    successes, failures = [], []
    try:
        eps = lang_client.list_endpoints(compartment_id=compartment_id, lifecycle_state="ACTIVE").data
    except Exception as e:
        failures.append({"resource_name": "", "resource_id": "", "status": "failed",
                         "message": f"Failed to list AI Language endpoints: {e}"})
        return {"success": successes, "failed": failures}
    for ep in eps:
        print(f"  Stopping AI Language Endpoint {ep.display_name} ({ep.id}) ...")
        try:
            lang_client.deactivate_endpoint(ep.id)
            successes.append({"resource_name": ep.display_name, "resource_id": ep.id,
                              "status": "success", "message": ""})
        except Exception as e:
            failures.append({"resource_name": ep.display_name, "resource_id": ep.id,
                             "status": "failed", "message": f"Failed to stop AI Language Endpoint: {e}"})
    return {"success": successes, "failed": failures}

def stop_analytics_instances(analytics_client, compartment_id):
    """Stops all ACTIVE Oracle Analytics Cloud instances."""
    successes, failures = [], []
    try:
        analytics_instances = analytics_client.list_analytics_instances(compartment_id=compartment_id, lifecycle_state="ACTIVE").data
    except Exception as e:
        failures.append({"resource_name": "", "resource_id": "", "status": "failed",
                         "message": f"Failed to list Analytics Cloud instances: {e}"})
        return {"success": successes, "failed": failures}
    for inst in analytics_instances:
        print(f"  Stopping Analytics Instance {inst.name} ({inst.id}) ...")
        try:
            analytics_client.stop_analytics_instance(inst.id)
            successes.append({"resource_name": inst.name, "resource_id": inst.id,
                              "status": "success", "message": ""})
        except Exception as e:
            failures.append({"resource_name": inst.name, "resource_id": inst.id,
                             "status": "failed", "message": f"Failed to stop Analytics Instance: {e}"})
    return {"success": successes, "failed": failures}

def stop_data_science_notebooks(ds_client, compartment_id):
    """Stops all ACTIVE Oracle Data Science notebook sessions."""
    successes, failures = [], []
    try:
        notebooks = ds_client.list_notebook_sessions(compartment_id=compartment_id, lifecycle_state="ACTIVE").data
    except Exception as e:
        failures.append({"resource_name": "", "resource_id": "", "status": "failed",
                         "message": f"Failed to list Data Science notebooks: {e}"})
        return {"success": successes, "failed": failures}
    for nb in notebooks:
        print(f"  Stopping Data Science Notebook Session {nb.display_name} ({nb.id}) ...")
        try:
            ds_client.deactivate_notebook_session(nb.id)
            successes.append({"resource_name": nb.display_name, "resource_id": nb.id,
                              "status": "success", "message": ""})
        except Exception as e:
            failures.append({"resource_name": nb.display_name, "resource_id": nb.id,
                             "status": "failed", "message": f"Failed to stop Data Science Notebook: {e}"})
    return {"success": successes, "failed": failures}

def stop_ai_data_platform_pipeline_runs(adp_client, compartment_id):
    """Stops all ACTIVE AI Data Platform pipeline runs."""
    successes, failures = [], []
    try:
        runs = adp_client.list_pipeline_runs(compartment_id=compartment_id, lifecycle_state="ACTIVE").data
    except Exception as e:
        failures.append({"resource_name": "", "resource_id": "", "status": "failed",
                         "message": f"Failed to list AI Data Platform pipeline runs: {e}"})
        return {"success": successes, "failed": failures}
    for run in runs:
        print(f"  Stopping AI Data Platform Pipeline Run {run.display_name} ({run.id}) ...")
        try:
            adp_client.deactivate_pipeline_run(run.id)
            successes.append({"resource_name": run.display_name, "resource_id": run.id,
                              "status": "success", "message": ""})
        except Exception as e:
            failures.append({"resource_name": run.display_name, "resource_id": run.id,
                             "status": "failed", "message": f"Failed to stop AI Data Platform Pipeline Run: {e}"})
    return {"success": successes, "failed": failures}

def stop_mysql_db_systems(mysql_client, compartment_id):
    """Stops all AVAILABLE MySQL DB Systems."""
    successes, failures = [], []
    try:
        dbs = mysql_client.list_db_systems(compartment_id=compartment_id, lifecycle_state="ACTIVE").data
    except Exception as e:
        failures.append({"resource_name": "", "resource_id": "", "status": "failed",
                         "message": f"Failed to list MySQL DB Systems: {e}"})
        return {"success": successes, "failed": failures}
    for db in dbs:
        print(f"  Stopping MySQL DB System {db.display_name} ({db.id}) ...")
        try:
            stop_details = oci.mysql.models.StopDbSystemDetails(shutdown_type="FAST")  # Specify shutdown type!
            mysql_client.stop_db_system(db.id, stop_details)
            successes.append({"resource_name": db.display_name, "resource_id": db.id,
                              "status": "success", "message": ""})
        except Exception as e:
            failures.append({"resource_name": db.display_name, "resource_id": db.id,
                             "status": "failed", "message": f"Failed to stop MySQL DB System: {e}"})
    return {"success": successes, "failed": failures}

def stop_integration_instances(oic_client, compartment_id):
    """Stops all AVAILABLE Integration Cloud instances."""
    successes, failures = [], []
    try:
        oics = oic_client.list_integration_instances(compartment_id=compartment_id, lifecycle_state="ACTIVE").data
    except Exception as e:
        failures.append({"resource_name": "", "resource_id": "", "status": "failed",
                         "message": f"Failed to list Integration Instances: {e}"})
        return {"success": successes, "failed": failures}
    for inst in oics:
        print(f"  Stopping Integration Instance {inst.display_name} ({inst.id}) ...")
        try:
            oic_client.stop_integration_instance(inst.id)
            successes.append({"resource_name": inst.display_name, "resource_id": inst.id,
                              "status": "success", "message": ""})
        except Exception as e:
            failures.append({"resource_name": inst.display_name, "resource_id": inst.id,
                             "status": "failed", "message": f"Failed to stop Integration Instance: {e}"})
    return {"success": successes, "failed": failures}

def stop_big_data_clusters(bds_client, compartment_id):
    """Stops all ACTIVE Big Data clusters."""
    successes, failures = [], []
    try:
        clusters = bds_client.list_bds_instances(compartment_id=compartment_id, lifecycle_state="ACTIVE").data
    except Exception as e:
        failures.append({"resource_name": "", "resource_id": "", "status": "failed",
                         "message": f"Failed to list Big Data clusters: {e}"})
        return {"success": successes, "failed": failures}
    for c in clusters:
        print(f"  Stopping Big Data Cluster {c.display_name} ({c.id}) ...")
        try:
            details = oci.bds.models.StopBdsInstanceDetails()  # Create details object (may pass config if needed)
            bds_client.stop_bds_instance(c.id, details)
            successes.append({"resource_name": c.display_name, "resource_id": c.id,
                              "status": "success", "message": ""})
        except Exception as e:
            failures.append({"resource_name": c.display_name, "resource_id": c.id,
                             "status": "failed", "message": f"Failed to stop Big Data Cluster: {e}"})
    return {"success": successes, "failed": failures}

def stop_oda_instances(oda_client, compartment_id):
    """Stops all ACTIVE Oracle Digital Assistant Instances."""
    successes, failures = [], []
    try:
        odas = oda_client.list_digital_assistant_instances(compartment_id=compartment_id, lifecycle_state="ACTIVE").data
    except Exception as e:
        failures.append({"resource_name": "", "resource_id": "", "status": "failed",
                         "message": f"Failed to list ODA Instances: {e}"})
        return {"success": successes, "failed": failures}
    for oda in odas:
        print(f"  Stopping ODA Instance {oda.display_name} ({oda.id}) ...")
        try:
            oda_client.stop_digital_assistant_instance(oda.id)
            successes.append({"resource_name": oda.display_name, "resource_id": oda.id,
                              "status": "success", "message": ""})
        except Exception as e:
            failures.append({"resource_name": oda.display_name, "resource_id": oda.id,
                             "status": "failed", "message": f"Failed to stop ODA Instance: {e}"})
    return {"success": successes, "failed": failures}

def stop_data_integration_pipeline_runs(di_client, compartment_id):
    """Stops all ACTIVE Data Integration pipeline runs."""
    successes, failures = [], []
    try:
        runs = di_client.list_pipeline_runs(compartment_id=compartment_id, lifecycle_state="ACTIVE").data
    except Exception as e:
        failures.append({"resource_name": "", "resource_id": "", "status": "failed",
                         "message": f"Failed to list Data Integration pipeline runs: {e}"})
        return {"success": successes, "failed": failures}
    for run in runs:
        print(f"  Stopping Data Integration Pipeline Run {run.display_name} ({run.id}) ...")
        try:
            di_client.deactivate_pipeline_run(run.id)
            successes.append({"resource_name": run.display_name, "resource_id": run.id,
                              "status": "success", "message": ""})
        except Exception as e:
            failures.append({"resource_name": run.display_name, "resource_id": run.id,
                             "status": "failed", "message": f"Failed to stop DI Pipeline Run: {e}"})
    return {"success": successes, "failed": failures}

def stop_network_firewalls(fw_client, compartment_id):
    """Stops all ACTIVE Network Firewalls."""
    successes, failures = [], []
    try:
        fws = fw_client.list_network_firewalls(
            compartment_id=compartment_id, lifecycle_state="ACTIVE"
        ).data
    except Exception as e:
        failures.append({"resource_name": "", "resource_id": "", "status": "failed",
                         "message": f"Failed to list Network Firewalls: {e}"})
        return {"success": successes, "failed": failures}
    for fw in fws.items:  # <-- Corrected: iterate over .items
        print(f"  Stopping Network Firewall {fw.display_name} ({fw.id}) ...")
        try:
            fw_client.stop_network_firewall(fw.id)
            successes.append({"resource_name": fw.display_name, "resource_id": fw.id,
                              "status": "success", "message": ""})
        except Exception as e:
            failures.append({"resource_name": fw.display_name, "resource_id": fw.id,
                             "status": "failed", "message": f"Failed to stop Network Firewall: {e}"})
    return {"success": successes, "failed": failures}

def stop_blockchain_platforms(bc_client, compartment_id):
    """Stops all ACTIVE Blockchain platforms."""
    successes, failures = [], []
    try:
        bps = bc_client.list_blockchain_platforms(
            compartment_id=compartment_id, lifecycle_state="ACTIVE"
        ).data
    except Exception as e:
        failures.append({"resource_name": "", "resource_id": "", "status": "failed",
                         "message": f"Failed to list Blockchain Platforms: {e}"})
        return {"success": successes, "failed": failures}
    for bp in bps.items:  # <-- corrected iteration
        print(f"  Stopping Blockchain Platform {bp.display_name} ({bp.id}) ...")
        try:
            bc_client.stop_blockchain_platform(bp.id)
            successes.append({"resource_name": bp.display_name, "resource_id": bp.id,
                              "status": "success", "message": ""})
        except Exception as e:
            failures.append({"resource_name": bp.display_name, "resource_id": bp.id,
                             "status": "failed", "message": f"Failed to stop Blockchain Platform: {e}"})
    return {"success": successes, "failed": failures}

def stop_opensearch_clusters(os_client, compartment_id):
    """Stops all ACTIVE OpenSearch clusters."""
    successes, failures = [], []
    try:
        clusters = os_client.list_opensearch_clusters(
            compartment_id=compartment_id, lifecycle_state="ACTIVE"
        ).data.items  # <-- THIS LINE IS THE FIX!
    except Exception as e:
        failures.append({"resource_name": "", "resource_id": "", "status": "failed",
                         "message": f"Failed to list OpenSearch Clusters: {e}"})
        return {"success": successes, "failed": failures}
    if not clusters:
        return {"success": [], "failed": []}
    for cluster in clusters:
        print(f"  Stopping OpenSearch Cluster {cluster.display_name} ({cluster.id}) ...")
        try:
            os_client.stop_opensearch_cluster(cluster.id)
            successes.append({"resource_name": cluster.display_name, "resource_id": cluster.id,
                              "status": "success", "message": ""})
        except Exception as e:
            failures.append({"resource_name": cluster.display_name, "resource_id": cluster.id,
                             "status": "failed", "message": f"Failed to stop OpenSearch Cluster: {e}"})
    return {"success": successes, "failed": failures}

def stop_redis_clusters(redis_client, compartment_id):
    """Stops all ACTIVE Redis clusters."""
    successes, failures = [], []
    try:
        clusters = redis_client.list_redis_clusters(compartment_id=compartment_id, lifecycle_state="ACTIVE").data
    except Exception as e:
        failures.append({"resource_name": "", "resource_id": "", "status": "failed",
                         "message": f"Failed to list Redis Clusters: {e}"})
        return {"success": successes, "failed": failures}
    for c in clusters:
        print(f"  Stopping Redis Cluster {c.display_name} ({c.id}) ...")
        try:
            redis_client.stop_redis_cluster(c.id)
            successes.append({"resource_name": c.display_name, "resource_id": c.id,
                              "status": "success", "message": ""})
        except Exception as e:
            failures.append({"resource_name": c.display_name, "resource_id": c.id,
                             "status": "failed", "message": f"Failed to stop Redis Cluster: {e}"})
    return {"success": successes, "failed": failures}

# -------- CSV Reader and Logger --------
def read_compartments_from_csv(csv_file_path):
    """Reads a CSV file with OCI compartment OCIDs (one per line), returns a list of OCIDs."""
    compartment_ocids = []
    with open(csv_file_path, 'r', newline='') as csvfile:
        for row in csv.reader(csvfile):
            if row and row[0].strip():
                compartment_ocids.append(row[0].strip())
    return compartment_ocids

def write_log_csv(log_file_path, log_rows):
    """Writes log rows to the specified CSV file (appends if exists, writes header otherwise)."""
    header = ["timestamp", "region", "compartment_id", "resource_type", "resource_name", "resource_id", "status", "message"]
    mode = "w" if not os.path.exists(log_file_path) else "a"
    with open(log_file_path, mode, newline='') as csvfile:
        writer = csv.writer(csvfile)
        if mode == "w":
            writer.writerow(header)
        writer.writerows(log_rows)

# -------- Main Control Logic --------
def main():
    """
    Main control logic for stopping resources specified by type and compartments,
    across all subscribed regions.
    Automatically generates a log file named by resource type and timestamp.
    """
    if len(sys.argv) != 3:
        print("Usage: python stop_resources.py <resource_type> <compartment_ocids.csv>")
        print("resource_type can be: compute, autonomous_database, generative_ai, visualbuilder, ai_language, analytics_cloud, data_science, ai_data_platform, mysql, integration_cloud, big_data, oracle_digital_assistant, data_integration, network_firewall, blockchain_cloud_service, opensearch, redis")
        sys.exit(1)
    resource_type = sys.argv[1].lower()
    csv_file = sys.argv[2]
    timestamp_str = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    log_file_path = f"stop_{resource_type}_log_{timestamp_str}.csv"

    compartment_ocids = read_compartments_from_csv(csv_file)
    print(f"You requested to stop ALL {resource_type} resources in these compartments:")
    for c in compartment_ocids:
        print(f"  - {c}")
    confirm = input("Are you sure? (y/N): ")
    if confirm.lower() != "y":
        print("Operation cancelled.")
        sys.exit(0)

    config = oci.config.from_file(profile_name=profile)
    identity = oci.identity.IdentityClient(config)
    tenancy_id = config["tenancy"]
    available_regions = [r.region_name for r in identity.list_region_subscriptions(tenancy_id).data]

    results = {}
    log_rows = []
    timestamp = datetime.datetime.utcnow().isoformat()

    for region in available_regions:
        print(f"\n##### Processing region: {region} #####")
        config["region"] = region
        for compartment_ocid in compartment_ocids:
            print(f"\n== Handling Compartment: {compartment_ocid}")
            if resource_type == "compute":
                compute_client = oci.core.ComputeClient(config)
                result = stop_compute_instances(compute_client, compartment_ocid)
            elif resource_type == "autonomous_database":
                db_client = oci.database.DatabaseClient(config)
                result = stop_autonomous_databases(db_client, compartment_ocid)
            elif resource_type == "generative_ai":
                genai_client = oci.generative_ai.GenerativeAiClient(config)
                result = stop_generative_ai_endpoints(genai_client, compartment_ocid)
            elif resource_type == "visualbuilder":
                vb_client = oci.vb_service.VbInstanceClient(config)
                result = stop_visual_builder_instances(vb_client, compartment_ocid)
            elif resource_type == "ai_language":
                lang_client = oci.ai_language.AIServiceLanguageClient(config)
                result = stop_ai_language_endpoints(lang_client, compartment_ocid)
            elif resource_type == "analytics_cloud":
                analytics_client = oci.analytics.AnalyticsClient(config)
                result = stop_analytics_instances(analytics_client, compartment_ocid)
            elif resource_type == "data_science":
                ds_client = oci.data_science.DataScienceClient(config)
                result = stop_data_science_notebooks(ds_client, compartment_ocid)
            elif resource_type == "ai_data_platform":
                adp_client = oci.ai_data_platform.AiDataPlatformClient(config)
                result = stop_ai_data_platform_pipeline_runs(adp_client, compartment_ocid)
            elif resource_type == "mysql":
                mysql_client = oci.mysql.DbSystemClient(config)
                result = stop_mysql_db_systems(mysql_client, compartment_ocid)
            elif resource_type == "integration_cloud":
                oic_client = oci.integration.IntegrationInstanceClient(config)
                result = stop_integration_instances(oic_client, compartment_ocid)
            elif resource_type == "big_data":
                bds_client = oci.bds.BdsClient(config)
                result = stop_big_data_clusters(bds_client, compartment_ocid)
            elif resource_type == "oracle_digital_assistant":
                oda_client = oci.oda.DigitalAssistantClient(config)
                result = stop_oda_instances(oda_client, compartment_ocid)
            elif resource_type == "data_integration":
                di_client = oci.data_integration.DataIntegrationClient(config)
                result = stop_data_integration_pipeline_runs(di_client, compartment_ocid)
            elif resource_type == "network_firewall":
                fw_client = oci.network_firewall.NetworkFirewallClient(config)
                result = stop_network_firewalls(fw_client, compartment_ocid)
            elif resource_type == "blockchain_cloud_service":
                bc_client = oci.blockchain.BlockchainPlatformClient(config)
                result = stop_blockchain_platforms(bc_client, compartment_ocid)
            elif resource_type == "opensearch":
                os_client = oci.opensearch.OpensearchClusterClient(config)
                result = stop_opensearch_clusters(os_client, compartment_ocid)
            elif resource_type == "redis":
                redis_client = oci.cache.RedisClusterClient(config)
                result = stop_redis_clusters(redis_client, compartment_ocid)
            else:
                print(f"Resource type '{resource_type}' is not supported.")
                continue
            results[(region, compartment_ocid)] = result
            for s in result["success"]:
                log_rows.append([
                    timestamp,
                    region,
                    compartment_ocid,
                    resource_type,
                    s.get("resource_name", ""),
                    s.get("resource_id", ""),
                    "success",
                    s.get("message", "")
                ])
            for f in result["failed"]:
                log_rows.append([
                    timestamp,
                    region,
                    compartment_ocid,
                    resource_type,
                    f.get("resource_name", ""),
                    f.get("resource_id", ""),
                    "failed",
                    f.get("message", "")
                ])
    print("\n======= Summary =======")
    for (region, comp), res in results.items():
        print(f"\n[Region: {region}] Compartment: {comp}")
        if res["success"]:
            print("  Successful:")
            for s in res["success"]:
                print(f"    - {s.get('resource_name')} ({s.get('resource_id')})")
        else:
            print("  No successful actions.")
        if res["failed"]:
            print("  Failed:")
            for f in res["failed"]:
                print(f"    - {f.get('resource_name')} ({f.get('resource_id')}): {f.get('message')}")
        else:
            print("  No failed actions.")
    write_log_csv(log_file_path, log_rows)
    print(f"\nLog written to {log_file_path}")

if __name__ == "__main__":
    main()