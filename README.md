# OCI Resource Stopper – Multi-Resource Multi-Region Automation Script

This script automates the stopping (deactivation) of a wide variety of Oracle Cloud Infrastructure (OCI) resources—across **all subscribed regions**—for one or more compartments, as provided in a CSV file.  
It’s designed for cost optimization, operational hygiene, and streamlined management.

---

## Supported Resource Types

- Compute Instances
- Autonomous Databases
- Generative AI Endpoints
- Visual Builder Instances
- AI Language Endpoints
- Analytics Cloud Instances
- Data Science Notebook Sessions
- AI Data Platform Pipeline Runs
- MySQL DB Systems
- Integration Cloud Instances
- Big Data Clusters
- Digital Assistant (ODA) Instances
- Data Integration Pipeline Runs
- Network Firewalls
- Blockchain Platform Instances
- OpenSearch Clusters
- Redis Clusters

---

## Prerequisites

- Python 3.x installed
- OCI Python SDK installed  
  `pip install oci`
- A [valid OCI config file](https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/pythonsdk.htm#config) (`~/.oci/config`) with API keys and profile
- Permissions to stop resources in the target compartments
- CSV file containing **compartment OCIDs** (one per line)

---

## Usage

```sh
python stop_resources.py <resource_type> <compartments.csv>
```

- **resource_type**: One of the supported types listed above  
  (e.g., `compute`, `autonomous_database`, `generative_ai`, etc.)
- **compartments.csv**: CSV file with compartment OCIDs, one per line

### **Example**

```sh
python stop_resources.py compute my_compartments.csv
```

You will be prompted to confirm before any destructive action is taken.

---

## What the Script Does

- Iterates through all **subscribed regions** in your tenancy
- For each region and each listed compartment:
  - Finds resources of the requested type in a RUNNING/ACTIVE/AVAILABLE state
  - Attempts to stop/deactivate each resource
  - Logs all operations (with status and details) to a timestamped CSV file

---

## Output

A CSV log file named `stop_<resource_type>_log_<timestamp>.csv` with columns:
- timestamp, region, compartment_id, resource_type, resource_name, resource_id, status, message

---

## Important Notes

- **Run Safely:** This script performs disruptive operations! Double-check your compartments before running, and always review prompts.
- **Profile:** Set your preferred OCI config profile at the top of the script (`profile = "int03"`) or edit as needed.
- **Permissions:** Make sure your OCI user/API key has stopping/deactivation privileges for all resource types.
- **Error Handling:** Errors are logged in the output file; nothing is skipped silently.
- **Extensible:** Add more resource-type handlers as needed.

---

## Example compartments.csv

```
ocid1.compartment.oc1..aaaaaaaaxxxxxxxxyyyyyyyyzzzzzzzz
ocid1.compartment.oc1..aaaaaaaammqqqqqqwwwwwwwwvvvvvvvv
```

---

## Contributing

Pull requests and suggestions are welcome! Please create an issue for major changes and propose improvements.

---

## License

Specify your project’s license here (e.g., MIT, Apache 2.0, etc.)

---

## Author

*Chaitanya Kumar Upadhyay*
