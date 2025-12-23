import oci

# Global Variables
domain_url = "https://idcs-e291d524a1e8439d999d85f5c7b520f1.identity.oraclecloud.com"
profile_name = "oci01"
users_to_exclude = [
    "chaitanya.kumar.upadhyay@oracle.com",                # Usernames to save
    "vishal.z.anand@oracle.com",
    "veronica.t.taing@oracle.com"
    # Add more as needed
]
users_to_include = [
        # Usernames to disable
    
]

# --- Initialize OCI Clients ---
config = oci.config.from_file(profile_name=profile_name)
oci_client = oci.identity_domains.IdentityDomainsClient(config, domain_url)

# Functions
def list_users():
    users = []
    page_token = None
    attributes = (
        "userName,id,displayName,active"
    )
    try:
        while True:
            response = oci_client.list_users(attributes=attributes, page=page_token, limit=100)
            users.extend(response.data.resources)
            if response.has_next_page:
                page_token = response.next_page
            else:
                break
    except oci.exceptions.ServiceError as e:
        print(f"Error listing users: {e}")
    return users

def deactivate_user_in_identity_domain(user_name, user_id):
    """
    Deactivates a user in an OCI Identity Domain.
    Args:
        user_name (str): The User name of the user.
        user_id (str): The User ID as defined in IDCS 
    """
    try:
        print(f"Deactivating {user_name}")
        oci_client.patch_user(
                user_id=user_id,
                patch_op={
                    "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                    "Operations": [{"op": "replace", "path": "active", "value": False}]
                }
            )
        print(f"User '{user_name}' (OCID: {user_id}) deactivated successfully.")
    except oci.exceptions.ServiceError as e:
        print(f"Error deactivating user: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def deactivate_users(user_list, exclude_list, users_to_include):
    for user in user_list:
            # print(f"Checking User '{user.user_name}' ...")
            if not getattr(user, "active", True):
                print(f" - User '{user.user_name}' is already deactivated")
                continue

            if user.user_name in exclude_list:
                print(f"Skipping excluded user '{user.user_name}' (OCID: {user.id})")
                continue
            elif user.user_name in users_to_include:
                print(f" - Deactivating user '{user.user_name}' (OCID: {user.id})...")
                deactivate_user_in_identity_domain(user.user_name, user.id)
                continue
            else:
               # print(f" - Deactivating user '{user.user_name}' (OCID: {user.id})...")
               # deactivate_user_in_identity_domain(user.user_name, user.id)
                continue

def main():
    print(f"Using OCI config profile: {profile_name}")
    list_users_data = list_users()
    # Pass the required parameters!
    deactivate_users(list_users_data,users_to_exclude,users_to_include)

if __name__ == "__main__":
    main()