import subprocess
import os
import shutil
import json
import requests

# Import the generated frontend assets
from .frontend_assets import *

def create_nextjs_project(project_path, use_src):
    src_flag = '--src-dir' if use_src else ''
    subprocess.run(['npx', 'create-next-app@latest', project_path, '--typescript', '--eslint', '--tailwind', '--app','--no-import-alias', src_flag], check=True)
    os.chdir(project_path)
    subprocess.run(['npm', 'install', '-D', 'tailwindcss', 'postcss', 'autoprefixer'], check=True)
    subprocess.run(['npx', 'tailwindcss', 'init', '-p'], check=True)

def configure_tailwind(use_src):
    content_paths = [
        "./app/**/*.{js,ts,jsx,tsx,mdx}",
        "./pages/**/*.{js,ts,jsx,tsx,mdx}",
        "./components/**/*.{js,ts,jsx,tsx,mdx}"
    ]
    if use_src:
        content_paths.append("./src/**/*.{js,ts,jsx,tsx,mdx}")
    tailwind_config = f"""
/** @type {{import('tailwindcss').Config}} */
module.exports = {{
  content: {content_paths},
  theme: {{
    extend: {{}},
  }},
  plugins: [],
}}
"""
    with open('tailwind.config.js', 'w') as f:
        f.write(tailwind_config)

    globals_css = """
@tailwind base;
@tailwind components;
@tailwind utilities;
"""
    styles_dir = 'src/styles' if use_src else 'styles'
    os.makedirs(styles_dir, exist_ok=True)
    with open(os.path.join(styles_dir, 'globals.css'), 'w') as f:
        f.write(globals_css)

def remove_conflicting_files(project_path):
    if os.path.exists(project_path):
        try:
            shutil.rmtree(project_path)
        except Exception as e:
            print(f"Error removing directory {project_path}: {e}")
    os.makedirs(project_path, exist_ok=True)

def remove_src_directory(project_path):
    src_dir = os.path.join(project_path, 'src')
    if os.path.exists(src_dir):
        shutil.rmtree(src_dir)  # Remove the entire src directory

def create_env_file(project_path, api_host, open_ai_key, open_ai_json_mode_model):
    env_content = f"""
NEXT_PUBLIC_API_HOST={api_host}
NEXT_PUBLIC_OPEN_AI_KEY={open_ai_key}
NEXT_PUBLIC_OPEN_AI_JSON_MODE_MODEL={open_ai_json_mode_model}
"""
    with open(os.path.join(project_path, '.env'), 'w') as f:
        f.write(env_content)

def to_camel_case(snake_str):
    components = snake_str.split('_')
    return components[0].lower() + ''.join(x.title() for x in components[1:])

def deploy_project(location_of_next_project_in_local_machine, VERCEL_ACCESS_TOKEN, project_name, NETLIFY_TOKEN, domain_name):
    base_domain = ".".join(domain_name.split(".")[-2:])

    def deploy_with_vercel():
        try:
            # Navigate to the project directory
            os.chdir(location_of_next_project_in_local_machine)

            # Ensure Vercel CLI is installed
            subprocess.run(["npm", "install", "-g", "vercel"], check=True)

            # Set the Vercel token environment variable
            os.environ['VERCEL_TOKEN'] = VERCEL_ACCESS_TOKEN

            # Check if project is already linked
            try:
                subprocess.run(
                    ["vercel", "link", "--token", VERCEL_ACCESS_TOKEN, "--yes"],
                    check=True,
                    stdout=subprocess.PIPE,
                    text=True
                )
                print(f"Project {project_name} is already linked.")
            except subprocess.CalledProcessError:
                # Link the project if not already linked
                subprocess.run(
                    ["vercel", "link", "--name", project_name, "--token", VERCEL_ACCESS_TOKEN, "--yes"],
                    check=True,
                    stdout=subprocess.PIPE,
                    text=True
                )
                print(f"Linked to project {project_name}.")

            # Deploy the project using Vercel CLI to get initial URL
            result = subprocess.run(
                ["vercel", "--token", VERCEL_ACCESS_TOKEN, "--yes"],
                check=True,
                stdout=subprocess.PIPE,
                text=True
            )

            # Capture and print the deployment URL
            deployment_url = result.stdout.strip().split()[-1]
            print(f"Initial Deployment URL: {deployment_url}")

            # Add custom domain to Vercel project
            add_custom_domain_to_vercel(project_name, domain_name)

            # Update DNS settings on Netlify
            update_netlify_dns(deployment_url)

            # Deploy the project using Vercel CLI
            result = subprocess.run(
                ["vercel", "--prod", "--token", VERCEL_ACCESS_TOKEN, "--yes"],
                check=True,
                stdout=subprocess.PIPE,
                text=True
            )

            # Capture and print the deployment URL
            deployment_url = result.stdout.strip().split()[-1]
            print(f"Vercel Deployment URL: {deployment_url}")
            print(f"Domain Name Deployment URL: https://{domain_name}")

        except subprocess.CalledProcessError as e:
            print(f"An error occurred during deployment: {e}")

    def update_netlify_dns(deployment_url):
        try:
            # Extract the domain from the deployment URL
            domain = deployment_url.replace("https://", "").replace("http://", "").split("/")[0]

            headers = {
                "Authorization": f"Bearer {NETLIFY_TOKEN}",
                "Content-Type": "application/json"
            }

            # Get the DNS zone ID
            response = requests.get("https://api.netlify.com/api/v1/dns_zones", headers=headers)
            if response.status_code != 200:
                print(f"Failed to fetch DNS zones: {response.status_code} {response.text}")
                return

            dns_zones = response.json()
            print(f"DNS zones retrieved: {dns_zones}")  # Debugging statement
            zone_id = next((zone['id'] for zone in dns_zones if zone['name'] == base_domain), None)
            if not zone_id:
                print(f"No DNS zone found for domain: {base_domain}")
                return

            # Check for existing DNS records
            response = requests.get(f"https://api.netlify.com/api/v1/dns_zones/{zone_id}/dns_records", headers=headers)
            if response.status_code != 200:
                print(f"Failed to fetch DNS records: {response.status_code} {response.text}")
                return

            dns_records = response.json()
            print(f"DNS records retrieved for zone ID {zone_id}: {dns_records}")  # Debugging statement
            existing_record = next((record for record in dns_records if record['hostname'] == domain_name and record['type'] == "CNAME"), None)

            dns_record_data = {
                "type": "CNAME",
                "hostname": domain_name,
                "value": domain,
                "ttl": 3600
            }

            if existing_record:
                # Delete the existing DNS record
                delete_response = requests.delete(f"https://api.netlify.com/api/v1/dns_zones/{zone_id}/dns_records/{existing_record['id']}", headers=headers)
                if delete_response.status_code not in [200, 204]:
                    print(f"Failed to delete existing DNS record: {delete_response.status_code} {delete_response.text}")
                    return

                # Create the new DNS record after deletion
                response = requests.post(f"https://api.netlify.com/api/v1/dns_zones/{zone_id}/dns_records", headers=headers, json=dns_record_data)
                if response.status_code == 201:
                    print(f"DNS record for {domain_name} created successfully to point to {domain}")
                else:
                    print(f"Failed to create DNS record: {response.status_code} {response.text}")
            else:
                # Create a new DNS record
                response = requests.post(f"https://api.netlify.com/api/v1/dns_zones/{zone_id}/dns_records", headers=headers, json=dns_record_data)
                if response.status_code == 201:
                    print(f"DNS record for {domain_name} created successfully to point to {domain}")
                else:
                    print(f"Failed to create DNS record: {response.status_code} {response.text}")

        except Exception as e:
            print(f"An error occurred while updating DNS settings on Netlify: {e}")

    def add_custom_domain_to_vercel(project_name, domain_name):
        try:
            url = f"https://api.vercel.com/v9/projects/{project_name}/domains"
            headers = {
                "Authorization": f"Bearer {VERCEL_ACCESS_TOKEN}",
                "Content-Type": "application/json"
            }
            data = {
                "name": domain_name
            }
            response = requests.post(url, headers=headers, json=data)
            if response.status_code == 404:
                # Handle subdomain case by querying the base domain project
                project_id = get_vercel_project_id(base_domain)
                if project_id:
                    url = f"https://api.vercel.com/v9/projects/{project_id}/domains"
                    response = requests.post(url, headers=headers, json=data)
            
            if response.status_code == 409 and response.json().get("error", {}).get("code") == "domain_already_in_use":
                existing_project_id = response.json()["error"]["projectId"]
                remove_custom_domain_from_vercel(existing_project_id, domain_name)
                response = requests.post(url, headers=headers, json=data)

            if response.status_code in [200, 201]:
                print(f"Custom domain {domain_name} added to Vercel project {project_name} successfully.")
            else:
                print(f"Failed to add custom domain: {response.status_code} {response.text}")
        except Exception as e:
            print(f"An error occurred while adding custom domain to Vercel: {e}")

    def remove_custom_domain_from_vercel(project_id, domain_name):
        try:
            url = f"https://api.vercel.com/v9/projects/{project_id}/domains/{domain_name}"
            headers = {
                "Authorization": f"Bearer {VERCEL_ACCESS_TOKEN}",
                "Content-Type": "application/json"
            }
            response = requests.delete(url, headers=headers)
            if response.status_code in [200, 204]:
                print(f"Custom domain {domain_name} removed from Vercel project {project_id} successfully.")
            else:
                print(f"Failed to remove custom domain: {response.status_code} {response.text}")
        except Exception as e:
            print(f"An error occurred while removing custom domain from Vercel: {e}")

    def get_vercel_project_id(domain):
        try:
            url = f"https://api.vercel.com/v9/projects?name={domain}"
            headers = {
                "Authorization": f"Bearer {VERCEL_ACCESS_TOKEN}"
            }
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                projects = response.json()
                if projects and 'projects' in projects and projects['projects']:
                    return projects['projects'][0]['id']
            return None
        except Exception as e:
            print(f"An error occurred while fetching Vercel project ID: {e}")
            return None

    # Run the deployment
    deploy_with_vercel()


def main(project_name, frontend_local_deploy_path, host, backend_domain, frontend_domain, modals, modal_backend_config, non_user_modal_frontend_config, open_ai_key, open_ai_json_mode_model, netlify_key, vercel_key):

    modal_backend_config['modals']['users'] = {
        "columns": "username,password,type",
        "read_routes": [
            {"default": "SELECT * FROM users"}
        ]
    }

    modal_frontend_config = {
        "users": {
            "options": {
                "type": ["admin", "normal"]
            },
            "conditional_options": {},
            "scopes": {
                "create": True,
                "read": ["id","username","password","type", "created_at","updated_at"],
                "update": ["username","password","type"],
                "delete": True
            },
            "validation_rules": {
                "username": ["REQUIRED"],
                "password": ["REQUIRED"]
            },
        }
    }

    modal_frontend_config.update(non_user_modal_frontend_config)


    # Extract read_routes from modal_backend_config and append to modal_frontend_config
    for modal_name, modal_details in modal_backend_config.get('modals', {}).items():
        if isinstance(modal_details, dict) and 'read_routes' in modal_details:
            read_routes = [list(route.keys())[0] for route in modal_details['read_routes']]
            
            # Debugging: Print read_routes for each modal
            print(f"Read routes for {modal_name}: {read_routes}")
            
            if modal_name in modal_frontend_config:
                if 'read_routes' in modal_frontend_config[modal_name]:
                    modal_frontend_config[modal_name]['read_routes'].extend(read_routes)
                else:
                    modal_frontend_config[modal_name]['read_routes'] = read_routes
            else:
                modal_frontend_config[modal_name] = {
                    "read_routes": read_routes
                }
        else:
            # Debugging: Print the problematic modal_name and modal_details
            print(f"Error: {modal_name} does not have the expected structure. modal_details: {modal_details}")

    # Debugging: Print the final structure of modal_frontend_config
    print("modal_frontend_config:", json.dumps(modal_frontend_config, indent=4))

    # Get all modal names
    modals = ','.join(modal_backend_config.get('modals', {}).keys())
    print("Modals:", modals)


    use_src = True

    project_path = frontend_local_deploy_path
    api_host = f"https://{backend_domain}/"
    remove_conflicting_files(project_path)
    create_nextjs_project(project_path, use_src)
    remove_src_directory(project_path)
    configure_tailwind(use_src)
    create_env_file(project_path, api_host, open_ai_key, open_ai_json_mode_model)

    # Recreate the src directory and necessary subdirectories
    src_dir = os.path.join(project_path, 'src')
    os.makedirs(src_dir, exist_ok=True)
    os.makedirs(os.path.join(src_dir, 'pages'), exist_ok=True)
    os.makedirs(os.path.join(src_dir, 'components'), exist_ok=True)

    # Dictionary mapping variable names to file paths
    file_mapping = {
        "DIR__APP__FILE__PAGE__TSX": os.path.join(src_dir, "app/page.tsx"),
        "DIR__APP__FILE__LAYOUT__TSX": os.path.join(src_dir, "app/layout.tsx"),
        "DIR__APP__FILE__GLOBALS__CSS": os.path.join(src_dir, "app/globals.css"),
        "DIR__COMPONENTS__FILE__CREATE_MODAL__TSX": os.path.join(src_dir, "components/CreateModal.tsx"),
        "DIR__COMPONENTS__FILE__EDIT_MODAL__TSX": os.path.join(src_dir, "components/EditModal.tsx"),
        "DIR__COMPONENTS__FILE__SIDEBAR__TSX": os.path.join(src_dir, "components/Sidebar.tsx"),
        "DIR__COMPONENTS__FILE__DYNAMIC_TABLE__TSX": os.path.join(src_dir, "components/DynamicTable.tsx"),
        "DIR__COMPONENTS__FILE__VALIDATION_UTILS__TSX": os.path.join(src_dir, "components/validationUtils.tsx"),
        "DIR__COMPONENTS__FILE__CRUD_UTILS__TSX": os.path.join(src_dir, "components/crudUtils.tsx"),
        "DIR__COMPONENTS__FILE__FORMAT_UTILS__TSX": os.path.join(src_dir, "components/formatUtils.tsx"),
        "DIR__COMPONENTS__FILE__SEARCH_INPUT__TSX": os.path.join(src_dir, "components/SearchInput.tsx"),
        "DIR__COMPONENTS__FILE__SEARCH_UTILS__TSX": os.path.join(src_dir, "components/searchUtils.tsx"),
        "DIR__COMPONENTS__FILE__QUERY_INPUT__TSX": os.path.join(src_dir, "components/QueryInput.tsx"),
        "DIR__COMPONENTS__FILE__QUERY_UTILS__TSX": os.path.join(src_dir, "components/queryUtils.tsx"),
        "DIR__COMPONENTS__FILE__DOWNLOAD_UTILS__TSX": os.path.join(src_dir, "components/downloadUtils.tsx"),
        "DIR__COMPONENTS__FILE__MODAL_CONFIG__TSX": os.path.join(src_dir, "components/modalConfig.tsx"),
        "DIR__PAGES__FILE____MODAL____TSX": os.path.join(src_dir, "pages/[modal].tsx"),
        "DIR__PAGES__FILE__LOGIN__TSX": os.path.join(src_dir, "pages/login.tsx"),
        "ROOT__FILE__MIDDLEWARE__TSX": os.path.join(src_dir, "middleware.tsx"),
        # Add other mappings as needed
    }

    # Create necessary files from frontend assets
    for var_name, file_path in file_mapping.items():
        if var_name in globals():
            content = globals()[var_name]
            # Replace {{ and }} with { and }
            content = content.replace('{{', '{').replace('}}', '}')
            dir_path = os.path.dirname(file_path)
            os.makedirs(dir_path, exist_ok=True)
            with open(file_path, 'w') as f:
                f.write(content)

    search_phrase = "const modalConfig: ModalConfigMap"
    value = globals()["DIR__COMPONENTS__FILE__MODAL_CONFIG__TSX"]
    value = value.replace('{{', '{').replace('}}', '}')
    index = value.find(search_phrase)
    value = value[:index]
    # Convert the Python dictionary to a JSON string
    modal_config_json = json.dumps(modal_frontend_config, indent=2)
    # Format the JSON string to match the TSX syntax
    tsx_content = f"{value}\n\nconst modalConfig: ModalConfigMap = {modal_config_json};\n\nexport default modalConfig;"
    modal_config_file_path = os.path.join(src_dir, "components/modalConfig.tsx")
    # Write the formatted content to modalConfig.tsx
    with open(modal_config_file_path, 'w') as f:
        f.write(tsx_content)



    search_phrase_2 = "export const config"
    value_2 = globals()["ROOT__FILE__MIDDLEWARE__TSX"]
    value_2 = value_2.replace('{{', '{').replace('}}', '}')
    index_2 = value_2.find(search_phrase_2)
    value_2 = value_2[:index_2]
    keys = non_user_modal_frontend_config.keys()
    formatted_keys = [f"/{key}" for key in keys]
    existing_routes = ['/', '/login', '/users']
    all_routes = existing_routes + formatted_keys
    tsx_content_2 = f"{value_2}\n\nexport const config = {{ matcher: {all_routes},}};"

    middleware_file_path = os.path.join(src_dir, "middleware.tsx")
    # Write the formatted content to modalConfig.tsx
    with open(middleware_file_path, 'w') as f:
        f.write(tsx_content_2)

    #subprocess.run(['npm', 'run', 'dev'], check=True)


    deploy_project(
        location_of_next_project_in_local_machine=frontend_local_deploy_path,
        VERCEL_ACCESS_TOKEN=vercel_key,
        project_name=project_name,
        NETLIFY_TOKEN=netlify_key,
        domain_name=frontend_domain,
    )
