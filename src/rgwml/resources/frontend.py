import subprocess
import os
import shutil
import json
import requests
from google.cloud import storage
from .frontend_assets import *


def create_nextjs_project(project_path, use_src):
    src_flag = '--src-dir' if use_src else ''
    subprocess.run(['npx', 'create-next-app@latest', project_path, '--typescript', '--no-eslint', '--tailwind', '--app', '--no-import-alias', src_flag], check=True)
    os.chdir(project_path)

    # Remove .eslintrc.json if it exists
    eslintrc_path = os.path.join(project_path, '.eslintrc.json')
    if os.path.exists(eslintrc_path):
        os.remove(eslintrc_path)

    subprocess.run(['npm', 'install', '-D', 'tailwindcss', 'postcss', 'autoprefixer', 'papaparse'], check=True)
    subprocess.run(['npm', 'i', '--save-dev', '@types/papaparse'], check=True)
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


def create_env_file(project_path, api_host, open_ai_key, open_ai_json_mode_model, apk_url):
    env_content = f"""
NEXT_PUBLIC_API_HOST={api_host}
NEXT_PUBLIC_OPEN_AI_KEY={open_ai_key}
NEXT_PUBLIC_OPEN_AI_JSON_MODE_MODEL={open_ai_json_mode_model}
NEXT_PUBLIC_APK_URL={apk_url}
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


def create_and_deploy_next_js_frontend(project_name, frontend_local_deploy_path, host, backend_domain, frontend_domain, modals, modal_frontend_config, open_ai_key, open_ai_json_mode_model, netlify_key, vercel_key, apk_url):

    # Get all modal names
    # modals = ','.join(modal_backend_config.get('modals', {}).keys())
    # print("Modals:", modals)

    use_src = True

    project_path = frontend_local_deploy_path
    api_host = f"https://{backend_domain}/"
    remove_conflicting_files(project_path)
    create_nextjs_project(project_path, use_src)
    remove_src_directory(project_path)
    configure_tailwind(use_src)
    create_env_file(project_path, api_host, open_ai_key, open_ai_json_mode_model, apk_url)

    # Recreate the src directory and necessary subdirectories
    src_dir = os.path.join(project_path, 'src')
    os.makedirs(src_dir, exist_ok=True)
    os.makedirs(os.path.join(src_dir, 'pages'), exist_ok=True)
    os.makedirs(os.path.join(src_dir, 'components'), exist_ok=True)
    os.makedirs(os.path.join(src_dir, 'styles'), exist_ok=True)

    # Dictionary mapping variable names to file paths
    file_mapping = {
        "DIR__APP__FILE__PAGE__TSX": os.path.join(src_dir, "app/page.tsx"),
        "DIR__APP__FILE__LAYOUT__TSX": os.path.join(src_dir, "app/layout.tsx"),
        "DIR__APP__FILE__GLOBALS__CSS": os.path.join(src_dir, "app/globals.css"),
        "DIR__STYLES__FILE__SCROLLBAR__CSS": os.path.join(src_dir, "styles/scrollbar.css"),
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
        "DIR__COMPONENTS__FILE__BULK_OPERATIONS__TSX": os.path.join(src_dir, "components/BulkOperations.tsx"),
        "DIR__COMPONENTS__FILE__BULK_OPERATIONS_UTILS__TSX": os.path.join(src_dir, "components/bulkOperationsUtils.tsx"),
        "DIR__PAGES__FILE____MODAL____TSX": os.path.join(src_dir, "pages/[modal].tsx"),
        "DIR__PAGES__FILE__BULK__OPERATIONS__TSX": os.path.join(src_dir, "pages/bulk_operations.tsx"),
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
    keys = modal_frontend_config.keys()
    formatted_keys = [f"/{key}" for key in keys]
    existing_routes = ['/', '/login', '/users']
    all_routes = existing_routes + formatted_keys
    tsx_content_2 = f"{value_2}\n\nexport const config = {{ matcher: {all_routes},}};"

    middleware_file_path = os.path.join(src_dir, "middleware.tsx")
    # Write the formatted content to modalConfig.tsx
    with open(middleware_file_path, 'w') as f:
        f.write(tsx_content_2)

    # subprocess.run(['npm', 'run', 'dev'], check=True)

    deploy_project(
        location_of_next_project_in_local_machine=frontend_local_deploy_path,
        VERCEL_ACCESS_TOKEN=vercel_key,
        project_name=project_name,
        NETLIFY_TOKEN=netlify_key,
        domain_name=frontend_domain,
    )


def create_and_deploy_flutter_frontend(project_name, frontend_flutter_app_path, backend_domain, modals, modal_frontend_config, open_ai_key, open_ai_json_mode_model, cloud_storage_credential_path, cloud_storage_bucket_name, version):

    def create_public_bucket_if_not_exists_and_upload_version(cloud_storage_credential_path, cloud_storage_bucket_name, version):
        # Authenticate using the selected credentials file
        client = storage.Client.from_service_account_json(cloud_storage_credential_path)
        bucket_name = cloud_storage_bucket_name

        # Attempt to create the bucket
        try:
            buckets = list(client.list_buckets())
            bucket_names = [bucket.name for bucket in buckets]

            if bucket_name not in bucket_names:
                bucket = client.bucket(bucket_name)
                bucket.create(location="us")

                # Make the bucket publicly accessible
                bucket.make_public(recursive=True, future=True)

                print(f"Bucket '{bucket_name}' created and made public.")
            else:
                bucket = client.bucket(bucket_name)
                print(f"Bucket '{bucket_name}' already exists.")

            # Define the content of the VERSION.json file
            version_info = {
                "version": version,
                "apk_url": f"https://storage.googleapis.com/{bucket_name}/app-release.apk"
            }

            # Convert the version_info dictionary to a JSON string
            version_info_json = json.dumps(version_info)

            # Upload the VERSION.json file to the bucket
            blob = bucket.blob('VERSION.json')
            blob.upload_from_string(version_info_json, content_type='application/json')

            print(f"VERSION.json file uploaded to bucket '{bucket_name}'.")

            # Return the publicly accessible URL of the JSON file
            version_url = f"https://storage.googleapis.com/{bucket_name}/VERSION.json"
            return version_url

        except Exception as e:
            print(f"An error occurred: {str(e)}")
            return None

    def remove_existing_directory(path):
        if os.path.exists(path):
            shutil.rmtree(path)
            print(f"Removed existing directory and all its contents from {path}.")
        else:
            print(f"No existing directory found at {path}; proceeding with creation.")

    def create_flutter_project(project_path):
        project = os.path.basename(project_path)
        command = ['flutter', 'create', '--project-name', project, project_path]
        subprocess.run(command, check=True)
        print(f"Flutter project '{project}' created at {project_path}.")

    def update_pubspec_yaml(project_path):
        pubspec_path = os.path.join(project_path, 'pubspec.yaml')
        with open(pubspec_path, 'r') as file:
            lines = file.readlines()

        # Check if the packages already exist in pubspec.yaml
        has_rgml_fl = any('rgwml_fl: ^0.0.49' in line for line in lines)
        has_flutter_launcher_icons = any('flutter_launcher_icons: ^0.13.1' in line for line in lines)

        # Add the necessary dependencies if they don't exist
        if not has_rgml_fl:
            lines.insert(lines.index('dependencies:\n') + 1, '  rgwml_fl: ^0.0.49\n')
        if not has_flutter_launcher_icons:
            lines.insert(lines.index('dev_dependencies:\n') + 1, '  flutter_launcher_icons: ^0.13.1\n')

        # Add the flutter_launcher_icons configuration
        flutter_launcher_config = """
flutter_icons:
  android: true
  image_path: "assets/icon/icon.png"
  adaptive_icon_background: "#ffffff"
  adaptive_icon_foreground: "assets/icon/icon.png"
"""
        lines.append(flutter_launcher_config)

        with open(pubspec_path, 'w') as file:
            file.writelines(lines)
        print(f"Updated {pubspec_path} with necessary configurations.")

    def create_assets_directory(project_path):
        assets_icon_path = os.path.join(project_path, 'assets/icon')
        os.makedirs(assets_icon_path, exist_ok=True)
        icon_url = 'https://storage.googleapis.com/rgw-general-public/rgwml-assets/icon.png'
        response = requests.get(icon_url)
        with open(os.path.join(assets_icon_path, 'icon.png'), 'wb') as file:
            file.write(response.content)
        print(f"Downloaded icon and placed it in {assets_icon_path}.")

    def update_android_manifest(project_path, project_name):
        android_manifest_path = os.path.join(project_path, 'android/app/src/main/AndroidManifest.xml')
        with open(android_manifest_path, 'r') as file:
            lines = file.readlines()

        # Add the necessary permissions
        permissions = [
            '<uses-permission android:name="android.permission.INTERNET"/>',
            '<uses-permission android:name="android.permission.WRITE_EXTERNAL_STORAGE"/>',
            '<uses-permission android:name="android.permission.READ_EXTERNAL_STORAGE"/>',
            '<uses-permission android:name="android.permission.ACCESS_FINE_LOCATION"/>',
            '<uses-permission android:name="android.permission.ACCESS_COARSE_LOCATION"/>'
        ]

        for line in permissions:
            if line not in lines:
                lines.insert(1, line + '\n')
        project = os.path.basename(project_path)
        # Replace the app name with the project_name
        for i in range(len(lines)):
            if 'android:label=' in lines[i]:
                lines[i] = lines[i].replace(f'android:label="{project}"', f'android:label="{project_name}"')

        with open(android_manifest_path, 'w') as file:
            file.writelines(lines)
        print(f"Updated AndroidManifest.xml with necessary permissions and project name '{project_name}'.")

    def update_flutter_settings_gradle_version(project_path):
        settings_gradle_path = os.path.join(project_path, 'android/settings.gradle')

        new_content = """pluginManagement {
    def flutterSdkPath = {
        def properties = new Properties()
        file("local.properties").withInputStream { properties.load(it) }
        def flutterSdkPath = properties.getProperty("flutter.sdk")
        assert flutterSdkPath != null, "flutter.sdk not set in local.properties"
        return flutterSdkPath
    }()

    includeBuild("$flutterSdkPath/packages/flutter_tools/gradle")

    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

plugins {
    id "dev.flutter.flutter-plugin-loader" version "1.0.0"
    id "com.android.application" version "7.3.0" apply false
    id "org.jetbrains.kotlin.android" version "1.9.0" apply false
}

include ":app"
        """  # Ensure this block has the exact format you need

        with open(settings_gradle_path, 'w') as file:
            file.write(new_content.strip())

        print("Updated settings.gradle to use the specified content.")

    def add_modal_config_to_main_dart_file(frontend_flutter_app_path, modal_frontend_config):
        def convert_to_dart_bool(value):
            return 'true' if value else 'false'

        def convert_read_routes(read_routes):
            read_route_configs = []
            for route_name, route_config in read_routes.items():
                belongs_to_user_id = convert_to_dart_bool(route_config['belongs_to_user_id'])
                read_route_configs.append(f'"{route_name}": ReadRouteConfig(belongsToUserId: {belongs_to_user_id})')
            return '{' + ', '.join(read_route_configs) + '}'

        def convert_conditional_options(conditional_options):
            result = {}
            for key, value in conditional_options.items():
                result[key] = [
                    f'ConditionalOption(condition: "{co["condition"]}", options: {json.dumps(co["options"])})'
                    for co in value
                ]
            return result

        modal_config_entries = []
        for modal_name, modal_data in modal_frontend_config.items():
            options = json.dumps(modal_data.get("options", {}), indent=4)
            conditional_options = convert_conditional_options(modal_data.get("conditional_options", {}))

            # Convert dictionary to Dart map literal
            conditional_options_str = '{' + ', '.join(f'"{key}": [{", ".join(value)}]' for key, value in conditional_options.items()) + '}'

            scopes_create = convert_to_dart_bool(modal_data.get("scopes", {}).get("create", False))
            scopes_read = json.dumps(modal_data.get("scopes", {}).get("read", []))
            scopes_read_summary = json.dumps(modal_data.get("scopes", {}).get("read_summary", []))
            scopes_update = json.dumps(modal_data.get("scopes", {}).get("update", []))
            scopes_delete = convert_to_dart_bool(modal_data.get("scopes", {}).get("delete", False))
            validation_rules = json.dumps(modal_data.get("validation_rules", {}), indent=4)
            ai_quality_checks = json.dumps(modal_data.get("ai_quality_checks", {}), indent=4)

            # Convert read_routes to the new ReadRouteConfig structure
            read_routes = modal_data.get("read_routes", {})
            read_routes_str = convert_read_routes(read_routes)

            modal_config_entry = f"""
        "{modal_name}": ModalConfig(
            options: Options({options}),
            conditionalOptions: {conditional_options_str},
            scopes: Scopes(
                create: {scopes_create},
                read: {scopes_read},
                read_summary: {scopes_read_summary},
                update: {scopes_update},
                delete: {scopes_delete},
            ),
            validationRules: {validation_rules},
            aiQualityChecks: {ai_quality_checks},
            readRoutes: {read_routes_str},
        ),
            """
            modal_config_entries.append(modal_config_entry)

        modal_config_entries_str = '\n'.join(modal_config_entries)

        main_dart_content = f"""import 'package:rgwml_fl/rgwml_fl.dart';

    void main() {{
      final modalConfig = ModalConfigMap({{
        {modal_config_entries_str}
      }});

      runApp(MyApp(modalConfig: modalConfig));
    }}
        """
        main_dart_path = os.path.join(frontend_flutter_app_path, 'lib', 'main.dart')

        if not os.path.exists(os.path.join(frontend_flutter_app_path, 'lib')):
            os.makedirs(os.path.join(frontend_flutter_app_path, 'lib'))

        with open(main_dart_path, 'w') as f:
            f.write(main_dart_content)

        print(f"Generated main.dart at {main_dart_path}")

    def add_my_app_class_to_main_dart(frontend_flutter_app_path, version, project_name, version_url, backend_domain, open_ai_json_mode_model, open_ai_key):
        my_app_class = f"""
class MyApp extends StatelessWidget {{
  final ModalConfigMap modalConfig;

  static const String currentVersion = "{version}";

  MyApp({{super.key, required this.modalConfig}});

  @override
  Widget build(BuildContext context) {{
    return MaterialApp(
      title: '{project_name}',
      theme: ThemeData(
        primarySwatch: Colors.blue,
      ),
      home: CRMDashboard(
        title: '{project_name} Dashboard',
        versionUrl: '{version_url}',
        currentVersion: currentVersion,
        apiHost: 'https://{backend_domain}/',
        modalConfig: modalConfig,
        openAiJsonModeModel: '{open_ai_json_mode_model}',
        openAiApiKey: '{open_ai_key}'
      ),
    );
  }}
}}
        """

        main_dart_path = os.path.join(frontend_flutter_app_path, 'lib', 'main.dart')

        with open(main_dart_path, 'a') as f:  # Append to the existing main.dart
            f.write(my_app_class)

        print(f"Appended MyApp class to main.dart at {main_dart_path}")

    def run_flutter_launcher_icons_commands(frontend_flutter_app_path):
        os.chdir(frontend_flutter_app_path)

        result = subprocess.run(['flutter', 'pub', 'get'], capture_output=True, text=True)
        if result.returncode == 0:
            print("Successfully ran 'flutter pub get'")
        else:
            print(f"Error running 'flutter pub get': {result.stderr}")

        result = subprocess.run(['flutter', 'pub', 'run', 'flutter_launcher_icons'], capture_output=True, text=True)
        if result.returncode == 0:
            print("Successfully ran 'flutter pub run flutter_launcher_icons'")
        else:
            print(f"Error running 'flutter pub run flutter_launcher_icons': {result.stderr}")

    # print(project_name, frontend_local_deploy_path, host, backend_domain, modals, modal_backend_config, non_user_modal_frontend_config, open_ai_key, open_ai_json_mode_model)
    variables = {
        "project_name": project_name,
        "frontend_flutter_app_path": frontend_flutter_app_path,
        "backend_domain": backend_domain,
        "modals": modals,
        # "modal_backend_config": modal_backend_config,
        "modal_frontend_config": modal_frontend_config,
        "open_ai_key": open_ai_key,
        "open_ai_json_mode_model": open_ai_json_mode_model,
        "cloud_storage_credential_path": cloud_storage_credential_path,
        "cloud_storage_bucket_name": cloud_storage_bucket_name,
        "version": version
    }

    for name, value in variables.items():
        print(f"{name}: {value}")
        print()

    def build_and_upload_release_apk_and_update_version(cloud_storage_credential_path, cloud_storage_bucket_name, frontend_flutter_app_path, version):
        try:
            # Step 1: Build the release APK
            print("Building the release APK...")
            subprocess.run(
                ['flutter', 'build', 'apk', '--release'],
                cwd=frontend_flutter_app_path,
                check=True
            )
            print("Release APK built successfully.")

            # Path to the generated APK file
            apk_path = f"{frontend_flutter_app_path}/build/app/outputs/flutter-apk/app-release.apk"
            apk_blob_name = "app-release.apk"

            # Step 2: Upload the release APK to the bucket
            print("Uploading the release APK to the bucket...")
            client = storage.Client.from_service_account_json(cloud_storage_credential_path)
            bucket = client.bucket(cloud_storage_bucket_name)

            blob = bucket.blob(apk_blob_name)
            blob.upload_from_filename(apk_path)

            # Make the blob public
            blob.make_public()

            apk_url = f"https://storage.googleapis.com/{cloud_storage_bucket_name}/{apk_blob_name}"
            print(f"Release APK uploaded to '{apk_url}'.")

            # Step 3: Update the VERSION.json with the new APK URL
            print("Updating VERSION.json with new APK URL...")
            version_blob = bucket.blob('VERSION.json')

            version_info_json = version_blob.download_as_text()
            version_info = json.loads(version_info_json)

            version_info["apk_url"] = apk_url

            updated_version_info_json = json.dumps(version_info)
            version_blob.upload_from_string(updated_version_info_json, content_type='application/json')

            print(f"VERSION.json updated with new APK URL in bucket '{project_name}'.")
            return apk_url

        except subprocess.CalledProcessError as e:
            print(f"An error occurred while building the release APK: {str(e)}")
        except Exception as e:
            print(f"An error occurred: {str(e)}")

    # STEP 1: Create a GCS bucket by the project_name, to store the APK that would eventually be created along with the version.json file
    version_url = create_public_bucket_if_not_exists_and_upload_version(cloud_storage_credential_path, cloud_storage_bucket_name, version)

    # STEP 2: Create skeleton of theflutter project at the absolute path frontend_flutter_app_path (the name of the flutter project is specified in the abolsute path), with:
    remove_existing_directory(frontend_flutter_app_path)
    create_flutter_project(frontend_flutter_app_path)
    update_pubspec_yaml(frontend_flutter_app_path)
    create_assets_directory(frontend_flutter_app_path)
    update_android_manifest(frontend_flutter_app_path, project_name)
    update_flutter_settings_gradle_version(frontend_flutter_app_path)

    # STEP 3: Build the main.dart file
    add_modal_config_to_main_dart_file(frontend_flutter_app_path, modal_frontend_config)
    add_my_app_class_to_main_dart(frontend_flutter_app_path, version, project_name, version_url, backend_domain, open_ai_json_mode_model, open_ai_key)
    run_flutter_launcher_icons_commands(frontend_flutter_app_path)

    # STEP 4: Build and upload the APK
    apk_url = build_and_upload_release_apk_and_update_version(cloud_storage_credential_path, cloud_storage_bucket_name, frontend_flutter_app_path, version)
    return apk_url


def update_modal_frontend_config_with_user_modal(modal_backend_config, non_user_modal_frontend_config):

    modal_backend_config['modals']['users'] = {
        "columns": "username,password,type",
        "read_routes": [
            {"default": "SELECT * FROM users"}
        ]
    }

    modal_frontend_config = {
        "users": {
            "options": {
                "type[XOR]": ["admin", "normal"]
            },
            "conditional_options": {},
            "scopes": {
                "create": True,
                "read": ["id", "username", "password", "type", "created_at", "updated_at"],
                "read_summary": ["id", "username", "password", "type"],
                "update": ["username", "password", "type"],
                "delete": True
            },
            "validation_rules": {
                "username": ["REQUIRED"],
                "password": ["REQUIRED"]
            },
            "ai_quality_checks": {},
        }
    }

    modal_frontend_config.update(non_user_modal_frontend_config)

    # Extract and transform read_routes from modal_backend_config and append to modal_frontend_config
    for modal_name, modal_details in modal_backend_config.get('modals', {}).items():
        if isinstance(modal_details, dict) and 'read_routes' in modal_details:
            read_routes = {}
            for route in modal_details['read_routes']:
                for route_name, query in route.items():
                    read_routes[route_name] = {
                        "belongs_to_user_id": "belongs_to_user_id = '$belongs_to_user_id$'" in query
                    }

            # Debugging: Print read_routes for each modal
            print(f"Read routes for {modal_name}: {read_routes}")

            if modal_name in modal_frontend_config:
                if 'read_routes' in modal_frontend_config[modal_name]:
                    modal_frontend_config[modal_name]['read_routes'].update(read_routes)
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

    return modal_frontend_config


def main(project_name, frontend_local_deploy_path, frontend_flutter_app_path, host, backend_domain, frontend_domain, modals, modal_backend_config, non_user_modal_frontend_config, open_ai_key, open_ai_json_mode_model, netlify_key, vercel_key, cloud_storage_credential_path, cloud_storage_bucket_name, version, deploy_web, deploy_flutter):

    modal_frontend_config = update_modal_frontend_config_with_user_modal(modal_backend_config, non_user_modal_frontend_config)

    if deploy_flutter:
        apk_url = create_and_deploy_flutter_frontend(project_name, frontend_flutter_app_path, backend_domain, modals, modal_frontend_config, open_ai_key, open_ai_json_mode_model, cloud_storage_credential_path, cloud_storage_bucket_name, version)
        # print(apk_url)

    if deploy_web:
        apk_url = f"https://storage.googleapis.com/{cloud_storage_bucket_name}/app-release.apk"
        create_and_deploy_next_js_frontend(project_name, frontend_local_deploy_path, host, backend_domain, frontend_domain, modals, modal_frontend_config, open_ai_key, open_ai_json_mode_model, netlify_key, vercel_key, apk_url)

    # apk_url = f"https://storage.googleapis.com/{cloud_storage_bucket_name}/app-release.apk"
    # create_and_deploy_flutter_frontend(project_name, frontend_flutter_app_path, backend_domain, modals, modal_frontend_config, open_ai_key, open_ai_json_mode_model, cloud_storage_credential_path, cloud_storage_bucket_name, version)
    # create_and_deploy_next_js_frontend(project_name, frontend_local_deploy_path, host, backend_domain, frontend_domain, modals, modal_frontend_config, open_ai_key, open_ai_json_mode_model, netlify_key, vercel_key, apk_url)
