import os
import subprocess
import requests

location_of_next_project_in_local_machine = "/home/rgw/Apps/forge_frontend"
VERCEL_ACCESS_TOKEN = "obSWqaC94kq3QX5XGCIF5Ewx"
project_name = "forge-frontend"
NETLIFY_TOKEN = "nfp_kqTg4zYEBqumgE7XVKtUgvRYiugb29zo2a14"
domain_name = "test.10xlabs.in"
base_domain = "10xlabs.in"

def deploy_with_vercel(project_name):
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
        print(f"Final Deployment URL: {deployment_url}")

        # Optionally, write the deployment URL to a file
        with open("deployment-url.txt", "w") as file:
            file.write(deployment_url)

        print("Deployment successful")

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
        existing_record = next((record for record in dns_records if record['hostname'] == domain_name and record['type'] == "CNAME"), None)

        dns_record_data = {
            "type": "CNAME",
            "hostname": domain_name,
            "value": domain,
            "ttl": 3600
        }

        if existing_record:
            # Update the existing DNS record
            response = requests.put(f"https://api.netlify.com/api/v1/dns_zones/{zone_id}/dns_records/{existing_record['id']}", headers=headers, json=dns_record_data)
            if response.status_code == 200:
                print(f"DNS record for {domain_name} updated successfully to point to {domain}")
            else:
                print(f"Failed to update DNS record: {response.status_code} {response.text}")
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
        if response.status_code in [200, 201]:
            print(f"Custom domain {domain_name} added to Vercel project {project_name} successfully.")
        else:
            print(f"Failed to add custom domain: {response.status_code} {response.text}")
    except Exception as e:
        print(f"An error occurred while adding custom domain to Vercel: {e}")

# Run the deployment
deploy_with_vercel(project_name)

