import requests

def manage_dns_records(netlify_token, app_name, domain_name, vm_ip_address):
    def get_dns_zone_id():
        url = "https://api.netlify.com/api/v1/dns_zones"
        headers = {"Authorization": f"Bearer {netlify_token}"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        zones = response.json()
        for zone in zones:
            if zone["name"] == domain_name:
                return zone["id"]
        raise ValueError(f"DNS zone for domain {domain_name} not found.")

    def get_dns_records(zone_id):
        url = f"https://api.netlify.com/api/v1/dns_zones/{zone_id}/dns_records"
        headers = {"Authorization": f"Bearer {netlify_token}"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    def create_dns_record(zone_id, hostname, ip_address):
        url = f"https://api.netlify.com/api/v1/dns_zones/{zone_id}/dns_records"
        headers = {
            "Authorization": f"Bearer {netlify_token}",
            "Content-Type": "application/json"
        }
        data = {
            "type": "A",
            "hostname": hostname,
            "value": ip_address,
            "ttl": 3600
        }
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()

    zone_id = get_dns_zone_id()
    dns_records = get_dns_records(zone_id)

    subdomains = [f"{app_name}-api.{domain_name}"]
    records_to_create = []

    for subdomain in subdomains:
        exists = False
        for record in dns_records:
            if record["hostname"] == subdomain and record["value"] == vm_ip_address:
                exists = True
                break
        if not exists:
            records_to_create.append(subdomain)
    
    for subdomain in records_to_create:
        create_dns_record(zone_id, subdomain, vm_ip_address)
        print(f"Created DNS record for {subdomain} pointing to {vm_ip_address}")

"""
# Example usage
NETLIFY_TOKEN = "nfp_kqTg4zYEBqumgE7XVKtUgvRYiugb29zo2a14"
app_name = "test"
domain_name = "10xlabs.in"
vm_ip_address = "34.131.124.18"

manage_dns_records(NETLIFY_TOKEN, app_name, domain_name, vm_ip_address)
"""
