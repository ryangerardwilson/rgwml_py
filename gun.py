import os
import paramiko

VM_PRESETS = {
 "name": "labs_main_server",
 "host": "34.131.124.18",
 "ssh_user": "rgw",
 "ssh_key_path": "/home/rgw/Apps/rgw/SSH/wiombotServer"
}

location_of_app_file_on_vm = "/home/rgw/Apps/test/app.py"
app_name = "test"
app_file_name = "app"
domain_name = "test-api.10xlabs.in"


app_directory = os.path.dirname(location_of_app_file_on_vm)
service_file_name = f"{app_name}_API_BACKEND_SYSTEMD_SERVICE_FILE"
nginx_config_file_name = f"{domain_name}"
service_file_path = os.path.join(app_directory, f"{service_file_name}.service")
temp_nginx_config_file_path = os.path.join(app_directory, f"temp_{nginx_config_file_name}")

# SSH into the server
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(VM_PRESETS['host'], username=VM_PRESETS['ssh_user'], key_filename=VM_PRESETS['ssh_key_path'])

# Define the commands to execute
commands = [
    "sudo apt update",
    "sudo apt install -y python3-pip nginx",
    "sudo pip3 uninstall -y gunicorn bottle",
    "sudo pip3 install gunicorn bottle",
    f"echo '[Unit]\nDescription=Gunicorn instance to serve {app_name} API\nAfter=network.target\n\n[Service]\nWorkingDirectory={app_directory}\nExecStart=/usr/local/bin/gunicorn --workers=4 --bind unix:{app_directory}/{app_file_name}.sock app:app\nEnvironment=\"PATH=/usr/local/bin:/usr/bin:/bin\"\nEnvironment=\"PYTHONPATH=/usr/local/lib/python3.10/dist-packages\"\nRestart=always\n\n[Install]\nWantedBy=multi-user.target' > {service_file_path}",
    f"sudo mv {service_file_path} /etc/systemd/system/{service_file_name}.service",
    "sudo systemctl daemon-reload",
    f"sudo systemctl restart {service_file_name}",
    f"sudo systemctl status {service_file_name}",
    f"echo 'server {{\nlisten 80;\nserver_name {domain_name};\n\nlocation / {{\ninclude proxy_params;\nproxy_pass http://unix:{app_directory}/{app_file_name}.sock;\n}}\n}}' > {temp_nginx_config_file_path}",
    f"sudo mv {temp_nginx_config_file_path} /etc/nginx/sites-available/{nginx_config_file_name}",
    f"sudo chmod 777 /etc/nginx/sites-available/{nginx_config_file_name}",
    # Remove existing symbolic link if it exists
    f"if [ -L /etc/nginx/sites-enabled/{domain_name} ]; then sudo rm /etc/nginx/sites-enabled/{domain_name}; fi",
    # Create correct symbolic link
    f"sudo ln -s /etc/nginx/sites-available/{nginx_config_file_name} /etc/nginx/sites-enabled/{domain_name}",
    "sudo nginx -t",
    "sudo systemctl reload nginx",
    "sudo ufw allow 'Nginx Full'",
    "sudo apt install -y certbot python3-certbot-nginx",
    # Terminate any existing Certbot processes gracefully
    "for pid in $(pgrep certbot); do sudo kill -9 $pid; done",
    # Verify that no Certbot processes are running
    "while pgrep certbot > /dev/null; do sleep 1; done",
    # Run Certbot to obtain/renew certificates with --reinstall flag
    f"sudo certbot --nginx -d {domain_name} --reinstall",
]

# Execute each command
for command in commands:
    stdin, stdout, stderr = ssh.exec_command(command)
    print(stdout.read().decode())
    print(stderr.read().decode())

# Close the SSH connection
ssh.close()
