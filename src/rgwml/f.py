import os
import json
from .resources.backend import main as backend_main
from .resources.frontend import main as frontend_main

class f:
    def __init__(self):
        pass

    def ser(self, db_preset_name, new_db_name, vm_preset_name, modal_backend_config, modal_frontend_config, backend_deploy_at, backend_deploy_port, frontend_deploy_path, open_ai_json_mode_model):
        """[f.ser(db_preset_name='your_rgwml_config_mysql_preset_name', new_db_name='name_of_db_to_br_created', vm_preset_name='your_rgwml_config_gcs_vm_preset_name',modal_backend_config={'customers': 'mobile,issue,status', 'partners': 'mobile,issue,status'}, modal_frontend_config, modal_frontend_config, backend_deploy_at='path/on/your/vm/to/deploy/your/backend', backend_deploy_port='8080', frontend_deploy_path='/path/on/your/local/machine/to/provision/your/frontend', 'gpt-3.5-turbo')]"""
        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [
                os.path.join(home_dir, "Desktop"),
                os.path.join(home_dir, "Documents"),
                os.path.join(home_dir, "Downloads"),
            ]

            for path in search_paths:
                for root, dirs, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        def load_db_config(db_preset_name):
            config_path = locate_config_file()
            with open(config_path, 'r') as f:
                config = json.load(f)

            db_presets = config.get('db_presets', [])
            db_preset = next((preset for preset in db_presets if preset['name'] == db_preset_name), None)
            if not db_preset:
                raise ValueError(f"No matching db_preset found for {db_preset_name}")

            return db_preset

        def load_vm_config(vm_preset_name):
            config_path = locate_config_file()
            with open(config_path, 'r') as f:
                config = json.load(f)

            vm_presets = config.get('vm_presets', [])
            vm_preset = next((preset for preset in vm_presets if preset['name'] == vm_preset_name), None)
            if not vm_preset:
                raise ValueError(f"No matching vm_preset found for {vm_preset_name}")

            return vm_preset

        def load_open_ai_config():
            config_path = locate_config_file()
            with open(config_path, 'r') as f:
                config = json.load(f)

            open_ai_key = config.get('open_ai_key')
            return open_ai_key



        # Load DB config
        db_preset = load_db_config(db_preset_name)
        db_type = db_preset['db_type']

        if db_type == 'mysql':
            host = db_preset['host']
            username = db_preset['username']
            password = db_preset['password']
            database = db_preset.get('database', '')

            db_config = {
                'host': host,
                'user': username,
                'password': password,
                'database': new_db_name
            }

        # Load VM config
        if vm_preset_name:
            vm_preset = load_vm_config(vm_preset_name)
            ssh_key_path = vm_preset['ssh_key_path']
        
            vm_host = vm_preset['host']
            ssh_user = vm_preset['ssh_user']
            instance = f"{ssh_user}@{vm_host}"
        else:
            raise ValueError("vm_preset_name is required")


        open_ai_key = load_open_ai_config()

        # Infer modals from modal_map keys
        #modals = ','.join(modal_backend_config.keys())
        modals = ','.join(modal_backend_config['modals'].keys())
    
        # Deploy backend
        backend_main(db_config, modal_backend_config, ssh_key_path, instance, backend_deploy_at, backend_deploy_port)

        # Deploy frontend
        frontend_main(frontend_deploy_path, vm_host, backend_deploy_port, modals, modal_frontend_config, open_ai_key, open_ai_json_mode_model)


