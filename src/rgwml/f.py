import os
import json
from .resources.backend import main as backend_main
from .resources.frontend import main as frontend_main

class f:
    def __init__(self):
        pass

    def ser(self, project_name, new_db_name, db_preset_name, vm_preset_name, modal_backend_config, modal_frontend_config, backend_vm_deploy_path, backend_domain, frontend_local_deploy_path, frontend_domain, open_ai_json_mode_model):
        """Usage: Deployment.ser(project_name, new_db_name, db_preset_name, vm_preset_name, modal_backend_config, modal_frontend_config, backend_vm_deploy_path, backend_domain, frontend_local_deploy_path, frontend_domain, open_ai_json_mode_model)"""

        def locate_config_file(filename="rgwml.config"):
            home_dir = os.path.expanduser("~")
            search_paths = [os.path.join(home_dir, folder) for folder in ["Desktop", "Documents", "Downloads"]]

            for path in search_paths:
                for root, _, files in os.walk(path):
                    if filename in files:
                        return os.path.join(root, filename)
            raise FileNotFoundError(f"{filename} not found in Desktop, Documents, or Downloads folders")

        def load_config(preset_name, key):
            config_path = locate_config_file()
            with open(config_path, 'r') as f:
                config = json.load(f)

            presets = config.get(key, [])
            preset = next((preset for preset in presets if preset['name'] == preset_name), None)
            if not preset:
                raise ValueError(f"No matching {key} found for {preset_name}")

            return preset

        def load_key(key_name):
            config_path = locate_config_file()
            with open(config_path, 'r') as f:
                config = json.load(f)

            return config.get(key_name)

        # Load DB config
        db_preset = load_config(db_preset_name, 'db_presets')
        db_config = {
            'host': db_preset['host'],
            'user': db_preset['username'],
            'password': db_preset['password'],
            'database': new_db_name
        }

        # Load VM config
        vm_preset = load_config(vm_preset_name, 'vm_presets')
        instance = f"{vm_preset['ssh_user']}@{vm_preset['host']}"

        # Load keys
        open_ai_key = load_key('open_ai_key')
        netlify_key = load_key('netlify_token')
        vercel_key = load_key('vercel_token')

        # Deploy backend
        backend_main(project_name, new_db_name, db_config, modal_backend_config, vm_preset['ssh_key_path'], instance, backend_vm_deploy_path, backend_domain, netlify_key, vm_preset, vm_preset['host'])

        # Deploy frontend
        modals = ','.join(modal_backend_config['modals'].keys())
        frontend_main(project_name, frontend_local_deploy_path, vm_preset['host'], backend_domain, frontend_domain, modals, modal_backend_config, modal_frontend_config, open_ai_key, open_ai_json_mode_model, netlify_key, vercel_key)

