import subprocess
import os
import shutil
import json

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

def main(frontend_deploy_path, host, api_port, modals, modal_frontend_config, open_ai_key, open_ai_json_mode_model):

    use_src = True

    project_path = frontend_deploy_path
    api_host = f"http://{host}:{api_port}/"
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
        "DIR__COMPONENTS__FILE__FILTER_INPUT__TSX": os.path.join(src_dir, "components/FilterInput.tsx"),
        "DIR__COMPONENTS__FILE__FILTER_UTILS__TSX": os.path.join(src_dir, "components/filterUtils.tsx"),
        "DIR__COMPONENTS__FILE__MODAL_CONFIG__TSX": os.path.join(src_dir, "components/modalConfig.tsx"),
        "DIR__PAGES__FILE__PAGE__TSX": os.path.join(src_dir, "pages/page.tsx"),
        "DIR__PAGES__FILE____MODAL____TSX": os.path.join(src_dir, "pages/[modal].tsx"),
        "DIR__PAGES__FILE__LOGIN__TSX": os.path.join(src_dir, "pages/login.tsx"),
        "ROOT__FILE__MIDDLEWARE__TSX": os.path.join(src_dir, "pages/middleware.tsx"),
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

    # Convert the Python dictionary to a JSON string
    modal_config_json = json.dumps(modal_frontend_config, indent=2)

    # Format the JSON string to match the TSX syntax
    tsx_content = f"const modalConfig = {modal_config_json};\n\nexport default modalConfig;"
    modal_config_file_path = os.path.join(src_dir, "modalConfig.tsx")

    # Write the formatted content to modalConfig.tsx
    with open(modal_config_file_path, 'w') as f:
        f.write(tsx_content)

    subprocess.run(['npm', 'run', 'dev'], check=True)

