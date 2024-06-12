import os
import sys
import doctest
import importlib
import inspect
import argparse

def parse_init_py(init_file):
    modules_to_import = []
    with open(init_file, 'r') as file:
        lines = file.readlines()
        for line in lines:
            if line.startswith('from .'):
                module_name = line.split(' ')[1].strip().split('.')[0]
                modules_to_import.append(module_name)
    return modules_to_import

def parse_doctests(module_dir):
    doctest_functions = {}
    for root, _, files in os.walk(module_dir):
        for file in files:
            if file.endswith('.py'):
                module_name = file[:-3]
                module = importlib.import_module(f"rgwml.{module_name}")
                functions = inspect.getmembers(module, inspect.isfunction)
                for func_name, func in functions:
                    if func.__doc__ and '>>>' in func.__doc__:
                        if module_name not in doctest_functions:
                            doctest_functions[module_name] = {}
                        doctest_functions[module_name][func_name] = func
    return doctest_functions

def run_all_doctests(doctest_functions):
    for module_name, functions in doctest_functions.items():
        for func_name, func in functions.items():
            print(f"Running doctests for {module_name}.{func_name}...")
            doctest.run_docstring_examples(func, globals(), name=func_name)

def run_specific_doctest(module_name, class_name, method_name, doctest_functions):
    if module_name in doctest_functions:
        if method_name in doctest_functions[module_name]:
            func = doctest_functions[module_name][method_name]
            print(f"Running doctest for {module_name}.{method_name}...")
            doctest.run_docstring_examples(func, globals(), name=method_name)
        else:
            print(f"Method {method_name} not found in module {module_name}")
    else:
        print(f"Module {module_name} not found")

def main():
    parser = argparse.ArgumentParser(description='Run doctests in rgwml modules')
    parser.add_argument('--module', type=str, help='Module name (e.g., easy_pandas)')
    parser.add_argument('--cls', type=str, help='Class name (e.g., EP)')
    parser.add_argument('--method', type=str, help='Method name (e.g., fi)')

    args = parser.parse_args()

    init_file = 'src/rgwml/__init__.py'
    module_dir = 'src/rgwml'

    # Add the src directory to sys.path
    src_path = os.path.abspath('src')
    if src_path not in sys.path:
        sys.path.insert(0, src_path)

    modules_to_import = parse_init_py(init_file)
    for module in modules_to_import:
        importlib.import_module(f'rgwml.{module}')

    doctest_functions = parse_doctests(module_dir)

    if args.module and args.cls and args.method:
        run_specific_doctest(args.module, args.cls, args.method, doctest_functions)
    else:
        run_all_doctests(doctest_functions)

if __name__ == '__main__':
    main()

