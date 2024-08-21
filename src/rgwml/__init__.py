from .p import *
from .d import *
from .f import *
from .resources.backend import *
from .resources.frontend import *
from .resources.frontend_assets import *

"""
# Import the most commonly used module
from .p import *

# List of modules to be lazily loaded
_lazy_modules = {
    'd': '.d',
    'f': '.f',
    'resources_backend': '.resources.backend',
    'resources_frontend': '.resources.frontend',
    'resources_frontend_assets': '.resources.frontend_assets'
}

# Function to handle lazy loading
def _lazy_import(module_name):
    import importlib
    import sys
    module = importlib.import_module(_lazy_modules[module_name], package=__name__)
    sys.modules[f'{__name__}.{module_name}'] = module
    globals()[module_name] = module

def __getattr__(name):
    if name in _lazy_modules:
        _lazy_import(name)
        return globals()[name]
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

# Define what should be exported when using `from rgwml import *`
__all__ = [name for name in globals() if not name.startswith('_')]

# Explicitly set just this module's name
__all__.extend(['d', 'f', 'resources_backend', 'resources_frontend', 'resources_frontend_assets'])
"""
