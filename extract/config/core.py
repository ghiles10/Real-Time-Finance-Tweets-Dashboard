from typing import Union,  Dict, List, Optional, Sequence, Tuple, Any
from pathlib import Path
import strictyaml


# Project Directories
ROOT = Path(__file__).parent.parent.parent
CONFIG_FILE = ROOT / "config.yaml"


def validate_config_yaml(config_path: Union[str, Path] = None) -> Path:
    """
    Validate the config.yaml file.

    """
    config_path = Path(config_path)
    
    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found at {config_path}")
        
    return config_path


def load_config(config_path: Union[str, Path] = CONFIG_FILE) -> strictyaml.YAML:
    """
    Load the config.yaml file containing twitter credentials using strictyaml.
    """
    file = validate_config_yaml(config_path)
    
    with open(file, 'r') as f:
        config = strictyaml.load(f.read())

    return config