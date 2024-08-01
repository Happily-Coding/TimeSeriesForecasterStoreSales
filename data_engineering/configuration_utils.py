from configparser import ConfigParser

def load_ini_config_section(ini_filepath: str, config_section:str) -> dict[str, str]:
    """Get the parameters stored in a configuration section of an ini file"""
    parser = ConfigParser()
    parser.read(ini_filepath)
    conn_info = {param[0]: param[1] for param in parser.items(config_section)}
    return conn_info