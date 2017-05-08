import yaml
import pprint

from jsonschema import validate

"""
@TODO(mivanov): Fix config validation
"""
schema = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "table": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "primary_key": {"type": "string"},
                "engine": {"type": "string"}
            }
        }
    }
}


def parse_config(cfg):
    """
    @TODO(mivanov): Fix config validation
    """
    validate(cfg, schema)



with open("cfg.yaml", 'r') as f:
    a = yaml.load(f)


parse_config(a)
pprint.pprint(a)