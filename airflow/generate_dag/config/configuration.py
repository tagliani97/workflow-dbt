import json
import yaml
import os
from yaml.loader import SafeLoader


class ConfigPath:

    config_filepath = 'utils/config.yml'

    @classmethod
    def configuration_files(cls) -> dict:

        json_conf = {}

        dir = os.path.dirname(__file__)
        filename = os.path.join(dir, cls.config_filepath)

        with open(filename) as f:
            yml_conf = yaml.load(f, Loader=SafeLoader)

        for filename in os.listdir(yml_conf["config_dag"]):
            if 'json' in filename:
                f = open(yml_conf["config_dag"] + filename)
                json_conf[filename] = json.load(f)

        return json_conf, yml_conf
