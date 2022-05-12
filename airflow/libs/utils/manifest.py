import json

JSON_MANIFEST_DBT = '/dbt/target/manifest.json'
PARENT_MAP = 'parent_map'


def load_manifest():
    local_filepath = JSON_MANIFEST_DBT
    with open(local_filepath) as f:
        data = json.load(f)

    return data


def make_dbt_task(node):
    """Returns an Airflow operator either run and test an individual model"""
    model = node.split(".")[-1]
    return model


data = load_manifest()
dbt_tasks = {}
for node in data["nodes"].keys():
    print(node)
