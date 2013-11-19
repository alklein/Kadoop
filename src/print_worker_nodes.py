import json
import sys

config_filename = 'config.json'
f = open(config_filename)
data = json.loads(f.read())

for node in data["worker_nodes"]:
    print node
