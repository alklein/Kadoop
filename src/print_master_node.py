import json
import sys

config_filename = 'config.json'
f = open(config_filename)
data = json.loads(f.read())

print data["master_node"]
