# Ansible Collection - mattgeddes.infrascale

## Overview

A collection of Ansible modules, roles, playbooks for managing largescale infrastructure.

## Plugins

### inventory/mongodb.py

A dynamic inventory module for providing Ansible inventory from the contents of a MongoDB database.

Sample configuration:

```yaml
---
plugin: mattgeddes.infrascale.mongodb
db_url: "mongodb://192.168.122.154:27017/"
db_name: "infrascale"
db_collection: "nodes"
db_groups_attr: "groups"
db_attrs:
  - name
db_shard:
```

Sample MongoDB document representing a node:

```json
{
  "name": "node1.example.com",
  "ansible_vars": {
    "ansible_user": "ansible",
    "ansible_become_method": "sudo"
  },
  "groups": [
    "production"
  ]
}
```

Sample MongoDB document representing a group:

```json
{
  "name": "production",
  "ansible_vars": {
    "ansible_user": "root",
    "ansible_python_interpreter": "/usr/bin/python3"
  }
}
```

