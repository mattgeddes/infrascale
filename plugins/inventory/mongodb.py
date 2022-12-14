#!/usr/bin/env python3

DOCUMENTATION = r'''
    name: mattgeddes.infrascale.mongodb
    plugin_type: inventory
    short_description: Returns Ansible inventory from MongoDB
    description: Returns Ansible inventory from MongoDB
    options:
      plugin:
          description: Name of the plugin
          required: true
          choices: ['mattgeddes.infrascale.mongodb']
      db_url:
        description: URL to database
        required: true
      db_name:
        description: database to connect to
        required: true
      db_collection:
        description: collection where inventory is stored
        required: true
      db_groups_attr:
        description: attribute(s) to group nodes by
        required: true
      db_attrs:
        description: attributes to expose as variables
        required: true
      db_shard:
        description: Attribute (key/val) to shard Ansible controllers against
        required: true
'''

import yaml
from ansible.plugins.inventory import BaseInventoryPlugin
from ansible.errors import AnsibleParserError
from pymongo import MongoClient
from pprint import pprint

def debug(msg):
    print(msg)
    pass

class DbMongo:
    def __init__(self, config):
        required = ["url"]  # parameters with no defaults that are required
        for val in required:
            if not val in config:
                raise AnsibleParserError("Database configuration key '%s' not found in configuration" % val)

        self.__dburi = config["url"]
        self.__dbname = config["dbname"] if "dbname" in config else "infrastructure"
        self.__dbcollection = config["collection"] if "collection" in config else "targets"
        self.__groups_attr = config["groups_attr"] if "groups_attr" in config else ""
        self.__attrs = config["attrs"] if "attrs" in config else []
        self.__shard = config["shard"] if "shard" in config else {}


    def connect(self):
        '''
        A quick sanity check of the configuration by connecting to the database.
        '''
        self.__db = MongoClient(self.__dburi)
        if self.__db:
            databases = self.__db.list_database_names()
            if self.__dbname in databases:
                # We're connected and the database we want exists.
                #debug("Connected to '%s', database '%s'" %
                        #(self.__dburi, self.__dbname))
                return True
            else:
                debug("Connected to '%s'. Database '%s' doesn't exist." %
                        (self.__dburi, self.__dbname))
                raise AnsibleParserError("Database doesn't exist %s/%s" %
                        (self.__dburi, self.__dbname))
        else:
            debug("Unable to connect to '%s'" % self.__dburi)
            raise AnsibleParserError("Unable to connect to %s/%s" %
                    (self.__dburi, self.__dbname))

        # Couldn't connect, or the database doesn't exist.
        return False

    def shard_myself(self, obj):
        # Quick method to check whether we're sharding and whether this is an
        # object within our shard
        if self.__shard and "key" in self.__shard and "val" in self.__shard:
            # We've been given a shard key and value, so we need to check that
            # against the provided object.
            if self.__shard["key"] in obj:
                if obj[self.__shard["key"]] == self.__shard["val"]:
                    debug("Shard key %s matched value %s" % \
                            (self.__shard["key"], self.__shard["val"]))
                    return True
                else:
                    debug("Sharding configured, but didn't match %s" % obj)
                    return False
            else:
                debug("Sharding not configured")
                return False

        # No sharding means that we always process the node
        return True

    def get_inventory(self):
        infra = self.__db[self.__dbname]
        targets = infra[self.__dbcollection]
        inv = {}
        inv["groups"] = {}
        inv["hosts"] = {}

        #docs = targets.find()
        pipeline = [
                { "$match": { "name": { "$exists": True } } },
                { "$lookup": { "from": "groups", "localField": "groups", "foreignField": "name", "as": "groups"} }
                ]
        docs = targets.aggregate(pipeline)
        #debug("Shard key/val: %s" % self.__shard)
        for doc in docs:
            #debug("Doc: %s" % doc)
            if not self.shard_myself(doc):
                # This isn't a node for our controller to manage, so skip it.
                continue
            inv["hosts"][doc["name"]] = {}
            # host-based Ansible variables
            if "ansible_vars" in doc:
                inv["hosts"][doc["name"]]["vars"] = doc["ansible_vars"]

            if "groups" in doc:
                # track group membership
                inv["hosts"][doc["name"]]["groups"] = []
                for group in doc["groups"]:
                    inv["hosts"][doc["name"]]["groups"].append(group["name"])

                    inv["groups"][group["name"]] = {}
                    if "ansible_vars" in group:
                        inv["groups"][group["name"]]["vars"] = group["ansible_vars"]

            # Empty groups list case
            if not inv["hosts"][doc["name"]]["groups"]:
                inv["hosts"][doc["name"]]["groups"].append("ungrouped")

        return inv


# Ansible inventory module implementation
class InventoryModule(BaseInventoryPlugin):

    NAME = 'mattgeddes.infrascale.mongodb'

    def __init__(self):
        #self._config = {}
        '''
        try:
            with open("config.yaml", "r") as f:
                config = yaml.safe_load(f)
        except Exception as e:
            raise AnsibleParserError("Failed to parse config.yaml: %s" % e)

        if not "database" in config:
            raise AnsibleParserError("Configuration file has no 'database' key")

        '''
        super(InventoryModule, self).__init__()

    def verify_file(self, path):
        '''
        Used to validate whether this module is (currently?) able to provide
        inventory.
        '''
        ret = False
        if super(InventoryModule, self).verify_file(path):
            # Check whether YAML file looks the part. This is pretty dumb though.
            # Looking at filenames for inherent context is a throwback to the 90s.
            if path.endswith(('db.yaml', 'db.yml', 'mongodb.yaml', 'mongodb.yml')):
                ret = True
        return ret

    def parse(self, inventory, loader, path, cache=False):
        '''
        Does the bulk of the heavy lifting. Where
            - inventory is the inventory data collected by Ansible so far
            - Ansible's built-in data loader (for loading data from files)
            - path string with inventory source
            - cache -- whether inventory can be cached or not
        '''

        super(InventoryModule, self).parse(inventory, loader, path, cache)

        # Read in the config file
        self._read_config_data(path)
        #debug("Reading %s" % path)
        config = {}
        config["database"] = {}
        config["database"]["url"] = self.get_option('db_url')
        config["database"]["dbname"] = self.get_option('db_name')
        config["database"]["collection"] = self.get_option('db_collection')
        config["database"]["groups_attr"] = self.get_option('db_groups_attr')
        config["database"]["attrs"] = self.get_option('db_attrs')
        config["database"]["shard"] = self.get_option('db_shard')
        #debug("Database config: %s" % config["database"])

        self.__db = DbMongo(config["database"])
        self.__db.connect()
        inv = self.__db.get_inventory()

        #debug("Inventory: %s" % inv)

        '''
        if "groups" in inv:
            for group in inv["groups"].keys():
                self.inventory.add_group(group)
                if "vars" in inv["groups"][group]:
                    #debug("Variables: %s" % inv["groups"][group]["vars"])
                    for (k,v) in inv["groups"][group]["vars"].items():
                        debug("(%s,%s)" % (k,v))
                        self.inventory.set_variable(group, k, v)
                if "hosts" in inv["groups"][group]:
                    for host in inv["groups"][group]["hosts"]:
                        self.inventory.add_host(host, group=group)
                        if host in inv["vars"]:
                            for (k, v) in inv["vars"][host].items():
                                self.inventory.set_variable(host, k, v)
        '''
        # Assemble inventory. We don't support nested groups today.
        for (name,host) in inv["hosts"].items():
            # Set all group membership
            for gname in host["groups"]:
                if gname == "ungrouped":
                    self.inventory.add_host(name)
                    continue
                if not gname in inv["groups"]:
                    continue
                group = inv["groups"][gname]
                self.inventory.add_group(gname)
                self.inventory.add_host(name, group=gname)
                # Set group variables.
                if "vars" in group:
                    for (k,v) in group["vars"].items():
                        self.inventory.set_variable(gname, k, v)
            # Set host variables
            if "vars" in host:
                for (k,v) in host["vars"].items():
                    self.inventory.set_variable(name, k, v)

        return True


if __name__ == '__main__':
    config = {}
    with open("inventory_mongodb.yaml", "r") as f:
        c = yaml.safe_load(f)
        config = {}
        config["url"] = c["db_url"]
        config["dbname"] = c["db_name"]
        config["collection"] = c["db_collection"]
        config["groups_attr"] = c["db_groups_attr"]
        config["attrs"] = c["db_attrs"]
        config["shard"] = c["db_shard"]
    db = DbMongo(config)
    db.connect()
    pprint(db.get_inventory())
