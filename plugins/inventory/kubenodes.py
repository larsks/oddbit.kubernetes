from ansible.errors import AnsibleParserError
from ansible.plugins.inventory import BaseInventoryPlugin, Cacheable, Constructable

from kubernetes import client, config

DOCUMENTATION = r"""
name: kubenodes
plugin_type: inventory
short_description: Inventory of kubernetes nodes
extends_documentation_fragment:
    - constructed
options:
    plugin:
        type: str
        description: Name of the plugin
        required: true
        choices: ['oddbit.kubernetes.kubenodes']
    group_by_role:
        type: boolean
        description: Create groups based on node roles
        required: false
        default: false
    node_selectors:
        type: dict
        description: Labels limiting the discovered nodes
        required: false
    group:
        type: str
        description: Add nodes to named group
        required: false
    group_vars:
        type: dict
        description: Arbitrary group variables
        required: false
"""

class InventoryModule(BaseInventoryPlugin, Constructable, Cacheable):
    NAME = "kubenodes"

    def __init__(self):
        super(InventoryModule, self).__init__()

        config.load_kube_config()
        self._v1 = client.CoreV1Api()

    def verify_file(self, path: str):
        if super(InventoryModule, self).verify_file(path):
            return path.endswith("kubernetes.yaml")
        return False

    def _set_variables(self, hostvars):
        strict = self.get_option("strict")

        for host in hostvars:
            for varname, varvalue in hostvars[host].items():
                self.inventory.set_variable(host, varname, varvalue)

            # create composite vars
            self._set_composite_vars(
                self.get_option("compose"), hostvars[host], host, strict=strict
            )

            # constructed groups based on conditionals
            self._add_host_to_composed_groups(
                self.get_option("groups"), hostvars[host], host, strict=strict
            )

            # constructed keyed_groups
            self._add_host_to_keyed_groups(
                self.get_option("keyed_groups"), hostvars[host], host, strict=strict
            )

    def _create_node_variables(self, name, node):
        hostvars = {}

        roles = [
            label.split("/")[1]
            for label in node.metadata.labels
            if label.startswith("node-role.kubernetes.io")
        ]

        hostvars["node_roles"] = roles
        hostvars["node_labels"] = node.metadata.labels
        hostvars["node_annotations"] = node.metadata.annotations
        hostvars["node_info"] = node.status._node_info.to_dict()
        hostvars["node_addresses"] = [addr.to_dict() for addr in node.status.addresses]
        hostvars["node_ready"] = next(
            (
                condition.status == "True"
                for condition in node.status.conditions
                if condition.type == "Ready"
            ),
            False,
        )

        return hostvars

    def parse(self, inventory, loader, path, cache: bool = True):
        super(InventoryModule, self).parse(inventory, loader, path, cache)
        self._read_config_data(path)

        group_name = None
        if self.has_option("group") and self.get_option("group"):
            group_name = self.get_option("group")
            self.inventory.add_group(group_name)
            if self.has_option("group_vars"):
                for name, value in self.get_option("group_vars").items():
                    self.inventory.set_variable(group_name, name, value)

        label_selector=None
        if self.has_option("node_selectors") and self.get_option("node_selectors"):
            selectors = []
            for label, val in self.get_option("node_selectors").items():
                selectors.append(f"{label}={val if val else ''}")

            label_selector=','.join(selectors)

        nodes = self._v1.list_node(label_selector=label_selector)
        hostvars = {}
        for node in nodes.items:
            name = node.metadata.name
            self.inventory.add_host(name, group=group_name)

            if self.get_option('group_by_role'):
                for label in node.metadata.labels:
                    if not label.startswith('node-role.kubernetes.io/'):
                        continue

                    role = label.split('/')[1].replace('-', '_')
                    role_name = f'node_role_{role}'
                    if role_name not in self.inventory.groups:
                        self.inventory.add_group(role_name)

                    self.inventory.groups[role_name].add_host(self.inventory.hosts[name])

            if hasattr(node.status, 'addresses'):
                try:
                    address = next(
                        addr.address
                        for addr in node.status.addresses
                        if addr.type == "InternalIP"
                    )
                    self.inventory.set_variable(name, "ansible_host", address)
                except StopIteration:
                    pass

            hostvars[name] = self._create_node_variables(name, node)

        self._set_variables(hostvars)
