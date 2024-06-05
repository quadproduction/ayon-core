""" Integrate USD

"""
from ayon_core.lib import usdlib, path_templates
from importlib import reload
reload(usdlib)
from pprint import pprint
import pyblish.api

import re
import os


class IntegrateUSD(pyblish.api.InstancePlugin):
    """Integrate USD."""

    label = "Integrate USD"
    order = pyblish.api.IntegratorOrder + 0.01
    families = ["usd"]
    active = True

    def process(self, instance):
        self.log.info('Start Integrating USD')
        published_files = self.get_published_files_from_representations(instance.data.get("published_representations"))
        if not published_files:
            self.log.info('No Published Files found')
            return
        self.log.info(f'Processing : {published_files}')
        usd_root_path = self.build_root_usd_template(instance.context.data)
        usdlib.create_root(usd_root_path, asset_name=instance.context.data['anatomyData']['folder']['name'], reference_layers=published_files)

    def get_published_files_from_representations(self, published_representations):
        published_data = []
        for representation_id, representation_data in published_representations.items():
            for attr_name, attr_value in representation_data.items():
                if attr_name == "published_files":
                    published_data.extend(attr_value)
        return published_data

    def build_root_usd_template(self, context_data):
        template_resolver = context_data
        template_resolver['root'] = context_data['anatomy'].roots
        template_resolver['folder'] = context_data['anatomyData']['folder']
        template_resolver['project'] = context_data['anatomyData']['project']
        template_resolver['hierarchy'] = context_data['anatomyData']['hierarchy']
        root_usd_dir_template = path_templates.StringTemplate("{root[work]}/{project[name]}/{hierarchy}/{folder[name]}/publish/usd")
        root_usd_dir = root_usd_dir_template.format_strict(template_resolver)
        self.log.info(f"USD root dir : {root_usd_dir}")

        template_resolver['version'] = self.get_root_usd_version(root_usd_dir)
        root_usd_template = path_templates.StringTemplate(f"{root_usd_dir_template}/{{folder[name]}}_USD_v{{version:0>5}}.usda")
        root_usd_path = root_usd_template.format_strict(template_resolver)
        self.log.info(f"USD root path : {root_usd_path}")
        return root_usd_path

    def get_root_usd_version(self, root_usd_dir_path):
        pattern = re.compile(r'_USD_v(\d+)\.usda$')

        versions = [
            int(match.group(1))
            for filename in os.listdir(root_usd_dir_path)
            if (match := pattern.search(filename))
        ]

        if not versions:
            versions.append(1)
        # TODO: set to max()
        return min(versions)