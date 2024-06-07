""" Integrate USD

"""
from ayon_core.lib import usdlib, path_templates
from importlib import reload
reload(usdlib)

import pyblish.api
import copy
import re
import os

from ayon_api import (
    get_attributes_for_type,
    get_product_by_name,
    get_version_by_name,
    get_representations,
)

from ayon_api.operations import (
    OperationsSession,
    new_product_entity,
    new_version_entity,
    new_representation_entity,
)

class IntegrateUSD(pyblish.api.InstancePlugin):
    """Integrate USD."""

    label = "Integrate USD"
    order = pyblish.api.IntegratorOrder + 0.01
    families = ["usd"]
    active = True

    def __init__(self):
        self.version_number = 0

    def process(self, instance):
        self.log.info('Start Integrating USD')
        published_files = self.get_published_files_from_representations(instance.data.get("published_representations"))
        if not published_files:
            self.log.info('No Published Files found, Skip Integrating USD')
            return
        self.log.info(f'Processing : {published_files}')
        instance.data['source'] = self.build_root_usd_template(instance.context.data)
        usdlib.create_root(instance.data['source'], asset_name=instance.context.data['anatomyData']['folder']['name'], reference_layers=published_files)
        root_repr = [{
            'ext': os.path.splitext(instance.data['source'])[1],
            'files': os.path.basename(instance.data['source']),
            'name': 'usd',
            'published_path': instance.data['source'],
            'stagingDir': os.path.dirname(instance.data['source'])
        }]
        instance.data['representations'].append(root_repr)
        self.register(instance)

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

        template_resolver['version'] = self.version_number = self.get_new_root_usd_version(root_usd_dir)
        root_usd_template = path_templates.StringTemplate(f"{root_usd_dir_template}/{{folder[name]}}_USD_v{{version:0>5}}.usda")
        root_usd_path = root_usd_template.format_strict(template_resolver)
        self.log.info(f"USD root path : {root_usd_path}")
        return root_usd_path

    def get_new_root_usd_version(self, root_usd_dir_path):
        pattern = re.compile(r'_USD_v(\d+)\.usda$')

        versions = [
            int(match.group(1))
            for filename in os.listdir(root_usd_dir_path)
            if (match := pattern.search(filename))
        ]

        if not versions:
            return 1

        # Increment version
        return max(versions) + 1

    def register(self, instance):
        op_session = OperationsSession()
        project_name = instance.context.data["projectName"]
        product_entity = self.prepare_product(instance, op_session, project_name)
        version_entity = self.prepare_version(instance, op_session, product_entity, project_name)
        op_session.commit()

        instance.data["versionEntity"] = version_entity

        # Get existing representations (if any)
        existing_repres_by_name = {
            repre_entity["name"].lower(): repre_entity
            for repre_entity in get_representations(
                project_name,
                version_ids=[version_entity["id"]]
            )
        }

        prepared_representation = self.prepare_repre(instance.data['representations'][0], version_entity, existing_repres_by_name, instance)
        repre_entity = prepared_representation["representation"]
        op_session.create_entity(project_name, "representation", repre_entity)
        op_session.commit()

    def prepare_product(self, instance, op_session, project_name):
        folder_entity = instance.data["folderEntity"]
        product_name = "usd_root"
        product_type = instance.data["productType"]

        # Get existing product if it exists
        existing_product_entity = get_product_by_name(
            project_name, product_name, folder_entity["id"]
        )

        # Define product data
        data = {
            "families": [instance.data["family"]]
        }

        attributes = {}

        product_group = instance.data.get("productGroup")
        if product_group:
            attributes["productGroup"] = product_group
        elif existing_product_entity:
            # Preserve previous product group if new version does not set it
            product_group = existing_product_entity.get("attrib", {}).get(
                "productGroup"
            )
            if product_group is not None:
                attributes["productGroup"] = product_group

        product_id = None
        if existing_product_entity:
            product_id = existing_product_entity["id"]

        product_entity = new_product_entity(
            product_name,
            product_type,
            folder_entity["id"],
            data=data,
            attribs=attributes,
            entity_id=product_id
        )

        if existing_product_entity is None:
            # Create a new product
            self.log.info(
                "Product '%s' not found, creating ..." % product_name
            )
            op_session.create_entity(
                project_name, "product", product_entity
            )

        else:
            # Update existing product data with new data and set in database.
            # We also change the found product in-place so we don't need to
            # re-query the product afterwards
            update_data = self.prepare_changes(
                existing_product_entity, product_entity
            )
            op_session.update_entity(
                project_name,
                "product",
                product_entity["id"],
                update_data
            )

        self.log.debug("Prepared product: {}".format(product_name))
        return product_entity

    def prepare_version(self, instance, op_session, product_entity, project_name):
        task_id = None
        task_entity = instance.data.get("taskEntity")
        if task_entity:
            task_id = task_entity["id"]

        existing_version = get_version_by_name(
            project_name,
            self.version_number,
            product_entity["id"]
        )
        version_id = None
        if existing_version:
            version_id = existing_version["id"]

        all_version_data = self.create_version_data(instance)
        version_data = {}
        version_attributes = {}
        attr_defs = self._get_attributes_by_type(instance.context)["version"]
        for key, value in all_version_data.items():
            if key in attr_defs:
                version_attributes[key] = value
            else:
                version_data[key] = value

        version_entity = new_version_entity(
            self.version_number,
            product_entity["id"],
            task_id=task_id,
            data=version_data,
            attribs=version_attributes,
            entity_id=version_id,
        )

        if existing_version:
            self.log.debug("Updating existing version ...")
            update_data = self.prepare_changes(existing_version, version_entity)
            op_session.update_entity(
                project_name,
                "version",
                version_entity["id"],
                update_data
            )
        else:
            self.log.debug("Creating new version ...")
            op_session.create_entity(
                project_name, "version", version_entity
            )

        self.log.debug(
            "Prepared version: v{0:03d}".format(version_entity["version"])
        )

        return version_entity

    def prepare_repre(self, repre, version_entity, existing_repres_by_name, instance):
        template_data = copy.deepcopy(instance.data["anatomyData"])
        files = repre["files"]
        template_data["representation"] = repre["name"]
        template_data["ext"] = repre["ext"]
        template_data["version"] = version_entity["version"]

        for key, anatomy_key in {
            # Representation Key: Anatomy data key
            "resolutionWidth": "resolution_width",
            "resolutionHeight": "resolution_height",
            "fps": "fps",
            "outputName": "output",
            "originalBasename": "originalBasename"
        }.items():
            # Allow to take value from representation
            # if not found also consider instance.data
            value = repre.get(key)
            if value is None:
                value = instance.data.get(key)

            if value is not None:
                template_data[anatomy_key] = value

        existing = existing_repres_by_name.get(repre["name"].lower())
        repre_id = None
        if existing:
            repre_id = existing["id"]

        attr_defs = self._get_attributes_by_type(instance.context)["representation"]
        attributes = {"path": instance.data['source'], "template": "{root[work]}/{project[name]}/{hierarchy}/{folder[name]}/publish/usd/{folder[name]}_USD_v{version:0>5}.usda"}
        data = {}
        for key, value in repre.get("data", {}).items():
            if key in attr_defs:
                attributes[key] = value
            else:
                data[key] = value

        repre_doc = new_representation_entity(
            repre["name"],
            version_entity["id"],
            # files are filled afterwards
            [],
            data=data,
            attribs=attributes,
            entity_id=repre_id
        )
        update_data = None
        if repre_id is not None:
            update_data = self.prepare_changes(existing, repre_doc)


        return {
            "representation": repre_doc,
            "repre_update_data": update_data,
            "anatomy_data": template_data,
            "transfers": [],
            "published_files": [instance.data['source']]
        }

    def prepare_changes(self, old_entity, new_entity):
        """Prepare changes for entity update.

        Args:
            old_entity: Existing entity.
            new_entity: New entity.

        Returns:
            dict[str, Any]: Changes that have new entity.

        """
        changes = {}
        for key in set(new_entity.keys()):
            if key == "attrib":
                continue

            if key in new_entity and new_entity[key] != old_entity.get(key):
                changes[key] = new_entity[key]
                continue

        attrib_changes = {}
        if "attrib" in new_entity:
            for key, value in new_entity["attrib"].items():
                if value != old_entity["attrib"].get(key):
                    attrib_changes[key] = value
        if attrib_changes:
            changes["attrib"] = attrib_changes
        return changes

    def create_version_data(self, instance):
        """Create the data dictionary for the version

        Args:
            instance: the current instance being published

        Returns:
            dict: the required information for version["data"]
        """

        context = instance.context

        # create relative source path for DB
        if "source" in instance.data:
            source = instance.data["source"]
        self.log.debug("Source: {}".format(source))

        version_data = {
            "families": [instance.data["family"]],
            "time": context.data["time"],
            "author": context.data["user"],
            "source": source,
            "comment": instance.data["comment"],
            "machine": context.data.get("machine"),
            "fps": instance.data.get("fps", context.data.get("fps"))
        }

        intent_value = context.data.get("intent")
        if intent_value and isinstance(intent_value, dict):
            intent_value = intent_value.get("value")

        if intent_value:
            version_data["intent"] = intent_value

        # Include optional data if present in
        optionals = [
            "frameStart", "frameEnd", "step",
            "handleEnd", "handleStart", "sourceHashes"
        ]
        for key in optionals:
            if key in instance.data:
                version_data[key] = instance.data[key]

        # Include instance.data[versionData] directly
        version_data_instance = instance.data.get("versionData")
        if version_data_instance:
            version_data.update(version_data_instance)

        return version_data

    def get_rootless_path(self, anatomy, path):
        """Returns, if possible, path without absolute portion from root
            (eg. 'c:\' or '/opt/..')

         This information is platform dependent and shouldn't be captured.
         Example:
             'c:/projects/MyProject1/Assets/publish...' >
             '{root}/MyProject1/Assets...'

        Args:
            anatomy (Anatomy): Project anatomy.
            path (str): Absolute path.

        Returns:
            str: Path where root path is replaced by formatting string.

        """
        success, rootless_path = anatomy.find_root_template_from_path(path)
        if success:
            path = rootless_path
        else:
            self.log.warning((
                "Could not find root path for remapping \"{}\"."
                " This may cause issues on farm."
            ).format(path))
        return path

    def _get_attributes_by_type(self, context):
        attributes = context.data.get("ayonAttributes")
        if attributes is None:
            attributes = {}
            for key in (
                "project",
                "folder",
                "product",
                "version",
                "representation",
            ):
                attributes[key] = get_attributes_for_type(key)
            context.data["ayonAttributes"] = attributes
        return attributes
