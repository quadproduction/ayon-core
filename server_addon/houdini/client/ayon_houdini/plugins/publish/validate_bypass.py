# -*- coding: utf-8 -*-
import hou

import pyblish.api
from ayon_core.pipeline import PublishValidationError

from ayon_houdini.api import plugin


class ValidateBypassed(plugin.HoudiniInstancePlugin):
    """Validate all primitives build hierarchy from attribute when enabled.

    The name of the attribute must exist on the prims and have the same name
    as Build Hierarchy from Attribute's `Path Attribute` value on the Alembic
    ROP node whenever Build Hierarchy from Attribute is enabled.

    """

    order = pyblish.api.ValidatorOrder - 0.1
    families = ["*"]
    label = "Validate ROP Bypass"

    def process(self, instance):

        if len(instance) == 0:
            # Ignore instances without any nodes
            # e.g. in memory bootstrap instances
            return

        invalid = self.get_invalid(instance)
        if invalid:
            rop = invalid[0]
            raise PublishValidationError(
                ("ROP node {} is set to bypass, publishing cannot "
                 "continue.".format(rop.path())),
                title=self.label
            )

    @classmethod
    def get_invalid(cls, instance):

        rop = hou.node(instance.data["instance_node"])
        if hasattr(rop, "isBypassed") and rop.isBypassed():
            return [rop]
