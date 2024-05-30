import os
import pyblish.api
import maya.cmds as cmds

from ayon_core.pipeline.publish import (
    RepairContextAction,
    PublishValidationError,
    OptionalPyblishPluginMixin
)
from ayon_maya.api.plugin import MayaInstancePlugin


class ValidateLoadedPlugin(MayaInstancePlugin,
                           OptionalPyblishPluginMixin):
    """Ensure there are no unauthorized loaded plugins"""

    label = "Loaded Plugin"
    order = pyblish.api.ValidatorOrder
    actions = [RepairContextAction]
    optional = True

    @classmethod
    def get_invalid(cls, context):

        invalid = []
        loaded_plugin = cmds.pluginInfo(query=True, listPlugins=True)
        # get variable from AYON settings
        whitelist_native_plugins = cls.whitelist_native_plugins
        authorized_plugins = cls.authorized_plugins or []

        for plugin in loaded_plugin:
            if not whitelist_native_plugins and os.getenv('MAYA_LOCATION') \
                    in cmds.pluginInfo(plugin, query=True, path=True):
                continue
            if plugin not in authorized_plugins:
                invalid.append(plugin)

        return invalid

    def process(self, context):
        if not self.is_active(context.data):
            return
        invalid = self.get_invalid(context)
        if invalid:
            raise PublishValidationError(
                "Found forbidden plugin name: {}".format(", ".join(invalid))
            )

    @classmethod
    def repair(cls, context):
        """Unload forbidden plugins"""

        for plugin in cls.get_invalid(context):
            cmds.pluginInfo(plugin, edit=True, autoload=False)
            cmds.unloadPlugin(plugin, force=True)
