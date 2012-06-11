#!/usr/bin/env python

"""
@package  ion.services.sa.resource_impl.sensor_device_impl
@author   Ian Katz
"""

#from pyon.core.exception import BadRequest, NotFound
from pyon.public import PRED, RT, LCE

from ion.services.sa.resource_impl.resource_impl import ResourceImpl
from ion.services.sa.instrument.policy import DevicePolicy

class SensorDeviceImpl(ResourceImpl):
    """
    @brief resource management for SensorDevice resources
    """

    def _primary_object_name(self):
        return RT.SensorDevice

    def _primary_object_label(self):
        return "sensor_device"

    def on_simpl_init(self):
        self.policy = DevicePolicy(self.clients)

        self.add_lce_precondition(LCE.PLAN, self.use_policy(self.policy.lce_precondition_plan))
        self.add_lce_precondition(LCE.DEVELOP, self.use_policy(self.policy.lce_precondition_develop))
        self.add_lce_precondition(LCE.INTEGRATE, self.use_policy(self.policy.lce_precondition_integrate))
        self.add_lce_precondition(LCE.DEPLOY, self.use_policy(self.policy.lce_precondition_deploy))
        self.add_lce_precondition(LCE.RETIRE, self.use_policy(self.policy.lce_precondition_retire))

    def link_model(self, sensor_device_id='', sensor_model_id=''):
        return self._link_resources(sensor_device_id, PRED.hasModel, sensor_model_id)

    def unlink_model(self, sensor_device_id='', sensor_model_id=''):
        return self._unlink_resources(sensor_device_id, PRED.hasModel, sensor_model_id)

    def find_having_model(self, sensor_model_id):
        return self._find_having(PRED.hasModel, sensor_model_id)

    def find_stemming_model(self, sensor_device_id):
        return self._find_stemming(sensor_device_id, PRED.hasModel, RT.SensorModel)
