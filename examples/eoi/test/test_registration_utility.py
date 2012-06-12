#!/usr/bin/env python

"""
@package 
@file test_registration_utility
@author Christopher Mueller
@brief 
"""
from ion.agents.instrument.instrument_agent import InstrumentAgentState

from pyon.util.int_test import IonIntegrationTestCase
from nose.plugins.attrib import attr
import unittest

#-----------------------------
# Copy the following imports to run as a script
#-----------------------------
from pyon.public import log
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand, AgentCommandResult
from pyon.ion.granule.taxonomy import TaxyTool
from examples.eoi.registration_utility import DatasetRegistration, FakeProcess
from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.objects import DataSource, DataSourceModel, ExternalDataProvider, ExternalDataset, ExternalDatasetModel,ExternalDatasetAgent, ExternalDatasetAgentInstance, DataProduct

@attr('INT_EX',group='eoi')
class TestRegistrationUtilityInt(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
#        self.container.start_rel_from_url('res/deploy/r2deploy_no_bootstrap.yml')
        self._dams_cli = DataAcquisitionManagementServiceClient()

    @unittest.skip('')
    def test_external_dataset_registration_existing_objs(self):
        # Make a DatasetRegistration instance
        dreg=DatasetRegistration()

        # Register the dataset - this creates and registers all objects EXCEPT the ExternalDatasetAgentInstance
        ds_obj_dict = dreg.register_dataset('dummy_1', 'test_data/dataset_registration/dummy_test.dsreg')

        # Register the dataset again - this should simply return the same ID's as the first time
        ds_obj_dict2 = dreg.register_dataset('dummy_2', 'test_data/dataset_registration/dummy_test.dsreg')

        self.assertEquals(ds_obj_dict['dset'][0], ds_obj_dict2['dset'][0])
        self.assertEquals(ds_obj_dict['eda'][0], ds_obj_dict2['eda'][0])
        self.assertEquals(ds_obj_dict['dproducer_id'], ds_obj_dict2['dproducer_id'])
        self.assertEquals(ds_obj_dict['stream_id'], ds_obj_dict2['stream_id'])

#    @unittest.skip('')
    def test_external_dataset_registration(self):
        cc = self.container

        # Make a DatasetRegistration instance
        dreg=DatasetRegistration()

        # Reference the particular configuration(s) for this dataset
        obj_ref_file = 'test_data/dataset_registration/dummy_test.dsreg'

        # Register the dataset - this creates and registers all objects EXCEPT the ExternalDatasetAgentInstance
        dset_obj_dict = dreg.register_dataset('dummy_agent_instance', obj_ref_file)

        # Verify that all required objects are present in the resource registry
        rr_cli = ResourceRegistryServiceClient()
        objs = {
            'dset':ExternalDataset,
            'dset_mdl':ExternalDatasetModel,
            'dsrc':DataSource,
            'dsrc_mdl':DataSourceModel,
            'eda':ExternalDatasetAgent,
            'edp':ExternalDataProvider,
            'dprod':DataProduct
        }
        for key, cls in objs.iteritems():
            obj_id = dset_obj_dict[key][0]
            obj = rr_cli.read(obj_id)
            self.assertIsInstance(obj,cls)


#    @unittest.skip('')
    def test_register_start_and_command_agent(self):
        cc = self.container

        # Make a DatasetRegistration instance
        dreg=DatasetRegistration()

        # Reference the particular configuration(s) for this dataset
        obj_ref_file = 'test_data/dataset_registration/seab_ruv.dsreg'

        # Register the dataset - this creates and registers all objects EXCEPT the ExternalDatasetAgentInstance
        dset_obj_dict = dreg.register_dataset('dummy_eda_inst', obj_ref_file)

        # Start the agent
        pid = self._dams_cli.start_external_dataset_agent_instance(dset_obj_dict['eda_inst_id'])

        # Create a client to control the agent
        ra_cli = ResourceAgentClient(dset_obj_dict['dset'][0], FakeProcess())

        # Get the current state (should be unintialized)
        retval=ra_cli.execute_agent(AgentCommand(command='get_current_state'))
        log.info('Current state is: {0}'.format(retval.result))
        self.assertEqual(retval.result, InstrumentAgentState.UNINITIALIZED)

        # Get the agent into observatory state
        retval=ra_cli.execute_agent(AgentCommand(command='initialize'))
        retval=ra_cli.execute_agent(AgentCommand(command='go_active'))
        retval=ra_cli.execute_agent(AgentCommand(command='run'))

        # Get the current state (should be observatory)
        retval=ra_cli.execute_agent(AgentCommand(command='get_current_state'))
        log.info('Current state is: {0}'.format(retval.result))
        self.assertEqual(retval.result, InstrumentAgentState.OBSERVATORY)

        # Start a granule logger
        cc.spawn_process(
            name='dummy_logger',
            module='ion.processes.data.stream_granule_logger',
            cls='StreamGranuleLogger',
            config={'process':{'stream_id':dset_obj_dict['stream_id']}}
        )

        # Get some data (repeat as much as you'd like)
        retval = ra_cli.execute(AgentCommand(command='acquire_data'))

        # Try getting data twice "concurrently" - second one is rejected
        retval = ra_cli.execute(AgentCommand(command='acquire_data'));retval = ra_cli.execute(AgentCommand(command='acquire_data'))



        # Reset the agent to 'uninitialized'
        retval=ra_cli.execute_agent(AgentCommand(command='reset'))
        retval=ra_cli.execute_agent(AgentCommand(command='get_current_state'))
        log.info('Current state is: {0}'.format(retval.result))
        self.assertEqual(retval.result, InstrumentAgentState.UNINITIALIZED)
