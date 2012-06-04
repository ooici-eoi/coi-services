#!/usr/bin/env python

"""
@package 
@file test_registration_utility
@author Christopher Mueller
@brief 
"""

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

@attr('INT_EX',group='eoi')
class TestRegistrationUtilityInt(IonIntegrationTestCase):

    def setUp(self):
        self._start_container()
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')
        self._dams_cli = DataAcquisitionManagementServiceClient()

    @unittest.skip('')
    def test_external_dataset_registration_existing_objs(self):
        # Make a DatasetRegistration instance
        dreg=DatasetRegistration()

        # Parse the objects to register from file(s)
        objs=dreg.parse_dsreg('test_data/dataset_registration/dsreg_file_refs.yml')

        # Register the dataset - this creates and registers all objects EXCEPT the ExternalDatasetAgentInstance
        dset_id, eda_id, dproducer_id, stream_id = dreg.register_dataset(*objs)

        # Register the dataset again - this should simply return the same ID's as the first time
        dset_id2, eda_id2, dproducer_id2, stream_id2 = dreg.register_dataset(*objs)

        self.assertEquals(dset_id, dset_id2)
        self.assertEquals(eda_id, eda_id2)
        self.assertEquals(dproducer_id, dproducer_id2)
        self.assertEquals(stream_id, stream_id2)

#    @unittest.skip('')
    def test_external_dataset_registration(self):
        cc = self.container
        assertions = self.assertTrue

        #-----------------------------
        # Copy below here to run as a script (don't forget the imports of course!)
        #-----------------------------

        # Make a DatasetRegistration instance
        dreg=DatasetRegistration()

        # Parse the objects to register from file(s)
        objs=dreg.parse_dsreg('test_data/dataset_registration/dsreg_file_refs.yml')

        # Register the dataset - this creates and registers all objects EXCEPT the ExternalDatasetAgentInstance
        dset_id, eda_id, dproducer_id, stream_id = dreg.register_dataset(*objs)

#        # Setup the particular configuration(s) for this dataset
#        tx = TaxyTool()
#        tx.add_taxonomy_set('data', 'external_data')
#
#        driver_config = {'dvr_mod' : 'ion.agents.data.handlers.base_data_handler', 'dvr_cls' : 'DummyDataHandler',}
#        driver_config['dh_cfg'] = {
#            'TESTING':True,
#            'stream_id':stream_id,#TODO: This should probably be a 'stream_config' dict with stream_name:stream_id members
#            'data_producer_id':dproducer_id,
#            'taxonomy':tx.dump(), #TODO: Currently does not support sets
#            'max_records':4,
#            }
#
#        # Create agent config.
#        agent_config = {
#            'driver_config' : driver_config,
#            'stream_config' : {},
#            'agent'         : {'resource_id': ''},#resource_id set in 'dams.start_external_dataset_agent_instance'
#            'test_mode' : True
#        }
#        taxonomy=tx.dump()

        agent_config = 'test_data/dataset_registration/agent_config.yml'
        taxonomy = 'test_data/dataset_registration/taxonomy.yml'

        # Create the agent_instance
        eda_inst_id, eda_inst_obj = dreg.make_agent_instance('dummy_eda_inst', agent_config, taxonomy)

        # Start the agent
        pid = self._dams_cli.start_external_dataset_agent_instance(eda_inst_id)

        ra_cli = ResourceAgentClient(dset_id, FakeProcess())

        # Get the current state (should be unintialized)
        retval=ra_cli.execute_agent(AgentCommand(command='get_current_state'))
        log.info('Current state is: {0}'.format(retval.result))

        # Get the agent into observatory state
        retval=ra_cli.execute_agent(AgentCommand(command='initialize'))
        retval=ra_cli.execute_agent(AgentCommand(command='go_active'))
        retval=ra_cli.execute_agent(AgentCommand(command='run'))

        # Get the current state (should be observatory)
        retval=ra_cli.execute_agent(AgentCommand(command='get_current_state'))
        log.info('Current state is: {0}'.format(retval.result))

        # Start a granule logger
        cc.spawn_process(
            name='dummy_logger',
            module='ion.processes.data.stream_granule_logger',
            cls='StreamGranuleLogger',
            config={'process':{'stream_id':stream_id}}
        )

        # Get some data (repeat as much as you'd like)
        retval = ra_cli.execute(AgentCommand(command='acquire_data'))

        # Try getting data twice "concurrently" - second is rejected
        retval = ra_cli.execute(AgentCommand(command='acquire_data'));retval = ra_cli.execute(AgentCommand(command='acquire_data'))

        retval=ra_cli.execute_agent(AgentCommand(command='reset'))
