#!/usr/bin/env python

"""
@package ion.agents.data.test.test_external_dataset_agent
@file ion/agents/data/test/test_external_dataset_agent.py
@author Tim Giguere
@author Christopher Mueller
@brief Test cases for R2 ExternalDatasetAgent

# bin/nosetests -s -v --nologcapture ion.agents.data.test.test_external_dataset_agent:TestExternalDatasetAgent.test_acquire_data
# bin/nosetests -s -v --nologcapture ion.agents.data.test.test_external_dataset_agent:TestExternalDatasetAgent.test_acquire_data_while_streaming
# bin/nosetests -s -v --nologcapture ion.agents.data.test.test_external_dataset_agent:TestExternalDatasetAgent.test_acquire_sample
# bin/nosetests -s -v --nologcapture ion.agents.data.test.test_external_dataset_agent:TestExternalDatasetAgent.test_streaming
# bin/nosetests -s -v --nologcapture ion.agents.data.test.test_external_dataset_agent:TestExternalDatasetAgent.test_observatory
# bin/nosetests -s -v --nologcapture ion.agents.data.test.test_external_dataset_agent:TestExternalDatasetAgent.test_get_set_param
# bin/nosetests -s -v --nologcapture ion.agents.data.test.test_external_dataset_agent:TestExternalDatasetAgent.test_initialize
# bin/nosetests -s -v --nologcapture ion.agents.data.test.test_external_dataset_agent:TestExternalDatasetAgent.test_states
# bin/nosetests -s -v --nologcapture ion.agents.data.test.test_external_dataset_agent:TestExternalDatasetAgent.test_capabilities
# bin/nosetests -s -v --nologcapture ion.agents.data.test.test_external_dataset_agent:TestExternalDatasetAgent.test_errors

"""

# Import pyon first for monkey patching.
from pyon.public import log, CFG
from pyon.core.exception import InstParameterError, NotFound
# Standard imports.

# 3rd party imports.
from gevent import spawn
from gevent.event import AsyncResult
import gevent
from nose.plugins.attrib import attr
from mock import patch
import unittest

# ION imports.
from interface.objects import StreamQuery, Attachment, AttachmentType, Granule
from interface.services.icontainer_agent import ContainerAgentClient
from interface.services.dm.ipubsub_management_service import PubsubManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from pyon.public import StreamSubscriberRegistrar
from prototype.sci_data.stream_defs import ctd_stream_definition
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand
from pyon.util.containers import get_safe
from pyon.util.int_test import IonIntegrationTestCase
from pyon.util.context import LocalContextMixin
from pyon.event.event import EventSubscriber

# MI imports
from ion.agents.instrument.instrument_agent import InstrumentAgentState
from ion.agents.instrument.exceptions import InstrumentParameterException

# todo: rethink this
from ion.agents.data.handlers.base_data_handler import PACKET_CONFIG

from pyon.ion.granule.taxonomy import TaxyTool


#########################
# For Validation Purposes
#########################

# Used to validate param config retrieved from driver.
PARAMS = {
    'POLLING_INTERVAL':int,
    'PATCHABLE_CONFIG_KEYS':list
}

# To validate the list of resource commands
CMDS = {
    'acquire_data':str,
    'start_autosample':str,
    'stop_autosample':str
}

# To validate the list of agent commands
AGT_CMDS = {
    'clear':str,
    'end_transaction':str,
    'get_current_state':str,
    'go_active':str,
    'go_direct_access':str,
    'go_inactive':str,
    'go_observatory':str,
    'go_streaming':str,
    'initialize':str,
    'pause':str,
    'power_down':str,
    'power_up':str,
    'reset':str,
    'resume':str,
    'run':str,
    'start_transaction':str,
    }

class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''

@patch.dict(CFG, {'endpoint':{'receive':{'timeout': 60}}})
class ExternalDatasetAgentTestBase(object):

    # Agent parameters.
    EDA_RESOURCE_ID = '123xyz'
    EDA_NAME = 'ExampleEDA'
    EDA_MOD = 'ion.agents.data.external_dataset_agent'
    EDA_CLS = 'ExternalDatasetAgent'


    """
    Test cases for instrument agent class. Functions in this class provide
    instrument agent integration tests and provide a tutorial on use of
    the agent setup and interface.
    """
    def setUp(self):
        """
        Initialize test members.
        """

#        log.warn('Starting the container')
        # Start container.
        self._start_container()

        # Bring up services in a deploy file
#        log.warn('Starting the rel')
        self.container.start_rel_from_url('res/deploy/r2deploy.yml')

        # Create a pubsub client to create streams.
#        log.warn('Init a pubsub client')
        self._pubsub_client = PubsubManagementServiceClient(node=self.container.node)
#        log.warn('Init a ContainerAgentClient')
        self._container_client = ContainerAgentClient(node=self.container.node, name=self.container.name)

        # Data async and subscription  TODO: Replace with new subscriber
        self._finished_count = None
        #TODO: Switch to gevent.queue.Queue
        self._async_finished_result = AsyncResult()
        self._finished_events_received = []
        self._finished_event_subscriber = None
        self._start_finished_event_subscriber()
        self.addCleanup(self._stop_finished_event_subscriber)

        # TODO: Finish dealing with the resources and whatnot
        # TODO: DVR_CONFIG and (potentially) stream_config could both be reconfigured in self._setup_resources()
        self._setup_resources()

        #TG: Setup/configure the granule logger to log granules as they're published

        # Create agent config.
        agent_config = {
            'driver_config' : self.DVR_CONFIG,
            'stream_config' : {},
            'agent'         : {'resource_id': self.EDA_RESOURCE_ID},
            'test_mode' : True
        }

        # Start instrument agent.
        self._ia_pid = None
        log.debug('TestInstrumentAgent.setup(): starting EDA.')
        self._ia_pid = self._container_client.spawn_process(
            name=self.EDA_NAME,
            module=self.EDA_MOD,
            cls=self.EDA_CLS,
            config=agent_config
        )
        log.info('Agent pid=%s.', str(self._ia_pid))

        # Start a resource agent client to talk with the instrument agent.
        self._ia_client = None
        self._ia_client = ResourceAgentClient(self.EDA_RESOURCE_ID, process=FakeProcess())
        log.info('Got ia client %s.', str(self._ia_client))

    ########################################
    # Private "setup" functions
    ########################################

    def _setup_resources(self):
        raise NotImplementedError('_setup_resources must be implemented in the subclass')

    def create_stream_and_logger(self, name, stream_id=''):
        if not stream_id or stream_id is '':
            stream_id = self._pubsub_client.create_stream(name=name, encoding='ION R2')

        pid = self._container_client.spawn_process(
            name=name+'_logger',
            module='ion.processes.data.stream_granule_logger',
            cls='StreamGranuleLogger',
            config={'process':{'stream_id':stream_id}}
        )
        log.info('Started StreamGranuleLogger \'{0}\' subscribed to stream_id={1}'.format(pid, stream_id))

        return stream_id

    def _start_finished_event_subscriber(self):

        def consume_event(*args,**kwargs):
            if args[0].description == 'TestingFinished':
                log.debug('TestingFinished event received')
                self._finished_events_received.append(args[0])
                if self._finished_count and self._finished_count == len(self._finished_events_received):
                    log.debug('Finishing test...')
                    self._async_finished_result.set(len(self._finished_events_received))
                    log.debug('Called self._async_finished_result.set({0})'.format(len(self._finished_events_received)))

        self._finished_event_subscriber = EventSubscriber(event_type='DeviceEvent', callback=consume_event)
        self._finished_event_subscriber.activate()

    def _stop_finished_event_subscriber(self):
        if self._finished_event_subscriber:
            self._finished_event_subscriber.deactivate()
            self._finished_event_subscriber = None


    ########################################
    # Custom assertion functions
    ########################################

    def assertListsEqual(self, lst1, lst2):
        lst1.sort()
        lst2.sort()
        return lst1 == lst2

    def assertSampleDict(self, val):
        """
        Verify the value is a sample dictionary for the sbe37.
        """
        #{'p': [-6.945], 'c': [0.08707], 't': [20.002], 'time': [1333752198.450622]}
        self.assertTrue(isinstance(val, dict))
        self.assertTrue(val.has_key('c'))
        self.assertTrue(val.has_key('t'))
        self.assertTrue(val.has_key('p'))
        self.assertTrue(val.has_key('time'))
        c = val['c'][0]
        t = val['t'][0]
        p = val['p'][0]
        time = val['time'][0]

        self.assertTrue(isinstance(c, float))
        self.assertTrue(isinstance(t, float))
        self.assertTrue(isinstance(p, float))
        self.assertTrue(isinstance(time, float))

    def assertParamDict(self, pd, all_params=False):
        """
        Verify all device parameters exist and are correct type.
        """
        if all_params:
            self.assertEqual(set(pd.keys()), set(PARAMS.keys()))
            for (key, type_val) in PARAMS.iteritems():
                if type_val == list or type_val == tuple:
                    self.assertTrue(isinstance(pd[key], (list, tuple)))
                else:
                    self.assertTrue(isinstance(pd[key], type_val))

        else:
            for (key, val) in pd.iteritems():
                self.assertTrue(PARAMS.has_key(key))
                self.assertTrue(isinstance(val, PARAMS[key]))

    def assertParamVals(self, params, correct_params):
        """
        Verify parameters take the correct values.
        """
        self.assertEqual(set(params.keys()), set(correct_params.keys()))
        for (key, val) in params.iteritems():
            correct_val = correct_params[key]
            if isinstance(val, float):
                # Verify to 5% of the larger value.
                max_val = max(abs(val), abs(correct_val))
                self.assertAlmostEqual(val, correct_val, delta=max_val*.01)

            elif isinstance(val, (list, tuple)):
                # list of tuple.
                self.assertEqual(list(val), list(correct_val))

            else:
                # int, bool, str.
                self.assertEqual(val, correct_val)


    ########################################
    # Test functions
    ########################################

    def test_acquire_data(self):
        cmd=AgentCommand(command='initialize')
        _ = self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command='go_active')
        _ = self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command='run')
        _ = self._ia_client.execute_agent(cmd)

        self._finished_count = 3

        log.info('Send an unconstrained request for data (\'new data\')')
        cmd = AgentCommand(command='acquire_data')
        self._ia_client.execute(cmd)

        log.info('Send a second unconstrained request for data (\'new data\'), should be rejected')
        cmd = AgentCommand(command='acquire_data')
        self._ia_client.execute(cmd)

        config_mods={}

        log.info('Send a constrained request for data: constraints = HIST_CONSTRAINTS_1')
        config_mods['stream_id'] = self.create_stream_and_logger(name='stream_id_for_historical_1')
        config_mods['constraints']=self.HIST_CONSTRAINTS_1
        cmd = AgentCommand(command='acquire_data', args=[config_mods])
        self._ia_client.execute(cmd)

        log.info('Send a second constrained request for data: constraints = HIST_CONSTRAINTS_2')
        config_mods['stream_id'] = self.create_stream_and_logger(name='stream_id_for_historical_2')
        config_mods['constraints']=self.HIST_CONSTRAINTS_2
#        config={'stream_id':'second_historical','TESTING':True, 'constraints':self.HIST_CONSTRAINTS_2}
        cmd = AgentCommand(command='acquire_data', args=[config_mods])
        self._ia_client.execute(cmd)

        finished = self._async_finished_result.get(timeout=10)
        self.assertEqual(finished,self._finished_count)

        cmd = AgentCommand(command='reset')
        _ = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

    def test_acquire_data_while_streaming(self):
        # Test instrument driver execute interface to start and stop streaming mode.
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.INACTIVE)

        cmd = AgentCommand(command='go_active')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        # Make sure the polling interval is appropriate for a test
        params = {
            'POLLING_INTERVAL':5
        }
        self._ia_client.set_param(params)

        self._finished_count = 2

        # Begin streaming.
        cmd = AgentCommand(command='go_streaming')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.STREAMING)

        config = get_safe(self.DVR_CONFIG, 'dh_cfg', {})

        log.info('Send a constrained request for data: constraints = HIST_CONSTRAINTS_1')
        config['stream_id'] = self.create_stream_and_logger(name='stream_id_for_historical_1')
        config['constraints']=self.HIST_CONSTRAINTS_1
        cmd = AgentCommand(command='acquire_data', args=[config])
        reply = self._ia_client.execute(cmd)
        self.assertNotEqual(reply.status, 660)

        gevent.sleep(12)

        # Halt streaming.
        cmd = AgentCommand(command='go_observatory')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        # Assert that data was received
        self._async_finished_result.get(timeout=10)
        self.assertTrue(len(self._finished_events_received) >= 3)

        cmd = AgentCommand(command='reset')
        _ = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

    def test_streaming(self):
        # Test instrument driver execute interface to start and stop streaming mode.
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.INACTIVE)

        cmd = AgentCommand(command='go_active')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        # Make sure the polling interval is appropriate for a test
        params = {
            'POLLING_INTERVAL':5
        }
        self._ia_client.set_param(params)

        self._finished_count = 3

        # Begin streaming.
        cmd = AgentCommand(command='go_streaming')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.STREAMING)

        # Wait for some samples to roll in.
        gevent.sleep(12)

        # Halt streaming.
        cmd = AgentCommand(command='go_observatory')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        # Assert that data was received
        self._async_finished_result.get(timeout=10)
        self.assertTrue(len(self._finished_events_received) >= 3)

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

    def test_observatory(self):
        # Test instrument driver get and set interface.

        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.INACTIVE)

        cmd = AgentCommand(command='go_active')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        # Retrieve all resource parameters.
        reply = self._ia_client.get_param('DRIVER_PARAMETER_ALL')
        self.assertParamDict(reply, True)
        orig_config = reply

        ## Retrieve a subset of resource parameters.
        params = [
            'POLLING_INTERVAL'
        ]
        reply = self._ia_client.get_param(params)
        self.assertParamDict(reply)
        orig_params = reply

        # Set a subset of resource parameters.
        new_params = {
            'POLLING_INTERVAL' : (orig_params['POLLING_INTERVAL'] * 2),
        }
        self._ia_client.set_param(new_params)
        check_new_params = self._ia_client.get_param(params)
        self.assertParamVals(check_new_params, new_params)

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

    def test_get_set_param(self):
        cmd=AgentCommand(command='initialize')
        _ = self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command='go_active')
        _ = self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command='run')
        _ = self._ia_client.execute_agent(cmd)

        # Get a couple parameters
        retval = self._ia_client.get_param(['POLLING_INTERVAL','PATCHABLE_CONFIG_KEYS'])
        log.debug('Retrieved parameters from agent: {0}'.format(retval))
        self.assertTrue(isinstance(retval,dict))
        self.assertEqual(type(retval['POLLING_INTERVAL']),int)
        self.assertEqual(type(retval['PATCHABLE_CONFIG_KEYS']),list)

        # Attempt to get a parameter that doesn't exist
        log.debug('Try getting a non-existent parameter \'BAD_PARAM\'')
        self.assertRaises(InstParameterError, self._ia_client.get_param,['BAD_PARAM'])

        # Set the polling_interval to a new value, then get it to make sure it set properly
        self._ia_client.set_param({'POLLING_INTERVAL':10})
        retval = self._ia_client.get_param(['POLLING_INTERVAL'])
        log.debug('Retrieved parameters from agent: {0}'.format(retval))
        self.assertTrue(isinstance(retval,dict))
        self.assertEqual(retval['POLLING_INTERVAL'],10)

        # Attempt to set a parameter that doesn't exist
        log.debug('Try setting a non-existent parameter \'BAD_PARAM\'')
        self.assertRaises(InstParameterError, self._ia_client.set_param, {'BAD_PARAM':'bad_val'})

        # Attempt to set one parameter that does exist, and one that doesn't
        self.assertRaises(InstParameterError, self._ia_client.set_param, {'POLLING_INTERVAL':20,'BAD_PARAM':'bad_val'})

        retval = self._ia_client.get_param(['POLLING_INTERVAL'])
        log.debug('Retrieved parameters from agent: {0}'.format(retval))
        self.assertTrue(isinstance(retval,dict))
        self.assertEqual(retval['POLLING_INTERVAL'],20)

        cmd = AgentCommand(command='reset')
        _ = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

    def test_initialize(self):
        # Test agent initialize command. This causes creation of driver process and transition to inactive.

        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.INACTIVE)

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)

        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

    def test_states(self):
        # Test agent state transitions.

        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.INACTIVE)

        cmd = AgentCommand(command='go_active')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        cmd = AgentCommand(command='pause')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.STOPPED)

        cmd = AgentCommand(command='resume')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        cmd = AgentCommand(command='clear')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        cmd = AgentCommand(command='pause')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.STOPPED)

        cmd = AgentCommand(command='clear')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        self._finished_count = 1

        cmd = AgentCommand(command='go_streaming')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.STREAMING)

        gevent.sleep(5)

        cmd = AgentCommand(command='go_observatory')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        self._async_finished_result.get(timeout=5)

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

    def test_capabilities(self):
        # Test the ability to retrieve agent and resource parameter and command capabilities.
        acmds = self._ia_client.get_capabilities(['AGT_CMD'])
        log.debug('Agent Commands: {0}'.format(acmds))
        acmds = [item[1] for item in acmds]
        self.assertListsEqual(acmds, AGT_CMDS.keys())
        apars = self._ia_client.get_capabilities(['AGT_PAR'])
        log.debug('Agent Parameters: {0}'.format(apars))

        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.INACTIVE)

        rcmds = self._ia_client.get_capabilities(['RES_CMD'])
        log.debug('Resource Commands: {0}'.format(rcmds))
        rcmds = [item[1] for item in rcmds]
        self.assertListsEqual(rcmds, CMDS.keys())

        rpars = self._ia_client.get_capabilities(['RES_PAR'])
        log.debug('Resource Parameters: {0}'.format(rpars))
        rpars = [item[1] for item in rpars]
        self.assertListsEqual(rpars, PARAMS.keys())

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

    def test_errors(self):
        # Test illegal behavior and replies.

        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

        # Can't go active in unitialized state.
        # Status 660 is state error.
        cmd = AgentCommand(command='go_active')
        retval = self._ia_client.execute_agent(cmd)
        log.info('GO ACTIVE CMD %s',str(retval))
        self.assertEquals(retval.status, 660)

        # Can't command driver in this state.
        cmd = AgentCommand(command='acquire_sample')
        reply = self._ia_client.execute(cmd)
        self.assertEqual(reply.status, 660)

        cmd = AgentCommand(command='initialize')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.INACTIVE)

        cmd = AgentCommand(command='go_active')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.IDLE)

        cmd = AgentCommand(command='run')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.OBSERVATORY)

        # 404 unknown agent command.
        cmd = AgentCommand(command='kiss_edward')
        retval = self._ia_client.execute_agent(cmd)
        self.assertEquals(retval.status, 404)

        # 670 unknown driver command.
        cmd = AgentCommand(command='acquire_sample_please')
        retval = self._ia_client.execute(cmd)
        self.assertEqual(retval.status, 670)

        # 630 Parameter error.
        self.assertRaises(InstParameterError, self._ia_client.get_param, 'bogus bogus')

        cmd = AgentCommand(command='reset')
        retval = self._ia_client.execute_agent(cmd)
        cmd = AgentCommand(command='get_current_state')
        retval = self._ia_client.execute_agent(cmd)
        state = retval.result
        self.assertEqual(state, InstrumentAgentState.UNINITIALIZED)

@attr('INT', group='eoi')
class TestExternalDatasetAgent(ExternalDatasetAgentTestBase, IonIntegrationTestCase):
    # DataHandler config
    DVR_CONFIG = {
        'dvr_mod' : 'ion.agents.data.handlers.base_data_handler',
        'dvr_cls' : 'DummyDataHandler',
        }

    # Constraints dict
    HIST_CONSTRAINTS_1 = {
        'array_len':15,
        }
    HIST_CONSTRAINTS_2 = {
        'array_len':10,
        }

    NDC = {
    }

    def _setup_resources(self):
        stream_id = self.create_stream_and_logger(name='dummydata_stream')

        tx = TaxyTool()
        tx.add_taxonomy_set('data', 'external_data')
        self.DVR_CONFIG['dh_cfg'] = {
            'TESTING':True,
            'stream_id':stream_id,#TODO: This should probably be a 'stream_config' dict with stream_name:stream_id members
            'data_producer_id':'dummy_data_producer_id',
            'taxonomy':tx.dump(),
            'max_records':4,
            }

@attr('INT_LONG', group='eoi')
class TestExternalDatasetAgent_Fibonacci(ExternalDatasetAgentTestBase, IonIntegrationTestCase):
    DVR_CONFIG = {
        'dvr_mod' : 'ion.agents.data.handlers.base_data_handler',
        'dvr_cls' : 'FibonacciDataHandler',
    }

    HIST_CONSTRAINTS_1 = {
        'count':15,
    }

    HIST_CONSTRAINTS_2 = {
        'count':10,
    }

    NDC = {
    }

    def _setup_resources(self):
        stream_id = self.create_stream_and_logger(name='fibonacci_stream')
        tx = TaxyTool()
        tx.add_taxonomy_set('data', 'external_data')
        #TG: Build TaxonomyTool & add to dh_cfg.taxonomy
        self.DVR_CONFIG['dh_cfg'] = {
            'TESTING':True,
            'stream_id':stream_id,
            'data_producer_id':'fibonacci_data_producer_id',
            'taxonomy':tx.dump(),
            'max_records':4,
            }

