'''
@author Luke Campbell <lcampbell@asascience.com>
@file ion/services/dm/inventory/data_retriever_service.py
@description Data Retriever Service
'''

from interface.services.dm.idata_retriever_service import BaseDataRetrieverService
from interface.services.dm.ireplay_process import ReplayProcessClient
from interface.objects import Replay, ProcessDefinition, StreamDefinitionContainer
from prototype.sci_data.constructor_apis import DefinitionTree, StreamDefinitionConstructor
from pyon.core.exception import BadRequest, NotFound
from pyon.public import PRED, RT


class DataRetrieverService(BaseDataRetrieverService):

    def __init__(self, *args, **kwargs):
        super(DataRetrieverService,self).__init__(*args,**kwargs)

        self.process_definition_id = None


    def on_start(self):
        super(DataRetrieverService,self).on_start()

        res_list, _ = self.clients.resource_registry.find_resources(
            restype=RT.ProcessDefinition,
            name='data_replay_process',
            id_only=True)

        if len(res_list):
            self.process_definition_id = res_list[0]


    def on_quit(self):
        #self.clients.process_dispatcher.delete_process_definition(process_definition_id=self.process_definition_id)
        super(DataRetrieverService,self).on_quit()





    def define_replay(self, dataset_id='', query=None, delivery_format=None):
        ''' Define the stream that will contain the data from data store by streaming to an exchange name.

        '''
        # Get the datastore name from the dataset object, use dm_datastore by default.
        """
        delivery_format
            - fields
        """
        if not dataset_id:
            raise BadRequest('(Data Retriever Service %s): No dataset provided.' % self.name)

        if self.process_definition_id is None:
            res, _  = self.clients.resource_registry.find_resources(restype=RT.ProcessDefinition,name='data_replay_process',id_only=True)
            if not len(res):
                raise BadRequest('No replay process defined.')
            self.process_definition_id = res[0]

        dataset = self.clients.dataset_management.read_dataset(dataset_id=dataset_id)
        datastore_name = dataset.datastore_name
        datastore = self.container.datastore_manager.get_datastore(datastore_name)
        delivery_format = delivery_format or {}

        view_name = dataset.view_name
        key_id = dataset.primary_view_key
        # Make a new definition container


        process_definition = self.clients.resource_registry.read(self.process_definition_id)

        definition_container = None

        #---------------------------------------- WARNING ----------------------------------------
        # Warning: using absolute module path checking for reverse compatability
        # This will break if the module moves locations
        #---------------------------------------- WARNING ----------------------------------------

        replay_stream_id = ''
        if process_definition.executable['module'] == 'ion.processes.data.replay.replay_process' :

            # Make a definition
            try:
                opts = {'key':[dataset.primary_view_key,0],'include_docs':True}
                definition = datastore.query_view('datasets/dataset_by_id',opts=opts)[0]['doc']
            except IndexError:
                raise NotFound('The requested document was not located.')
            definition_container = definition

            # Tell pubsub about our definition that we want to use and setup the association so clients can figure out
            # What belongs on the stream
            definition_id = self.clients.pubsub_management.create_stream_definition(container=definition_container)
            # Make a stream
            replay_stream_id = self.clients.pubsub_management.create_stream(stream_definition_id=definition_id)
            delivery_format.update({'definition_id':definition_id})
        elif process_definition.executable['module'] == 'ion.processes.data.replay.replay_process_a':
            replay_stream_id = self.clients.pubsub_management.create_stream()




        replay = Replay()
        replay.delivery_format = delivery_format

        if definition_container is not None:
            definition_container.stream_resource_id = replay_stream_id




        replay.process_id = 0

        replay_id, rev = self.clients.resource_registry.create(replay)
        replay._id = replay_id
        replay._rev = rev
        config = {'process':{
            'query':query,
            'datastore_name':datastore_name,
            'dataset_id':dataset_id,
            'view_name':view_name,
            'key_id':key_id,
            'delivery_format':delivery_format,
            'publish_streams':{'output':replay_stream_id}
            }
        }


        pid = self.clients.process_dispatcher.schedule_process(
            process_definition_id=self.process_definition_id,
            configuration=config
        )

        replay.process_id = pid

        self.clients.resource_registry.update(replay)
        self.clients.resource_registry.create_association(replay_id, PRED.hasStream, replay_stream_id)
        return replay_id, replay_stream_id



    def start_replay(self, replay_id=''):
        """
        Problem: start_replay does not return until execute replay is complete - it is all chained rpc.
        Execute replay should be a command which is fired, not RPC???
        """

        replay = self.clients.resource_registry.read(replay_id)
        pid = replay.process_id
        cli = ReplayProcessClient(name=pid)
        cli.execute_replay()

    def cancel_replay(self, replay_id=''):
        replay = self.clients.resource_registry.read(replay_id)
        pid = replay.process_id
        self.clients.process_dispatcher.cancel_process(pid)

        for pred in [PRED.hasStream]:
            assocs = self.clients.resource_registry.find_associations(replay_id, pred, id_only=True)
            for assoc in assocs:
                self.clients.resource_registry.delete_association(assoc)

        self.clients.resource_registry.delete(replay_id)

