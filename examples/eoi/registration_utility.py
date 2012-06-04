#!/usr/bin/env python

"""
@package 
@file registration_utility
@author Christopher Mueller
@brief 
"""

from pyon.public import log
from pyon.core.bootstrap import obj_registry, IonObject
from pyon.core.object import ion_serializer, IonObjectDeserializer
from pyon.ion.resource import PRED, RT
from pyon.core.exception import NotFound, BadRequest
from pyon.util.containers import get_safe

from interface.objects import DataSource, DataSourceModel, ExternalDataProvider, ExternalDataset, ExternalDatasetModel,ExternalDatasetAgent, ExternalDatasetAgentInstance, DataProduct

from interface.services.sa.idata_acquisition_management_service import DataAcquisitionManagementServiceClient
from interface.services.sa.idata_product_management_service import DataProductManagementServiceClient
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient

import yaml
from pyon.util.context import LocalContextMixin

ion_deserializer=IonObjectDeserializer(obj_registry=obj_registry)

class DatasetRegistration(object):

    def __init__(self):
        self._dams_cli = DataAcquisitionManagementServiceClient()
        self._dpms_cli = DataProductManagementServiceClient()
        self._rr_cli = ResourceRegistryServiceClient()
        self.outfile = 'test_data/dataset_registration/test_out_obj.yml'
        self.object_dict = {}

    def _write_yaml(self, filepath=None, ser_obj=None):
        filepath = filepath or self.outfile
        log.debug('Serialized object: {0}'.format(ser_obj))
        with open(filepath,'w') as f:
            log.debug('Write the object to file \'{0}\''.format(filepath))
            yaml.dump(ser_obj, f, indent=2)

    def _load_yaml(self, filepath=None):
        filepath = filepath or self.outfile

        obj_read = None
        with open(filepath,'r') as f:
            log.debug('Read the object from file \'{0}\''.format(filepath))
            obj_read = yaml.load(f)

        if obj_read is None:
            raise Exception('obj_read is None')

        return obj_read

    def _serialize(self, ion_obj=None):
        log.debug('Serialize the object: {0}'.format(ion_obj))
        ser_obj = ion_serializer.serialize(ion_obj)
        return ser_obj

    def _ionize(self, yaml_obj=None):
        log.debug('Ionize object: {0}'.format(yaml_obj))
        try:
            ion_obj = ion_deserializer.deserialize(yaml_obj)
            log.debug('Resulting object: {0}'.format(ion_obj))
        except NotFound as nf:
            ion_obj = None
            log.debug('Resource not found: {0}'.format(nf.message))

        return ion_obj

    def _verify_objs(self, dset_obj=None, dset_mdl_obj=None, dsrc_obj=None, dsrc_mdl_obj=None, edp_obj=None, eda_obj=None, eda_inst_obj=None, dprod_obj=None):
        if not dset_obj or not isinstance(dset_obj, ExternalDataset):
            raise Exception('Must provide an ExternalDataset')

        if not dset_mdl_obj or not isinstance(dset_mdl_obj, ExternalDatasetModel):
            raise Exception('Must provide an ExternalDatasetModel')

        if not dsrc_obj or not isinstance(dsrc_obj, DataSource):
            raise Exception('Must provide an DataSource')

        if not dsrc_mdl_obj or not isinstance(dsrc_mdl_obj, DataSourceModel):
            raise Exception('Must provide an DataSourceModel')

        if not edp_obj or not isinstance(edp_obj, ExternalDataProvider):
            raise Exception('Must provide an ExternalDataProvider')

        if not eda_obj or not isinstance(eda_obj, ExternalDatasetAgent):
            raise Exception('Must provide an ExternalDatasetAgent')

        if not eda_inst_obj or not isinstance(eda_inst_obj, ExternalDatasetAgentInstance):
            raise Exception('Must provide an ExternalDatasetAgentInstance')

        if not dprod_obj or not isinstance(dprod_obj, DataProduct):
            raise Exception('Must provide an DataProduct')


    def write_dsreg(self, filepath=None, dset_obj=None, dsrc_obj=None, edp_obj=None):
        # TODO: Make this work for passed ID's too --> read object from registry and then serialize
        # TODO: Have the ability to write each object to a separate file

        if filepath is None:
            raise Exception('Must provide an output file path')

        self._verify_objs(dset_obj, dsrc_obj, edp_obj)

        dout={}
        dout['dset'] = self._serialize(dset_obj)
        dout['dsrc'] = self._serialize(dsrc_obj)
        dout['edp'] = self._serialize(edp_obj)

        self._write_yaml(filepath, dout)

    def _unique_object_helper(self, ion_obj):
        # Ensure the passed object is an IonObject
#        if not isinstance(ion_obj, IonObject):
#            raise Exception('Object is not an ion object: {0}'.format(ion_obj))

        # Get all objects from the RR that have the type and name of the passed object
        objs, ids = self._rr_cli.find_resources(restype=ion_obj.type_, name=ion_obj.name)
        log.warn('Found objects: {0}'.format(objs))

        # We're assuming names are unique - so if there's an object in the list, it's ours
        if len(objs) > 0:
            ion_obj=objs[0]

#        # Iterate the objects found
#        for obj in objs:
#            if obj == ion_obj: # TODO: This won't EVER work because of all the fields that get created/updated on rr.create
#                # If one is equivalent to ion_obj, set ion_obj equal to it and exit the loop
#                log.warn('Equivalent object found in repository - use that one!!')
#                ion_obj = obj
#                break

        return ion_obj

    def _parse_helper(self, dobj, key, obj_name, append=None):
        if append is None:
            append = ' or \'{0}_id\''.format(key)

        if key in dobj:
            log.debug('Found {0} object'.format(key))
            ion_obj = self._ionize(dobj[key])
        elif '{0}_file'.format(key) in dobj:
            f_root = ''
            if 'file_root' in dobj:
                f_root = dobj['file_root']
            log.debug('Found {0} file'.format(key))
            #TODO, load and parse the file, return the object
            fp = f_root + dobj['{0}_file'.format(key)]
            yaml_obj = self._load_yaml(fp)
            ion_obj = self._ionize(yaml_obj)
        elif not key is 'dset' and '{0}_id'.format(key) in dobj:
            log.debug('Found {0} resource_id'.format(key))
            # TODO: Read from registry
            ion_obj = None
        else:
            raise Exception('{0} missing from registration file: key=\'{1}\'{2}'.format(obj_name,key,append))

        return ion_obj

    def parse_dsreg(self, filepath=None):
        if filepath is None:
            raise Exception('Must provide an input file path')

        # Get the dict obj from yaml
        dobj=self._load_yaml(filepath)

        # Find the dset, dsrc, dsrc_mdl, edp members
        dset_obj = self._parse_helper(dobj, 'dset', 'ExternalDataset', '')

        dset_mdl_obj = self._parse_helper(dobj, 'dset_mdl', 'ExternalDatasetModel')

        dsrc_obj = self._parse_helper(dobj, 'dsrc', 'DataSource')

        dsrc_mdl_obj = self._parse_helper(dobj, 'dsrc_mdl', 'DataSourceModel')

        edp_obj = self._parse_helper(dobj, 'edp', 'ExternalDataProvider')

        eda_obj = self._parse_helper(dobj, 'eda', 'ExternalDataAgent')

#        eda_inst_obj = self._parse_helper(dobj, 'eda_inst', 'ExternalDataAgentInstance')

        dprod_obj = self._parse_helper(dobj, 'dprod', 'DataProduct')


        return dset_obj, dset_mdl_obj, dsrc_obj, dsrc_mdl_obj, edp_obj, eda_obj, dprod_obj #eda_inst_obj, dprod_obj

    def _conditional_create(self, create_method, ion_obj, *args):

        ion_obj = self._unique_object_helper(ion_obj)
        preexisting = False

        if hasattr(ion_obj, '_id') and ion_obj._id:
            log.debug('Object has non-empty _id attribute: {0}'.format(ion_obj._id))
            id = ion_obj._id
            preexisting = True
        else:
            log.debug('Call \'{0}\' to create object {1} with args {2}'.format(create_method, ion_obj, args))
            id = create_method(ion_obj, *args)

        return id, preexisting

    def register_dataset(self, dset_obj=None, dset_mdl_obj=None, dsrc_obj=None, dsrc_mdl_obj=None, edp_obj=None, eda_obj=None, dprod_obj=None): #eda_inst_obj=None, dprod_obj=None):
#        self._verify_objs(dset_obj, dsrc_obj, edp_obj)

        ## Run everything through DAMS
        dset_mdl_id, preexisting = self._conditional_create(self._dams_cli.create_external_dataset_model, dset_mdl_obj)
        log.info('ExternalDatasetModel Id: <preexisting={0}> {1}'.format(preexisting, dset_mdl_id))
        #        dset_id = self._dams_cli.create_external_dataset(external_dataset=dset_obj)
        self.object_dict['dset_mdl'] = (dset_mdl_id,dset_mdl_obj)

        dset_id, preexisting = self._conditional_create(self._dams_cli.create_external_dataset, dset_obj, dset_mdl_id)
        log.info('ExternalDataset Id: <preexisting={0}> {1}'.format(preexisting, dset_id))
        self.object_dict['dset'] = (dset_id,dset_obj)
        if not preexisting:
            # Register the ExternalDataset
            dproducer_id = self._dams_cli.register_external_data_set(external_dataset_id=dset_id)
        else:
            dproducer_id = self._rr_cli.find_objects(dset_id, PRED.hasDataProducer, RT.DataProducer, id_only=True)
            for o in dproducer_id[1]:
                res=self._rr_cli.read(o.o)
                if res.is_primary:
                    dproducer_id=res._id
                    break


        dsrc_id, preexisting = self._conditional_create(self._dams_cli.create_data_source, dsrc_obj)
        log.info('DataSource Id: <preexisting={0}> {1}'.format(preexisting, dsrc_id))
        self.object_dict['dsrc'] = (dsrc_id,dsrc_obj)

        dsrc_mdl_id, preexisting = self._conditional_create(self._dams_cli.create_data_source_model, dsrc_mdl_obj)
        log.info('DataSourceModel Id: <preexisting={0}> {1}'.format(preexisting, dsrc_mdl_id))
        self.object_dict['dsrc_mdl'] = (dsrc_mdl_id,dsrc_mdl_obj)

        edp_id, preexisting = self._conditional_create(self._dams_cli.create_external_data_provider, edp_obj)
        log.info('ExternalDataProvider Id: <preexisting={0}> {1}'.format(preexisting, edp_id))
        self.object_dict['edp'] = (edp_id,edp_obj)

        eda_id, preexisting = self._conditional_create(self._dams_cli.create_external_dataset_agent, eda_obj, dset_mdl_id)
        log.info('ExternalDatasetAgent Id: <preexisting={0}> {1}'.format(preexisting, eda_id))
        self.object_dict['eda'] = (eda_id,eda_obj)

#        eda_inst_id = self._conditional_create(self._dams_cli.create_external_dataset_agent_instance, eda_inst_obj, eda_id, dset_id)
#        log.info('ExternalDatasetAgentInstance Id: {0}'.format(eda_inst_id))

        dprod_id, preexisting = self._conditional_create(self._dpms_cli.create_data_product, dprod_obj)
        log.info('DataProduct Id: <preexisting={0}> {1}'.format(preexisting, dprod_id))
        self.object_dict['dprod'] = (dprod_id,dprod_obj)

        # Create associations - will throw 400 errors if they already exist - catch and ignore
        try:
            self._dams_cli.assign_data_source_to_external_data_provider(data_source_id=dsrc_id, external_data_provider_id=edp_id)
        except BadRequest as br:
            if br.message.startswith('Association between'):
                log.info('Association already exists between {0} and {1}'.format(dsrc_id, edp_id))
            else:
                raise br

        try:
            self._dams_cli.assign_data_source_to_data_model(data_source_id=dsrc_id, data_source_model_id=dsrc_mdl_id)
        except BadRequest as br:
            if br.message.startswith('Association between'):
                log.info('Association already exists between {0} and {1}'.format(dsrc_id, dsrc_mdl_id))
            else:
                raise br

        try:
            self._dams_cli.assign_external_dataset_to_data_source(external_dataset_id=dset_id, data_source_id=dsrc_id)
        except BadRequest as br:
            if br.message.startswith('Association between'):
                log.info('Association already exists between {0} and {1}'.format(dset_id, dsrc_id))
            else:
                raise br

        # Generate the data product and associate it to the ExternalDataset
        try:
            stream_id = self._dams_cli.assign_data_product(input_resource_id=dset_id, data_product_id=dprod_id, create_stream=True)
        except BadRequest as br:
            if br.message.startswith('Association between'):
                log.info('Association already exists between {0} and {1}'.format(dset_id, dprod_id))
                stream_id = self._rr_cli.find_objects(dprod_id, PRED.hasStream, RT.Stream, id_only=True)
                stream_id = stream_id[0][0]
            else:
                raise br

        log.info('Stream Id: {0}'.format(stream_id))
        self.object_dict['stream_id'] = stream_id
        self.object_dict['dproducer_id'] = dproducer_id

        # TODO: Results in:
        # IonException: -1 - The stream is associated with more than one stream definition!
#        self._dpms_cli.activate_data_product_persistence(data_product_id=dprod_id, persist_data=True, persist_metadata=True)

        return dset_id, eda_id, dproducer_id, stream_id

    def make_agent_instance(self, name, agent_config, taxonomy):
        agt_cfg = None
        if isinstance(agent_config, str):
            log.error('agent_config a str: {0}'.format(agent_config))
            with open(agent_config, 'r') as f:
                agt_cfg = self._deorder_dict(yaml.load(f))
        else:
            agt_cfg = agent_config

        if agt_cfg is None:
            raise Exception('\'agent_config\' must be either a valid configuration dict, or a path to a valid yaml file containing the configuration')

        log.warn('Agent Config: {0}'.format(agt_cfg))

        tx = None
        if isinstance(taxonomy, str):
            log.error('taxonomy is a str: {0}'.format(taxonomy))
            with open(taxonomy, 'r') as f:
                tx = f.read()
        else:
            tx = taxonomy

        if tx is None:
            raise Exception('\'tx\' must be either a valid taxonomy dict, or a path to a valid yaml file containing the taxonomy')

        log.warn('Taxonomy: {0}'.format(tx))

        if get_safe(agt_cfg, 'driver_config.dh_cfg.stream_id') is None:
            agt_cfg['driver_config']['dh_cfg']['stream_id'] = self.object_dict['stream_id']
        if get_safe(agt_cfg, 'driver_config.dh_cfg.dproducer_id') is None:
            agt_cfg['driver_config']['dh_cfg']['dproducer_id'] = self.object_dict['dproducer_id']
        if get_safe(agt_cfg, 'driver_config.dh_cfg.taxonomy') is None:
            agt_cfg['driver_config']['dh_cfg']['taxonomy'] = tx

        eda_inst_obj = ExternalDatasetAgentInstance(name=name, dataset_driver_config=agt_cfg['driver_config'], dataset_agent_config=agt_cfg)

        eda_id = self.object_dict['eda'][0]
        dset_id = self.object_dict['dset'][0]

        eda_inst_id = self._dams_cli.create_external_dataset_agent_instance(eda_inst_obj, eda_id, dset_id)
#        eda_inst_obj = self._rr_cli.read(eda_inst_id)

        return eda_inst_id, eda_inst_obj

    def _deorder_dict(self, odict):
        from pyon.core.object import walk
        def o2d(obj):
            from collections import OrderedDict
            if isinstance(obj, OrderedDict):
                obj=dict(obj)
            return obj

        return walk(odict, o2d)



class FakeProcess(LocalContextMixin):
    """
    A fake process used because the test case is not an ion process.
    """
    name = ''
    id=''
    process_type = ''

def json_to_yml(file):
    import simplejson as json
    with open(file,'r') as f:
        jobj = json.load(f)
        print jobj

    with open(file.replace('.json','.yml'), 'w') as f:
        f.write(yaml.dump(jobj))


def test_this():
    from pyon.agent.agent import ResourceAgentClient
    from interface.objects import AgentCommand, AgentCommandResult, ExternalDatasetAgentInstance
    from pyon.ion.granule.taxonomy import TaxyTool
#    from examples.registration_utility import DatasetRegistration, FakeProcess

    dreg=DatasetRegistration()
    objs=dreg.parse_dsreg('test_data/dataset_registration/dsreg_file_refs.yml')
    dset_id, eda_id, dproducer_id, stream_id = dreg.register_dataset(*objs)

    # Augment the eda_inst in objs with particulars, in this case Dummy
    tx = TaxyTool()
    tx.add_taxonomy_set('data', 'external_data')

    DVR_CONFIG = {
            'dvr_mod' : 'ion.agents.data.handlers.base_data_handler',
            'dvr_cls' : 'DummyDataHandler',
        }
    DVR_CONFIG['dh_cfg'] = {
        'TESTING':True,
        'stream_id':stream_id,#TODO: This should probably be a 'stream_config' dict with stream_name:stream_id members
        'data_producer_id':dproducer_id,
        'taxonomy':tx.dump(), #TODO: Currently does not support sets
        'max_records':4,
        }

    # Create agent config.
    agent_config = {
        'driver_config' : DVR_CONFIG,
        'stream_config' : {},
        'agent'         : {'resource_id': ''},#resource_id set in 'dams.start_external_dataset_agent_instance'
        'test_mode' : True
    }

    eda_inst_obj = ExternalDatasetAgentInstance(name='dummy_eda_inst', dataset_driver_config=DVR_CONFIG, dataset_agent_config=agent_config)

    eda_inst_id = dreg._dams_cli.create_external_dataset_agent_instance(eda_inst_obj, eda_id, dset_id)


    pid = dreg._dams_cli.start_external_dataset_agent_instance(eda_inst_id)

    ra_cli = ResourceAgentClient(dset_id, FakeProcess())

    return ra_cli, stream_id


    ## WARNING - CAUSES CONTAINER TO FAIL
    #retval=ra_cli.execute_agent('initialize')

"""
from pyon.public import log
from pyon.agent.agent import ResourceAgentClient
from interface.objects import AgentCommand, AgentCommandResult
from examples.registration_utility import test_this
ra_cli, stream_id = test_this()

# Get the current state (should be unintialized)
retval=ra_cli.execute_agent(AgentCommand(command='get_current_state'))
print 'Current state is: {0}'.format(retval.result)

# Get the agent into observatory state
retval=ra_cli.execute_agent(AgentCommand(command='initialize'))
retval=ra_cli.execute_agent(AgentCommand(command='go_active'))
retval=ra_cli.execute_agent(AgentCommand(command='run'))

# Get the current state (should be observatory)
retval=ra_cli.execute_agent(AgentCommand(command='get_current_state'))
print 'Current state is: {0}'.format(retval.result)

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

"""

