#!/usr/bin/env python

"""
@package ion.agents.data.handlers.test.test_slocum_data_handler
@file ion/agents/data/handlers/test/test_slocum_data_handler
@author Christopher Mueller
@brief Test cases for slocum_data_handler
"""

from pyon.public import log
from pyon.util.unit_test import PyonTestCase
from nose.plugins.attrib import attr
from mock import patch, Mock, call, sentinel
import unittest

from ion.agents.data.handlers.slocum_data_handler import SlocumDataHandler, SlocumParser
from interface.objects import ExternalDatasetAgent, ExternalDatasetAgentInstance, ExternalDataProvider, DataProduct, DataSourceModel, ContactInformation, UpdateDescription, DatasetDescription, ExternalDataset, Institution, DataSource

@attr('UNIT', group='eoi')
class TestSlocumDataHandlerUnit(PyonTestCase):

    def setUp(self):
        self.__rr_cli = Mock()
        pass

    def test__init_acquisition_cycle_no_ext_ds_res(self):
        config = {}
        self.assertRaises(SystemError, SlocumDataHandler._init_acquisition_cycle, config)

    def test__init_acquisition_cycle_ext_ds_res(self):
        edres = ExternalDataset(name='test_ed_res', dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        edres.dataset_description.parameters['base_url'] = 'test_data/dir'
        edres.dataset_description.parameters['header_count'] = 12
        edres.dataset_description.parameters['pattern'] = 'test_filter'
        config = {'external_dataset_res':edres}
        SlocumDataHandler._init_acquisition_cycle(config)

        self.assertIn('ds_params', config)
        ds_params = config['ds_params']

        self.assertIn('header_count', ds_params)
        self.assertEquals(ds_params['header_count'],12)
        self.assertIn('base_url',ds_params)
        self.assertEquals(ds_params['base_url'],'test_data/dir')
        self.assertIn('pattern',ds_params)
        self.assertEquals(ds_params['pattern'], 'test_filter')

    def test__new_data_constraints(self):
#        ret = SlocumDataHandler._new_data_constraints({})
#        self.assertIsInstance(ret, dict)

        old_list = [
            ('test_data/slocum/ru05-2012-021-0-0-sbd.dat', 1337261358.0, 521081),
            ('test_data/slocum/ru05-2012-022-0-0-sbd.dat', 1337261358.0, 521081),
        ]

#        old_list = None

        edres = ExternalDataset(name='test_ed_res', dataset_description=DatasetDescription(), update_description=UpdateDescription(), contact=ContactInformation())
        config = {
            'external_dataset_res':edres,
            'new_data_check':old_list,
            'ds_params':{
                # These would be extracted from the dataset_description.parameters during _init_acquisition_cycle, but since that isn't called, just add them here
                'base_url':'test_data/slocum/',
                'pattern':'ru05-*-sbd.dat',# Appended to base to filter files; Either a shell style pattern (for filesystem) or regex (for http/ftp)
            }
        }
        ret = SlocumDataHandler._new_data_constraints(config)
        log.warn(ret)

    def test__get_data(self):
        config = {
            'constraints':{
                'new_files':[
                    ('test_data/slocum/ru05-2012-021-0-0-sbd.dat', 1337261358.0, 521081),
                ]
            }
        }

        for x in SlocumDataHandler._get_data(config):
            log.debug(x)

