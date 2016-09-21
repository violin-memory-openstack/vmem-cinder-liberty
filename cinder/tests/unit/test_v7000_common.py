# Copyright 2015 Violin Memory, Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Tests for Violin Memory 7000 Series All-Flash Array Common Driver
"""
import math
import mock

from oslo_utils import units

from cinder import context
from cinder import exception
from cinder import test
from cinder.tests.unit import fake_vmem_client as vmemclient
from cinder.volume import configuration as conf
from cinder.volume.drivers.violin import v7000_common
from cinder.volume import volume_types


VOLUME_ID = "abcdabcd-1234-abcd-1234-abcdeffedcba"
VOLUME = {"name": "volume-" + VOLUME_ID,
          "id": VOLUME_ID,
          "display_name": "fake_volume",
          "size": 2,
          "host": "irrelevant",
          "volume_type": None,
          "volume_type_id": None,
          }
SNAPSHOT_ID = "abcdabcd-1234-abcd-1234-abcdeffedcbb"
SNAPSHOT = {"name": "snapshot-" + SNAPSHOT_ID,
            "id": SNAPSHOT_ID,
            "volume_id": VOLUME_ID,
            "volume_name": "volume-" + VOLUME_ID,
            "volume_size": 2,
            "display_name": "fake_snapshot",
            }
SRC_VOL_ID = "abcdabcd-1234-abcd-1234-abcdeffedcbc"
SRC_VOL = {"name": "volume-" + SRC_VOL_ID,
           "id": SRC_VOL_ID,
           "display_name": "fake_src_vol",
           "size": 2,
           "host": "irrelevant",
           "volume_type": None,
           "volume_type_id": None,
           }
INITIATOR_IQN = "iqn.1111-22.org.debian:11:222"
CONNECTOR = {"initiator": INITIATOR_IQN}

DEFAULT_THICK_POOL = {"storage_pool": 'PoolA',
                      "storage_pool_id": 1,
                      "dedup": False,
                      "thin": False,
                      }
DEFAULT_THIN_POOL = {"storage_pool": 'PoolB',
                     "storage_pool_id": 2,
                     "dedup": False,
                     "thin": True,
                     }
DEFAULT_DEDUP_POOL = {"storage_pool": 'PoolC',
                      "storage_pool_id": 3,
                      "dedup": True,
                      "thin": True,
                      }

# Note:  select superfluous fields are removed for brevity
STATS_STORAGE_POOL_RESPONSE = [({
    'availsize_mb': 1572827,
    'category': 'Virtual Device',
    'name': 'dedup-pool',
    'object_id': '487d1940-c53f-55c3-b1d5-073af43f80fc',
    'size_mb': 2097124,
    'storage_pool_id': 1,
    'usedsize_mb': 524297},
    {'category': 'Virtual Device',
     'name': 'dedup-pool',
     'object_id': '487d1940-c53f-55c3-b1d5-073af43f80fc',
     'physicaldevices':
     [{'availsize_mb': 524281,
       'connection_type': 'fc',
       'name': 'VIOLIN:CONCERTO ARRAY.003',
       'object_id': '260f30b0-0300-59b5-b7b9-54aa55704a12',
       'owner': 'lab-host1',
       'size_mb': 524281,
       'type': 'Direct-Access',
       'usedsize_mb': 0},
      {'availsize_mb': 524281,
       'connection_type': 'fc',
       'name': 'VIOLIN:CONCERTO ARRAY.004',
       'object_id': '7b58eda2-69da-5aec-9e06-6607934efa93',
       'owner': 'lab-host1',
       'size_mb': 524281,
       'type': 'Direct-Access',
       'usedsize_mb': 0},
      {'availsize_mb': 0,
       'connection_type': 'fc',
       'name': 'VIOLIN:CONCERTO ARRAY.001',
       'object_id': '69adbea1-2349-5df5-a04a-abd7f14868b2',
       'owner': 'lab-host1',
       'size_mb': 524281,
       'type': 'Direct-Access',
       'usedsize_mb': 524281},
      {'availsize_mb': 524265,
       'connection_type': 'fc',
       'name': 'VIOLIN:CONCERTO ARRAY.002',
       'object_id': 'a14a0e36-8901-5987-95d8-aa574c6138a2',
       'owner': 'lab-host1',
       'size_mb': 524281,
       'type': 'Direct-Access',
       'usedsize_mb': 16}],
     'size_mb': 2097124,
     'storage_pool_id': 1,
     'total_physicaldevices': 4,
     'usedsize_mb': 524297}),
    ({'availsize': 0,
      'availsize_mb': 0,
      'category': None,
      'name': 'thick_pool_13531mgb',
      'object_id': '20610abd-4c58-546c-8905-bf42fab9a11b',
      'size': 0,
      'size_mb': 0,
      'storage_pool_id': 3,
      'tag': '',
      'total_physicaldevices': 0,
      'usedsize': 0,
      'usedsize_mb': 0},
     {'category': None,
      'name': 'thick_pool_13531mgb',
      'object_id': '20610abd-4c58-546c-8905-bf42fab9a11b',
      'resource_type': ['All'],
      'size': 0,
      'size_mb': 0,
      'storage_pool_id': 3,
      'tag': [''],
      'total_physicaldevices': 0,
      'usedsize': 0,
      'usedsize_mb': 0}),
    ({'availsize_mb': 627466,
      'category': 'Virtual Device',
      'name': 'StoragePool',
      'object_id': '1af66d9a-f62e-5b69-807b-892b087fa0b4',
      'size_mb': 21139267,
      'storage_pool_id': 7,
      'usedsize_mb': 20511801},
     {'category': 'Virtual Device',
      'name': 'StoragePool',
      'object_id': '1af66d9a-f62e-5b69-807b-892b087fa0b4',
      'physicaldevices':
      [{'availsize_mb': 0,
        'connection_type': 'block',
        'name': 'BKSC:OTHDISK-MFCN02.000',
        'object_id': 'ecc775f1-1228-5131-8f68-4176001786ef',
        'owner': 'lab-host1',
        'size_mb': 1048569,
        'type': 'Direct-Access',
        'usedsize_mb': 1048569},
       {'availsize_mb': 0,
        'connection_type': 'block',
        'name': 'BKSC:OTHDISK-MFCN01.000',
        'object_id': '5c60812b-34d2-5473-b7bf-21e30ec70311',
        'owner': 'lab-host1',
        'size_mb': 1048569,
        'type': 'Direct-Access',
        'usedsize_mb': 1048569},
       {'availsize_mb': 0,
        'connection_type': 'block',
        'name': 'BKSC:OTHDISK-MFCN08.001',
        'object_id': 'eb6d06b7-8d6f-5d9d-b720-e86d8ad1beab',
        'owner': 'lab-host1',
        'size_mb': 1048569,
        'type': 'Direct-Access',
        'usedsize_mb': 1048569},
       {'availsize_mb': 0,
        'connection_type': 'block',
        'name': 'BKSC:OTHDISK-MFCN03.001',
        'object_id': '063aced7-1f8f-5e15-b36e-e9d34a2826fa',
        'owner': 'lab-host1',
        'size_mb': 1048569,
        'type': 'Direct-Access',
        'usedsize_mb': 1048569},
       {'availsize_mb': 0,
        'connection_type': 'block',
        'name': 'BKSC:OTHDISK-MFCN07.001',
        'object_id': 'ebf34594-2b92-51fe-a6a8-b6cf91f05b2b',
        'owner': 'lab-host1',
        'size_mb': 1048569,
        'type': 'Direct-Access',
        'usedsize_mb': 1048569},
       {'availsize_mb': 0,
        'connection_type': 'block',
        'name': 'BKSC:OTHDISK-MFCN0A.000',
        'object_id': 'ff084188-b97f-5e30-9ff0-bc60e546ee06',
        'owner': 'lab-host1',
        'size_mb': 1048569,
        'type': 'Direct-Access',
        'usedsize_mb': 1048569},
       {'availsize_mb': 0,
        'connection_type': 'block',
        'name': 'BKSC:OTHDISK-MFCN06.001',
        'object_id': 'f9cbeadf-5524-5697-a3a6-667820e37639',
        'owner': 'lab-host1',
        'size_mb': 1048569,
        'type': 'Direct-Access',
        'usedsize_mb': 1048569},
       {'availsize_mb': 167887,
        'connection_type': 'block',
        'name': 'BKSC:OTHDISK-MFCN15.000',
        'object_id': 'aaacc124-26c9-519a-909a-a93d24f579a1',
        'owner': 'lab-host2',
        'size_mb': 167887,
        'type': 'Direct-Access',
        'usedsize_mb': 0},
       {'availsize_mb': 229276,
        'connection_type': 'block',
        'name': 'BKSC:OTHDISK-MFCN09.001',
        'object_id': '30967a84-56a4-52a5-ac3f-b4f544257bbd',
        'owner': 'lab-host1',
        'size_mb': 1048569,
        'type': 'Direct-Access',
        'usedsize_mb': 819293},
       {'availsize_mb': 0,
        'connection_type': 'block',
        'name': 'BKSC:OTHDISK-MFCN04.001',
        'object_id': 'd997eb42-55d4-5e4c-b797-c68b748e7e1f',
        'owner': 'lab-host1',
        'size_mb': 1048569,
        'type': 'Direct-Access',
        'usedsize_mb': 1048569},
       {'availsize_mb': 0,
        'connection_type': 'block',
        'name': 'BKSC:OTHDISK-MFCN05.001',
        'object_id': '56ecf98c-f10b-5bb5-9d3b-5af6037dad73',
        'owner': 'lab-host1',
        'size_mb': 1048569,
        'type': 'Direct-Access',
        'usedsize_mb': 1048569},
       {'availsize_mb': 0,
        'connection_type': 'block',
        'name': 'BKSC:OTHDISK-MFCN0B.000',
        'object_id': 'cfb6f61c-508d-5394-8257-78b1f9bcad3b',
        'owner': 'lab-host2',
        'size_mb': 1048569,
        'type': 'Direct-Access',
        'usedsize_mb': 1048569},
       {'availsize_mb': 0,
        'connection_type': 'block',
        'name': 'BKSC:OTHDISK-MFCN0C.000',
        'object_id': '7b0bcb51-5c7d-5752-9e18-392057e534f0',
        'owner': 'lab-host2',
        'size_mb': 1048569,
        'type': 'Direct-Access',
        'usedsize_mb': 1048569},
       {'availsize_mb': 0,
        'connection_type': 'block',
        'name': 'BKSC:OTHDISK-MFCN0D.000',
        'object_id': 'b785a3b1-6316-50c3-b2e0-6bb0739499c6',
        'owner': 'lab-host2',
        'size_mb': 1048569,
        'type': 'Direct-Access',
        'usedsize_mb': 1048569},
       {'availsize_mb': 0,
        'connection_type': 'block',
        'name': 'BKSC:OTHDISK-MFCN0E.000',
        'object_id': '76b9d038-b757-515a-b962-439a4fd85fd5',
        'owner': 'lab-host2',
        'size_mb': 1048569,
        'type': 'Direct-Access',
        'usedsize_mb': 1048569},
       {'availsize_mb': 0,
        'connection_type': 'block',
        'name': 'BKSC:OTHDISK-MFCN0F.000',
        'object_id': '9591d24a-70c4-5e80-aead-4b788202c698',
        'owner': 'lab-host2',
        'size_mb': 1048569,
        'type': 'Direct-Access',
        'usedsize_mb': 1048569},
       {'availsize_mb': 0,
        'connection_type': 'block',
        'name': 'BKSC:OTHDISK-MFCN10.000',
        'object_id': '2bb09a2b-9063-595b-9d7a-7e5fad5016db',
        'owner': 'lab-host2',
        'size_mb': 1048569,
        'type': 'Direct-Access',
        'usedsize_mb': 1048569},
       {'availsize_mb': 0,
        'connection_type': 'block',
        'name': 'BKSC:OTHDISK-MFCN11.000',
        'object_id': 'b9ff58eb-5e6e-5c79-bf95-fae424492519',
        'owner': 'lab-host2',
        'size_mb': 1048569,
        'type': 'Direct-Access',
        'usedsize_mb': 1048569},
       {'availsize_mb': 0,
        'connection_type': 'block',
        'name': 'BKSC:OTHDISK-MFCN12.000',
        'object_id': '6abd4fd6-9841-5978-bfcb-5d398d1715b4',
        'owner': 'lab-host2',
        'size_mb': 1048569,
        'type': 'Direct-Access',
        'usedsize_mb': 1048569},
       {'availsize_mb': 230303,
        'connection_type': 'block',
        'name': 'BKSC:OTHDISK-MFCN13.000',
        'object_id': 'ffd5a4b7-0f50-5a71-bbba-57a348b96c68',
        'owner': 'lab-host2',
        'size_mb': 1048569,
        'type': 'Direct-Access',
        'usedsize_mb': 818266},
       {'availsize_mb': 0,
        'connection_type': 'block',
        'name': 'BKSC:OTHDISK-MFCN14.000',
        'object_id': '52ffbbae-bdac-5194-ba6b-62ee17bfafce',
        'owner': 'lab-host2',
        'size_mb': 1048569,
        'type': 'Direct-Access',
        'usedsize_mb': 1048569}],
      'size_mb': 21139267,
      'storage_pool_id': 7,
      'tag': [''],
      'total_physicaldevices': 21,
      'usedsize_mb': 20511801}),
    ({'availsize_mb': 1048536,
      'category': 'Virtual Device',
      'name': 'thick-pool',
      'object_id': 'c1e0becc-3497-5d74-977a-1e5a79769576',
      'size_mb': 2097124,
      'storage_pool_id': 9,
      'usedsize_mb': 1048588},
     {'category': 'Virtual Device',
      'name': 'thick-pool',
      'object_id': 'c1e0becc-3497-5d74-977a-1e5a79769576',
      'physicaldevices':
      [{'availsize_mb': 524255,
        'connection_type': 'fc',
        'name': 'VIOLIN:CONCERTO ARRAY.001',
        'object_id': 'a90c4a11-33af-5530-80ca-2360fa477781',
        'owner': 'lab-host1',
        'size_mb': 524281,
        'type': 'Direct-Access',
        'usedsize_mb': 26},
       {'availsize_mb': 0,
        'connection_type': 'fc',
        'name': 'VIOLIN:CONCERTO ARRAY.002',
        'object_id': '0a625ec8-2e80-5086-9644-2ea8dd5c32ec',
        'owner': 'lab-host1',
        'size_mb': 524281,
        'type': 'Direct-Access',
        'usedsize_mb': 524281},
       {'availsize_mb': 0,
        'connection_type': 'fc',
        'name': 'VIOLIN:CONCERTO ARRAY.004',
        'object_id': '7018670b-3a79-5bdc-9d02-2d85602f361a',
        'owner': 'lab-host1',
        'size_mb': 524281,
        'type': 'Direct-Access',
        'usedsize_mb': 524281},
       {'availsize_mb': 524281,
        'connection_type': 'fc',
        'name': 'VIOLIN:CONCERTO ARRAY.003',
        'object_id': 'd859d47b-ca65-5d9d-a1c0-e288bbf39f48',
        'owner': 'lab-host1',
        'size_mb': 524281,
        'type': 'Direct-Access',
        'usedsize_mb': 0}],
      'size_mb': 2097124,
      'storage_pool_id': 9,
      'total_physicaldevices': 4,
      'usedsize_mb': 1048588})]

PHY_DEVICES_RESPONSE = {
    'data':
    {'physical_devices':
        [{'availsize': 1099504287744,
          'availsize_mb': 524284,
          'category': 'Virtual Device',
          'connection_type': 'block',
          'firmware': 'v1.0',
          'guid': '3cc4d6dd-166d-77d2-4967-00005463f597',
          'inquiry_string': '000002122b000032BKSC    OTHDISK-MFCN01  v1.0',
          'is_foreign': True,
          'name': 'BKSC:OTHDISK-MFCN01.000',
          'object_id': '84b834fb-1f4d-5d3b-b7ae-5796f9868151',
          'owner': 'example.com',
          'pool': None,
          'product': 'OTHDISK-MFCN01',
          'scsi_address':
          {'adapter': '98',
           'channel': '0',
           'id': '0',
           'lun': '0',
           'object_id': '6e0106fc-9c1c-52a2-95c9-396b7a653ac1'},
          'size': 1099504287744,
          'size_mb': 1048569,
          'type': 'Direct-Access',
          'usedsize': 0,
          'usedsize_mb': 0,
          'vendor': 'BKSC',
          'wwid': 'BKSC    OTHDISK-MFCN01  v1.0-0-0-00'},
         {'availsize': 1099504287744,
          'availsize_mb': 524284,
          'category': 'Virtual Device',
          'connection_type': 'block',
          'firmware': 'v1.0',
          'guid': '283b2694-192b-4745-6768-00005463f673',
          'inquiry_string': '000002122b000032BKSC    OTHDISK-MFCN08  v1.0',
          'is_foreign': False,
          'name': 'BKSC:OTHDISK-MFCN08.000',
          'object_id': '8555b888-bf43-5083-a433-f0c7b0282370',
          'owner': 'example.com',
          'pool':
          {'name': 'mga-pool',
           'object_id': '0818d3de-4437-535f-9cac-cc100a2c9313'},
          'product': 'OTHDISK-MFCN08',
          'scsi_address':
          {'adapter': '98',
           'channel': '0',
           'id': '11',
           'lun': '0',
           'object_id': '6e0106fc-9c1c-52a2-95c9-396b7a653ac1'},
          'size': 1099504287744,
          'size_mb': 1048569,
          'type': 'Direct-Access',
          'usedsize': 0,
          'usedsize_mb': 0,
          'vendor': 'BKSC',
          'wwid': 'BKSC    OTHDISK-MFCN08  v1.0-0-0-00'},
         {'availsize': 1099504287744,
          'availsize_mb': 1048569,
          'category': 'Virtual Device',
          'connection_type': 'block',
          'firmware': 'v1.0',
          'guid': '7f47db19-019c-707d-0df1-00005463f949',
          'inquiry_string': '000002122b000032BKSC    OTHDISK-MFCN09  v1.0',
          'is_foreign': False,
          'name': 'BKSC:OTHDISK-MFCN09.000',
          'object_id': '62a98898-f8b8-5837-af2b-764f5a72e291',
          'owner': 'a.b.c.d',
          'pool':
          {'name': 'mga-pool',
           'object_id': '0818d3de-4437-535f-9cac-cc100a2c9313'},
          'product': 'OTHDISK-MFCN09',
          'scsi_address':
          {'adapter': '98',
           'channel': '0',
           'id': '12',
           'lun': '0',
           'object_id': '6e0106fc-9c1c-52a2-95c9-396b7a653ac1'},
          'size': 1099504287744,
          'size_mb': 524284,
          'type': 'Direct-Access',
          'usedsize': 0,
          'usedsize_mb': 0,
          'vendor': 'BKSC',
          'wwid': 'BKSC    OTHDISK-MFCN09  v1.0-0-0-00'}],
        'total_physical_devices': 3},
    'msg': 'Successful',
    'success': True
}

GROUP_ID = "abcdabcd-1234-abcd-4321-abcdeffedcba"
GROUP = {'id': GROUP_ID, 'name': 'group_name'}
GROUP_VOLUME = {"name": "volume-" + VOLUME_ID,
                "id": VOLUME_ID,
                "display_name": "fake_volume2",
                "size": 2,
                "host": "irrelevant",
                "volume_type": None,
                "volume_type_id": None,
                "consistencygroup_id": GROUP_ID,
                }

SRC_GROUP_ID = 'bdbdacac-1234-bdac-4321-abcdeffedcba'
SRC_GROUP = {'id': SRC_GROUP_ID, 'name': 'src-group'}

CGSNAPSHOT_ID = "aabbccdd-1221-dcba-4334-abcdeffedcba"
CGSNAPSHOT = {"name": "snap-" + CGSNAPSHOT_ID,
              "id": CGSNAPSHOT_ID,
              "consistencygroup_id": GROUP_ID,
              }

UUID4 = "bbb17dac-60b4-4a8f-a208-ace7b5f37517"
UUID4_COMPRESSED = UUID4.replace('-', '')


class V7000CommonTestCase(test.TestCase):
    """Test case for Violin drivers."""
    def setUp(self):
        super(V7000CommonTestCase, self).setUp()
        self.conf = self.setup_configuration()
        self.driver = v7000_common.V7000Common(self.conf)
        self.driver.container = 'myContainer'
        self.driver.device_id = 'ata-VIOLIN_MEMORY_ARRAY_23109R00000022'
        self.stats = {}

    def tearDown(self):
        super(V7000CommonTestCase, self).tearDown()

    def setup_configuration(self):
        config = mock.Mock(spec=conf.Configuration)
        config.volume_backend_name = 'v7000_common'
        config.san_ip = '1.1.1.1'
        config.san_login = 'admin'
        config.san_password = ''
        config.san_thin_provision = False
        config.san_is_local = False
        config.gateway_mga = '2.2.2.2'
        config.gateway_mgb = '3.3.3.3'
        config.use_igroups = False
        config.container = 'myContainer'
        config.violin_request_timeout = 300
        config.violin_dedup_only_pools = []
        config.violin_dedup_capable_pools = []
        config.violin_pool_allocation_method = 'random'
        return config

    def setup_mock_concerto(self, m_conf=None):
        """Create a fake Concerto communication object."""
        _m_concerto = mock.Mock(name='Concerto',
                                version='1.1.1',
                                spec=vmemclient.mock_client_conf)

        if m_conf:
            _m_concerto.configure_mock(**m_conf)

        return _m_concerto

    def setup_mock_db(self, m_conf=None):
        """Create a fake db object."""
        m = mock.Mock()

        if m_conf:
            m.configure_mock(**m_conf)

        return m

    def test_check_for_setup_error(self):
        """No setup errors are found."""
        self.driver.vmem_mg = self.setup_mock_concerto()
        self.driver._is_supported_vmos_version = mock.Mock(return_value=True)

        result = self.driver.check_for_setup_error()

        self.driver._is_supported_vmos_version.assert_called_with(
            self.driver.vmem_mg.version)
        self.assertIsNone(result)

    def test_create_lun(self):
        """Thick lun is successfully created."""
        response = {'success': True, 'msg': 'Create resource successfully.'}
        size_in_mb = VOLUME['size'] * units.Ki
        spec_dict = {'pool_type': 'thick', 'size_mb': size_in_mb,
                     'lun_encryption': False}

        conf = {
            'lun.create_lun.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._send_cmd = mock.Mock(return_value=response)
        self.driver._process_extra_specs = mock.Mock(
            return_value=spec_dict)
        self.driver._get_storage_pool = mock.Mock(
            return_value=DEFAULT_THICK_POOL)

        result = self.driver._create_lun(VOLUME)

        self.driver._send_cmd.assert_called_with(
            self.driver.vmem_mg.lun.create_lun,
            'Create resource successfully.',
            VOLUME['id'], size_in_mb, False, False, False, size_in_mb,
            storage_pool_id=DEFAULT_THICK_POOL['storage_pool_id'])
        self.assertIsNone(result)

    def test_create_thin_lun(self):
        """Thin lun is successfully created."""
        response = {'success': True, 'msg': 'Create resource successfully.'}
        size_in_mb = VOLUME['size'] * units.Ki
        alloc_size = size_in_mb // 10
        spec_dict = {'pool_type': 'thin', 'size_mb': alloc_size,
                     'lun_encryption': False}

        conf = {
            'lun.create_lun.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._send_cmd = mock.Mock(return_value=response)
        self.driver._process_extra_specs = mock.Mock(
            return_value=spec_dict)
        self.driver._get_storage_pool = mock.Mock(
            return_value=DEFAULT_THIN_POOL)

        result = self.driver._create_lun(VOLUME)

        self.driver._send_cmd.assert_called_with(
            self.driver.vmem_mg.lun.create_lun,
            'Create resource successfully.',
            VOLUME['id'], alloc_size, False, False, True, size_in_mb,
            storage_pool_id=DEFAULT_THIN_POOL['storage_pool_id'])
        self.assertIsNone(result)

    def test_create_encrypted_lun(self):
        """Thin lun is successfully created."""
        response = {'success': True, 'msg': 'Create resource successfully.'}
        size_in_mb = VOLUME['size'] * units.Ki
        alloc_size = size_in_mb // 10
        spec_dict = {'pool_type': 'thin', 'size_mb': alloc_size,
                     'lun_encryption': True}

        conf = {
            'lun.create_lun.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._send_cmd = mock.Mock(return_value=response)
        self.driver._process_extra_specs = mock.Mock(
            return_value=spec_dict)
        self.driver._get_storage_pool = mock.Mock(
            return_value=DEFAULT_THIN_POOL)

        result = self.driver._create_lun(VOLUME)

        self.driver._send_cmd.assert_called_with(
            self.driver.vmem_mg.lun.create_lun,
            'Create resource successfully.',
            VOLUME['id'], alloc_size, False, True, True, size_in_mb,
            storage_pool_id=DEFAULT_THIN_POOL['storage_pool_id'])
        self.assertIsNone(result)

    def test_create_dedup_lun(self):
        """Dedup lun is successfully created."""
        response = {'success': True, 'msg': 'Create resource successfully.'}
        size_in_mb = VOLUME['size'] * units.Ki
        alloc_size = size_in_mb // 10
        spec_dict = {'pool_type': 'dedup', 'size_mb': alloc_size,
                     'lun_encryption': False}

        conf = {
            'lun.create_lun.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._send_cmd = mock.Mock(return_value=response)
        self.driver._process_extra_specs = mock.Mock(
            return_value=spec_dict)
        self.driver._get_storage_pool = mock.Mock(
            return_value=DEFAULT_DEDUP_POOL)

        result = self.driver._create_lun(VOLUME)

        self.driver._send_cmd.assert_called_with(
            self.driver.vmem_mg.lun.create_lun,
            'Create resource successfully.',
            VOLUME['id'], alloc_size, True, False, True, size_in_mb,
            storage_pool_id=DEFAULT_DEDUP_POOL['storage_pool_id'])
        self.assertIsNone(result)

    def test_create_consistencygroup_lun(self):
        """Thick lun is successfully created / placed in the consisgroup."""
        vol = GROUP_VOLUME.copy()
        response = {'success': True, 'msg': 'Create resource successfully.'}
        size_in_mb = vol['size'] * units.Ki
        spec_dict = {'pool_type': 'thick', 'size_mb': size_in_mb,
                     'lun_encryption': False}

        conf = {
            'lun.create_lun.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._send_cmd = mock.Mock(return_value=response)
        self.driver._process_extra_specs = mock.Mock(
            return_value=spec_dict)
        self.driver._get_storage_pool = mock.Mock(
            return_value=DEFAULT_THICK_POOL)
        self.driver._ensure_snapshot_resource_area = mock.Mock(
            return_value=None)
        self.driver._add_to_consistencygroup = mock.Mock(
            return_value=None)

        result = self.driver._create_lun(vol)

        self.driver._send_cmd.assert_called_with(
            self.driver.vmem_mg.lun.create_lun,
            'Create resource successfully.',
            vol['id'], size_in_mb, False, False, False, size_in_mb,
            storage_pool_id=DEFAULT_THICK_POOL['storage_pool_id'])
        self.driver._ensure_snapshot_resource_area.assert_called_once_with(
            vol['id'])
        self.driver._add_to_consistencygroup.assert_called_once_with(
            vol['consistencygroup_id'], vol['id'])
        self.assertIsNone(result)

    def test_create_lun_already_exists(self):
        """Array returns error that the lun already exists."""
        response = {'success': False,
                    'msg': 'Duplicate Virtual Device name. Error: 0x90010022'}
        size_in_mb = VOLUME['size'] * units.Ki
        spec_dict = {'pool_type': 'thick', 'size_mb': size_in_mb}

        conf = {
            'lun.create_lun.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._get_storage_pool = mock.Mock(
            return_value=DEFAULT_THICK_POOL)
        self.driver._send_cmd = mock.Mock(
            side_effect=exception.ViolinBackendErrExists)

        result = self.driver._create_lun(VOLUME)

        self.assertIsNone(result)

    def test_delete_lun(self):
        """Lun is deleted successfully."""
        response = {'success': True, 'msg': 'Delete resource successfully'}
        success_msgs = ['Delete resource successfully', '']

        conf = {
            'lun.delete_lun.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._send_cmd = mock.Mock(return_value=response)
        self.driver._delete_lun_snapshot_bookkeeping = mock.Mock()

        result = self.driver._delete_lun(VOLUME)

        self.driver._send_cmd.assert_called_with(
            self.driver.vmem_mg.lun.delete_lun,
            success_msgs, VOLUME['id'], True)
        self.driver._delete_lun_snapshot_bookkeeping.assert_called_with(
            VOLUME['id'])

        self.assertIsNone(result)

    def test_fail_extend_dedup_lun(self):
        """Volume extend fails when new size would shrink the volume."""
        vol = VOLUME.copy()
        vol['volume_type_id'] = '1'
        size_in_mb = vol['size'] * units.Ki

        # simulate extra specs of {'thin': 'true', 'dedupe': 'true'}
        self.driver._get_volume_type_extra_spec = mock.Mock(
            return_value="True")

        self.driver.vmem_mg = self.setup_mock_concerto()
        type(self.driver.vmem_mg.utility).is_external_head = mock.PropertyMock(
            return_value=False)

        self.assertRaises(exception.VolumeDriverException,
                          self.driver._extend_lun, vol, size_in_mb)

    def test_extend_lun(self):
        """Volume extend completes successfully."""
        new_volume_size = 10
        change_in_size_mb = (new_volume_size - VOLUME['size']) * units.Ki

        response = {'success': True, 'message': 'Expand resource successfully'}

        conf = {
            'lun.extend_lun.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._send_cmd = mock.Mock(return_value=response)

        result = self.driver._extend_lun(VOLUME, new_volume_size)

        self.driver._send_cmd.assert_called_with(
            self.driver.vmem_mg.lun.extend_lun,
            response['message'], VOLUME['id'], change_in_size_mb)
        self.assertIsNone(result)

    def test_extend_lun_new_size_is_too_small(self):
        """Volume extend fails when new size would shrink the volume."""
        new_volume_size = 0
        change_in_size_mb = (new_volume_size - VOLUME['size']) * units.Ki

        response = {'success': False, 'msg': 'Invalid size. Error: 0x0902000c'}
        failure = exception.ViolinBackendErr

        conf = {
            'lun.resize_lun.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._send_cmd = mock.Mock(side_effect=failure(message='fail'))

        self.assertRaises(failure, self.driver._extend_lun,
                          VOLUME, change_in_size_mb)

    def test_create_lun_snapshot(self):
        response = {'success': True, 'msg': 'Create TimeMark successfully'}

        self.driver.vmem_mg = self.setup_mock_concerto()
        self.driver._ensure_snapshot_resource_area = mock.Mock(
            return_value=True)
        self.driver._ensure_snapshot_policy = mock.Mock(return_value=True)
        self.driver._send_cmd = mock.Mock(return_value=response)

        with mock.patch('cinder.db.sqlalchemy.api.volume_get',
                        return_value=VOLUME):
            result = self.driver._create_lun_snapshot(SNAPSHOT)

        self.assertIsNone(result)

        self.driver._ensure_snapshot_resource_area.assert_called_with(
            VOLUME_ID)
        self.driver._ensure_snapshot_policy.assert_called_with(VOLUME_ID)
        self.driver._send_cmd.assert_called_once_with(
            self.driver.vmem_mg.snapshot.create_lun_snapshot,
            'Create TimeMark successfully',
            lun=VOLUME_ID,
            comment=self.driver._compress_snapshot_id(SNAPSHOT_ID),
            priority=v7000_common.CONCERTO_DEFAULT_PRIORITY,
            enable_notification=False)

    def test_delete_lun_snapshot(self):
        self.driver._wait_run_delete_lun_snapshot = mock.Mock(
            return_value=None)
        result = self.driver._delete_lun_snapshot(SNAPSHOT)

        self.assertIsNone(result)
        self.driver._wait_run_delete_lun_snapshot.assert_called_once_with(
            SNAPSHOT)

    def test_create_volume_from_snapshot(self):
        """Create a new cinder volume from a given snapshot of a lun."""
        object_id = '12345'
        vdev_id = 11111
        response = {'success': True,
                    'object_id': object_id,
                    'msg': 'Copy TimeMark successfully.'}
        lun_info = {'virtualDeviceID': vdev_id, 'subType': 'THICK'}
        compressed_snap_id = 'abcdabcd1234abcd1234abcdeffedcbb'
        spec_dict = {'pool_type': 'thick'}

        conf = {
            'lun.get_lun_info.return_value': lun_info,
            'lun.copy_snapshot_to_new_lun.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._process_extra_specs = mock.Mock(
            return_value=spec_dict)
        self.driver._get_storage_pool = mock.Mock(
            return_value=DEFAULT_THICK_POOL)
        self.driver._compress_snapshot_id = mock.Mock(
            return_value=compressed_snap_id)
        self.driver._wait_for_lun_or_snap_copy = mock.Mock()

        result = self.driver._create_volume_from_snapshot(SNAPSHOT, VOLUME)

        v = self.driver
        v.vmem_mg.lun.copy_snapshot_to_new_lun.assert_called_once_with(
            source_lun=SNAPSHOT['volume_id'],
            source_snapshot_comment=compressed_snap_id,
            destination=VOLUME['id'],
            storage_pool_id=DEFAULT_THICK_POOL['storage_pool_id'])
        v._wait_for_lun_or_snap_copy.assert_called_once_with(
            SNAPSHOT['volume_id'], dest_vdev_id=vdev_id)

        self.assertIsNone(result)

    def test_create_volume_from_snapshot_fails_already_exists(self):
        """Array returns error that the lun already exists."""
        response = {'success': False,
                    'msg': 'Duplicate Virtual Device name. Error: 0x90010022'}
        lun_info = {'subType': 'THICK'}
        compressed_snap_id = 'abcdabcd1234abcd1234abcdeffedcbb'
        spec_dict = {'pool_type': 'thick'}
        failure = exception.ViolinBackendErrExists

        conf = {
            'lun.get_lun_info.return_value': lun_info,
            'lun.copy_snapshot_to_new_lun.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._process_extra_specs = mock.Mock(
            return_value=spec_dict)
        self.driver._get_storage_pool = mock.Mock(
            return_value=DEFAULT_THICK_POOL)
        self.driver._compress_snapshot_id = mock.Mock(
            return_value=compressed_snap_id)

        self.driver._send_cmd = mock.Mock(side_effect=failure(message='fail'))

        self.assertRaises(failure, self.driver._create_volume_from_snapshot,
                          SNAPSHOT, VOLUME)

    def test_create_volume_from_snapshot_fails_for_non_thick_luns(self):
        """Cloning a volume from a non-thick LUN is not supported."""
        lun_info = {'subType': 'THIN'}

        conf = {
            'lun.get_lun_info.return_value': lun_info,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        self.assertRaises(exception.ViolinBackendErr,
                          self.driver._create_volume_from_snapshot,
                          SNAPSHOT, VOLUME)

    def test_create_consistencygroup_volume_from_snapshot(self):
        """Create a new cinder volume from a given snapshot of a lun."""
        object_id = '12345'
        vdev_id = 11111
        response = {'success': True,
                    'object_id': object_id,
                    'msg': 'Copy TimeMark successfully.'}
        lun_info = {'virtualDeviceID': vdev_id, 'subType': 'THICK'}
        compressed_snap_id = 'abcdabcd1234abcd1234abcdeffedcbb'
        spec_dict = {'pool_type': 'thick'}

        conf = {
            'lun.get_lun_info.return_value': lun_info,
            'lun.copy_snapshot_to_new_lun.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._process_extra_specs = mock.Mock(
            return_value=spec_dict)
        self.driver._get_storage_pool = mock.Mock(
            return_value=DEFAULT_THICK_POOL)
        self.driver._compress_snapshot_id = mock.Mock(
            return_value=compressed_snap_id)
        self.driver._wait_for_lun_or_snap_copy = mock.Mock()
        self.driver._ensure_snapshot_resource_area = mock.Mock()
        self.driver._add_to_consistencygroup = mock.Mock()

        result = self.driver._create_volume_from_snapshot(
            SNAPSHOT, GROUP_VOLUME)

        v = self.driver
        v.vmem_mg.lun.copy_snapshot_to_new_lun.assert_called_once_with(
            source_lun=SNAPSHOT['volume_id'],
            source_snapshot_comment=compressed_snap_id,
            destination=VOLUME['id'],
            storage_pool_id=DEFAULT_THICK_POOL['storage_pool_id'])
        v._wait_for_lun_or_snap_copy.assert_called_once_with(
            SNAPSHOT['volume_id'], dest_vdev_id=vdev_id)
        v._ensure_snapshot_resource_area.assert_called_once_with(
            GROUP_VOLUME['id'])
        v._add_to_consistencygroup.assert_called_once_with(
            GROUP_ID, GROUP_VOLUME['id'])

        self.assertIsNone(result)

    def test_create_lun_from_lun(self):
        """Lun full clone to new volume completes successfully."""
        object_id = '12345'
        response = {'success': True,
                    'object_id': object_id,
                    'msg': 'Copy Snapshot resource successfully'}
        spec_dict = {'pool_type': 'thick'}
        lun_info = {'subType': 'THICK'}

        conf = {
            'lun.get_lun_info.return_value': lun_info,
            'lun.copy_lun_to_new_lun.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._ensure_snapshot_resource_area = mock.Mock()
        self.driver._process_extra_specs = mock.Mock(
            return_value=spec_dict)
        self.driver._get_storage_pool = mock.Mock(
            return_value=DEFAULT_THICK_POOL)
        self.driver._wait_for_lun_or_snap_copy = mock.Mock()

        result = self.driver._create_lun_from_lun(SRC_VOL, VOLUME)

        self.driver._ensure_snapshot_resource_area.assert_called_with(
            SRC_VOL['id'])
        self.driver.vmem_mg.lun.copy_lun_to_new_lun.assert_called_with(
            source=SRC_VOL['id'], destination=VOLUME['id'],
            storage_pool_id=DEFAULT_THICK_POOL['storage_pool_id'])
        self.driver._wait_for_lun_or_snap_copy.assert_called_with(
            SRC_VOL['id'], dest_obj_id=object_id)

        self.assertIsNone(result)

    def test_create_lun_from_lun_fails(self):
        """Lun full clone detects errors properly."""
        failure = exception.ViolinBackendErr
        lun_info = {'subType': 'THICK'}

        conf = {
            'lun.get_lun_info.return_value': lun_info,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._ensure_snapshot_resource_area = mock.Mock(
            side_effect=failure)

        self.assertRaises(failure,
                          self.driver._create_lun_from_lun, SRC_VOL, VOLUME)

    def test_create_lun_from_lun_fails_for_non_thick_luns(self):
        """Cloning a volume from a non-thick LUN is not supported."""
        failure = exception.ViolinBackendErr
        lun_info = {'subType': 'THIN'}

        conf = {
            'lun.get_lun_info.return_value': lun_info,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        self.assertRaises(failure,
                          self.driver._create_lun_from_lun, SRC_VOL, VOLUME)

    def test_create_consistencygroup_lun_from_lun(self):
        """Lun clone with a consistency group specified works ok."""
        object_id = '12345'
        vol = GROUP_VOLUME.copy()
        size_in_mb = vol['size'] * units.Ki
        response = {'success': True,
                    'object_id': object_id,
                    'msg': 'Copy Snapshot resource successfully'}
        spec_dict = {'pool_type': 'thick', 'size_mb': size_in_mb}
        lun_info = {'subType': 'THICK'}

        conf = {
            'lun.get_lun_info.return_value': lun_info,
            'lun.copy_lun_to_new_lun.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._ensure_snapshot_resource_area = mock.Mock(
            return_value=None)
        self.driver._process_extra_specs = mock.Mock(
            return_value=spec_dict)
        self.driver._get_storage_pool = mock.Mock(
            return_value=DEFAULT_THICK_POOL)
        self.driver._wait_for_lun_or_snap_copy = mock.Mock(return_value=None)
        self.driver._ensure_snapshot_resource_area = mock.Mock(
            return_value=None)
        self.driver._add_to_consistencygroup = mock.Mock(return_value=None)

        result = self.driver._create_lun_from_lun(SRC_VOL, vol)

        self.driver.vmem_mg.lun.copy_lun_to_new_lun.assert_called_once_with(
            source=SRC_VOL['id'], destination=vol['id'],
            storage_pool_id=DEFAULT_THICK_POOL['storage_pool_id'])
        self.driver._wait_for_lun_or_snap_copy.assert_called_once_with(
            SRC_VOL['id'], dest_obj_id=object_id)
        self.driver._add_to_consistencygroup.assert_called_once_with(
            vol['consistencygroup_id'], vol['id'])

        self.assertIsNone(result)

    def test_send_cmd(self):
        """Command callback completes successfully."""
        success_msg = 'success'
        request_args = ['arg1', 'arg2', 'arg3']
        response = {'success': True, 'msg': 'Operation successful'}

        request_func = mock.Mock(return_value=response)

        result = self.driver._send_cmd(request_func, success_msg, request_args)

        self.assertEqual(response, result)

    def test_send_cmd_request_timed_out(self):
        """The callback retry timeout hits immediately."""
        failure = exception.ViolinRequestRetryTimeout
        success_msg = 'success'
        request_args = ['arg1', 'arg2', 'arg3']
        self.conf.violin_request_timeout = 0

        request_func = mock.Mock()

        self.assertRaises(failure, self.driver._send_cmd,
                          request_func, success_msg, request_args)

    def test_send_cmd_response_has_no_message(self):
        """The callback returns no message on the first call."""
        success_msg = 'success'
        request_args = ['arg1', 'arg2', 'arg3']
        response1 = {'success': True, 'msg': None}
        response2 = {'success': True, 'msg': 'success'}

        request_func = mock.Mock(side_effect=[response1, response2])

        self.assertEqual(response2, self.driver._send_cmd
                         (request_func, success_msg, request_args))

    def test_send_cmd_and_verify(self):
        """Command callback completes successfully."""
        success_msg = 'success'
        request_return_value = {
            'success': True,
            'msg': success_msg,
        }

        request_func = mock.Mock(return_value=request_return_value)
        request_args = ['1', '2']
        verify_func = mock.Mock(return_value=True)
        verify_args = ['a', 'b']

        result = self.driver._send_cmd_and_verify(
            request_func, verify_func, success_msg, request_args, verify_args)

        self.assertEqual(request_return_value, result)
        request_func.assert_called_once_with(*request_args)
        verify_func.assert_called_once_with(*verify_args)

    def test_send_cmd_and_verify_fails(self):
        """Send command and verify detects errors correctly."""
        failure = exception.ViolinBackendErr
        success_msg = 'success'
        request_return_value = {
            'success': False,
            'msg': 'Broken',
        }

        request_func = mock.Mock(return_value=request_return_value)
        request_args = ['a', 1]
        verify_func = mock.Mock()
        self.driver._check_error_code = mock.Mock(
            side_effect=failure)

        self.assertRaises(failure,
                          self.driver._send_cmd_and_verify,
                          request_func, verify_func, success_msg, request_args)
        request_func.assert_called_once_with(*request_args)

    def test_send_cmd_and_verify_timeout(self):
        """Test that timeouts are handled appropriately."""
        success_msg = 'success'
        self.driver.config.violin_request_timeout = 0

        request_func = mock.Mock()
        verify_func = mock.Mock()

        self.assertRaises(exception.ViolinRequestRetryTimeout,
                          self.driver._send_cmd_and_verify,
                          request_func, verify_func, success_msg)

    @mock.patch.object(context, 'get_admin_context')
    def test_ensure_snapshot_resource_area_medium(self,
                                                  m_get_admin_context):
        """Create a SRA for a medium LUN."""
        vol = VOLUME.copy()
        vol['size'] = 1
        snap_size_mb = int(math.ceil(vol['size'] * units.Ki * 0.5))
        response = {'success': True, 'msg': 'success'}
        pool_type = 'thick'
        spec_dict = {'pool_type': pool_type}

        conf = {
            'snapshot.lun_has_a_snapshot_resource.return_value': False,
            'snapshot.create_snapshot_resource.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._process_extra_specs = mock.Mock(
            return_value=spec_dict)
        self.driver._get_storage_pool = mock.Mock(
            return_value=DEFAULT_THICK_POOL)

        with mock.patch('cinder.db.sqlalchemy.api.volume_get',
                        return_value=vol):
            result = self.driver._ensure_snapshot_resource_area(VOLUME_ID)

        v = self.driver.vmem_mg
        x = v7000_common
        self.driver._process_extra_specs.assert_called_once_with(vol)
        self.driver._get_storage_pool.assert_called_once_with(
            vol, snap_size_mb, pool_type, None)
        v.snapshot.create_snapshot_resource.assert_called_once_with(
            lun=VOLUME_ID,
            size=snap_size_mb,
            enable_notification=False,
            policy=x.CONCERTO_DEFAULT_SRA_POLICY,
            enable_expansion=x.CONCERTO_DEFAULT_SRA_ENABLE_EXPANSION,
            expansion_threshold=x.CONCERTO_DEFAULT_SRA_EXPANSION_THRESHOLD,
            expansion_increment=x.CONCERTO_DEFAULT_SRA_EXPANSION_INCREMENT,
            expansion_max_size=x.CONCERTO_DEFAULT_SRA_EXPANSION_MAX_SIZE,
            enable_shrink=x.CONCERTO_DEFAULT_SRA_ENABLE_SHRINK,
            storage_pool_id=DEFAULT_THICK_POOL['storage_pool_id'])

        self.assertIsNone(result)

    @mock.patch.object(context, 'get_admin_context')
    def test_ensure_snapshot_resource_area_large(self,
                                                 m_get_admin_context):
        """Create a SRA for a large LUN."""
        vol = VOLUME.copy()
        vol['size'] = 10
        snap_size_mb = int(math.ceil(vol['size'] * units.Ki * 0.2))
        response = {'success': True, 'msg': 'success'}
        pool_type = 'thick'
        spec_dict = {'pool_type': pool_type}

        conf = {
            'snapshot.lun_has_a_snapshot_resource.return_value': False,
            'snapshot.create_snapshot_resource.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._process_extra_specs = mock.Mock(
            return_value=spec_dict)
        self.driver._get_storage_pool = mock.Mock(
            return_value=DEFAULT_THICK_POOL)

        with mock.patch('cinder.db.sqlalchemy.api.volume_get',
                        return_value=vol):
            result = self.driver._ensure_snapshot_resource_area(VOLUME_ID)

        v = self.driver.vmem_mg
        x = v7000_common
        self.driver._process_extra_specs.assert_called_once_with(vol)
        self.driver._get_storage_pool.assert_called_once_with(
            vol, snap_size_mb, pool_type, None)
        v.snapshot.create_snapshot_resource.assert_called_once_with(
            lun=VOLUME_ID,
            size=snap_size_mb,
            enable_notification=False,
            policy=x.CONCERTO_DEFAULT_SRA_POLICY,
            enable_expansion=x.CONCERTO_DEFAULT_SRA_ENABLE_EXPANSION,
            expansion_threshold=x.CONCERTO_DEFAULT_SRA_EXPANSION_THRESHOLD,
            expansion_increment=x.CONCERTO_DEFAULT_SRA_EXPANSION_INCREMENT,
            expansion_max_size=x.CONCERTO_DEFAULT_SRA_EXPANSION_MAX_SIZE,
            enable_shrink=x.CONCERTO_DEFAULT_SRA_ENABLE_SHRINK,
            storage_pool_id=DEFAULT_THICK_POOL['storage_pool_id'])

        self.assertIsNone(result)

    @mock.patch.object(context, 'get_admin_context')
    def test_ensure_snapshot_resource_area_noop(self,
                                                m_get_admin_context):
        vol = VOLUME.copy()

        conf = {
            'snapshot.lun_has_a_snapshot_resource.return_value': True,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        with mock.patch('cinder.db.sqlalchemy.api.volume_get',
                        return_value=vol):
            result = self.driver._ensure_snapshot_resource_area(VOLUME_ID)

        v = self.driver.vmem_mg
        self.assertIsNone(result)
        v.snapshot.lun_has_a_snapshot_resource.assert_called_once_with(
            lun=vol['id'])

    def test_ensure_snapshot_resource_policy(self):
        result_dict = {'success': True, 'msg': 'Successful'}

        conf = {
            'snapshot.lun_has_a_snapshot_policy.return_value': False,
            'snapshot.create_snapshot_policy.return_value': result_dict,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        result = self.driver._ensure_snapshot_policy(VOLUME_ID)
        self.assertIsNone(result)

        v = self.driver.vmem_mg
        x = v7000_common
        v.snapshot.create_snapshot_policy.assert_called_once_with(
            lun=VOLUME_ID,
            max_snapshots=x.CONCERTO_DEFAULT_POLICY_MAX_SNAPSHOTS,
            enable_replication=False,
            enable_snapshot_schedule=False,
            enable_cdp=False,
            retention_mode=x.CONCERTO_DEFAULT_POLICY_RETENTION_MODE)

    def test_ensure_snapshot_resource_policy_noop(self):
        conf = {
            'snapshot.lun_has_a_snapshot_policy.return_value': True,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        v = self.driver.vmem_mg
        result = self.driver._ensure_snapshot_policy(VOLUME_ID)
        self.assertIsNone(result)
        v.snapshot.lun_has_a_snapshot_policy.assert_called_once_with(
            lun=VOLUME_ID)

    def test_delete_lun_snapshot_bookkeeping(self):
        result_dict = {'success': True, 'msg': 'Successful'}

        conf = {
            'snapshot.get_snapshots.return_value': [],
            'snapshot.delete_snapshot_policy.return_value': result_dict,
            'snapshot.delete_snapshot_resource.return_value': None,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        result = self.driver._delete_lun_snapshot_bookkeeping(
            volume_id=VOLUME_ID)

        self.assertIsNone(result)

        v = self.driver.vmem_mg
        v.snapshot.get_snapshots.assert_called_with(VOLUME_ID)
        v.snapshot.delete_snapshot_policy.assert_called_once_with(
            lun=VOLUME_ID)
        v.snapshot.delete_snapshot_resource.assert_called_once_with(
            lun=VOLUME_ID)

    def test_compress_snapshot_id(self):
        test_snap_id = "12345678-abcd-1234-cdef-0123456789ab"
        expected = "12345678abcd1234cdef0123456789ab"

        result = self.driver._compress_snapshot_id(test_snap_id)
        self.assertEqual(expected, result)

    def test_wait_for_lun_or_snap_copy_completes_for_snap(self):
        """waiting for a snapshot to copy succeeds."""
        vdev_id = 11111
        response = (vdev_id, None, 100)

        conf = {
            'snapshot.get_snapshot_copy_status.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        result = self.driver._wait_for_lun_or_snap_copy(
            SRC_VOL['id'], dest_vdev_id=vdev_id)

        (self.driver.vmem_mg.snapshot.get_snapshot_copy_status.
         assert_called_with(SRC_VOL['id']))
        self.assertTrue(result)

    def test_wait_for_lun_or_snap_copy_completes_for_lun(self):
        """waiting for a lun to copy succeeds."""
        object_id = '12345'
        response = (object_id, None, 100)

        conf = {
            'lun.get_lun_copy_status.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        result = self.driver._wait_for_lun_or_snap_copy(
            SRC_VOL['id'], dest_obj_id=object_id)

        self.driver.vmem_mg.lun.get_lun_copy_status.assert_called_with(
            SRC_VOL['id'])
        self.assertTrue(result)

    def test_is_supported_vmos_version(self):
        version = 'Version 7.5.6'
        self.driver.vmem_mg = self.setup_mock_concerto()

        result = self.driver._is_supported_vmos_version(version)

        self.assertTrue(result)

    def test_is_supported_vmos_version_returns_false(self):
        version = 'invalid'
        self.driver.vmem_mg = self.setup_mock_concerto()

        result = self.driver._is_supported_vmos_version(version)

        self.assertFalse(result)

    def test_check_error_code(self):
        """Return an exception for a valid error code."""
        failure = exception.ViolinBackendErr
        response = {'success': False, 'msg': 'Error: 0x90000000'}
        self.assertRaises(failure, self.driver._check_error_code,
                          response)

    def test_check_error_code_non_fatal_error(self):
        """Returns no exception for a non-fatal error code."""
        response = {'success': False, 'msg': 'Error: 0x9001003c'}
        self.assertIsNone(self.driver._check_error_code(response))

    @mock.patch.object(context, 'get_admin_context')
    @mock.patch.object(volume_types, 'get_volume_type')
    def test_get_volume_type_extra_spec(self,
                                        m_get_volume_type,
                                        m_get_admin_context):
        """Volume_type extra specs are found successfully."""
        vol = VOLUME.copy()
        vol['volume_type_id'] = 1
        volume_type = {'extra_specs': {'override:test_key': 'test_value'}}

        m_get_admin_context.return_value = None
        m_get_volume_type.return_value = volume_type

        result = self.driver._get_volume_type_extra_spec(vol, 'test_key')

        m_get_admin_context.assert_called_with()
        m_get_volume_type.assert_called_with(None, vol['volume_type_id'])
        self.assertEqual('test_value', result)

    @mock.patch.object(context, 'get_admin_context')
    @mock.patch.object(volume_types, 'get_volume_type')
    def test_get_violin_extra_spec(self,
                                   m_get_volume_type,
                                   m_get_admin_context):
        """Volume_type extra specs are found successfully."""
        vol = VOLUME.copy()
        vol['volume_type_id'] = 1
        volume_type = {'extra_specs': {'violin:test_key': 'test_value'}}

        m_get_admin_context.return_value = None
        m_get_volume_type.return_value = volume_type

        result = self.driver._get_volume_type_extra_spec(vol, 'test_key')

        m_get_admin_context.assert_called_with()
        m_get_volume_type.assert_called_with(None, vol['volume_type_id'])
        self.assertEqual('test_value', result)

    def test_process_extra_specs_no_thin_no_extra_specs(self):
        '''With nothing specified: thick LUN.'''
        vol = VOLUME.copy()
        vol['volume_type_id'] = None
        self.conf.san_thin_provision = False
        expected = {
            'pool_type': 'thick',
            'size_mb': vol['size'] * units.Ki,
            'thick': True,
            'thin': False,
            'dedup': False,
            'lun_encryption': False,
        }

        self.driver.vmem_mg = self.setup_mock_concerto()

        result = self.driver._process_extra_specs(vol)
        self.assertDictEqual(expected, result)

    def test_process_extra_specs_no_thin_dedup_or_encrypt_extra_specs(self):
        '''With volume type but no extra spec: thick LUN.'''
        vol = VOLUME.copy()
        vol['volume_type_id'] = 1
        self.conf.san_thin_provision = False
        expected = {
            'pool_type': 'thick',
            'size_mb': vol['size'] * units.Ki,
            'thick': True,
            'thin': False,
            'dedup': False,
            'lun_encryption': False,
        }

        self.driver.vmem_mg = self.setup_mock_concerto()
        self.driver._get_volume_type_extra_spec = mock.Mock(
            return_value=None)

        result = self.driver._process_extra_specs(vol)
        self.assertDictEqual(expected, result)
        self.assertEqual(3, self.driver._get_volume_type_extra_spec.call_count)

    def test_process_extra_specs_no_thin_with_thin_extra_specs(self):
        '''With volume type and thin extra spec: thin LUN.'''
        vol = VOLUME.copy()
        vol['volume_type_id'] = 1
        self.conf.san_thin_provision = False
        expected = {
            'pool_type': 'thin',
            'size_mb': (vol['size'] * units.Ki) // 10,
            'thick': False,
            'thin': True,
            'dedup': False,
            'lun_encryption': False,
        }
        responses = ['True', None, 'False']

        self.driver.vmem_mg = self.setup_mock_concerto()
        self.driver._get_volume_type_extra_spec = mock.Mock(
            side_effect=responses)

        result = self.driver._process_extra_specs(vol)
        self.assertDictEqual(expected, result)
        self.assertEqual(3, self.driver._get_volume_type_extra_spec.call_count)

    def test_process_extra_specs_no_thin_with_dedup_extra_specs(self):
        '''With volume type and dedup extra spec: dedup LUN.'''
        vol = VOLUME.copy()
        vol['volume_type_id'] = 1
        self.conf.san_thin_provision = False
        expected = {
            'pool_type': 'dedup',
            'size_mb': (vol['size'] * units.Ki) // 10,
            'thick': False,
            'thin': True,
            'dedup': True,
            'lun_encryption': False,
        }
        responses = [None, 'True', 'False']

        self.driver.vmem_mg = self.setup_mock_concerto()
        self.driver._get_volume_type_extra_spec = mock.Mock(
            side_effect=responses)

        result = self.driver._process_extra_specs(vol)
        self.assertDictEqual(expected, result)
        self.assertEqual(3, self.driver._get_volume_type_extra_spec.call_count)

    def test_process_extra_specs_no_thin_with_all_extra_specs(self):
        '''With volume type and thin & dedup extra spec: dedup LUN.'''
        vol = VOLUME.copy()
        vol['volume_type_id'] = 1
        self.conf.san_thin_provision = False
        expected = {
            'pool_type': 'dedup',
            'size_mb': (vol['size'] * units.Ki) // 10,
            'thick': False,
            'thin': True,
            'dedup': True,
            'lun_encryption': True,
        }

        self.driver.vmem_mg = self.setup_mock_concerto()
        self.driver._get_volume_type_extra_spec = mock.Mock(
            return_value='True')

        result = self.driver._process_extra_specs(vol)
        self.assertDictEqual(expected, result)
        self.assertEqual(3, self.driver._get_volume_type_extra_spec.call_count)

    def test_process_extra_specs_with_thin_no_volume_type(self):
        '''With san_thin_provision but no volume type: thin LUN.'''
        vol = VOLUME.copy()
        vol['volume_type_id'] = None
        self.conf.san_thin_provision = True
        expected = {
            'pool_type': 'thin',
            'size_mb': (vol['size'] * units.Ki) // 10,
            'thick': False,
            'thin': True,
            'dedup': False,
            'lun_encryption': False,
        }

        self.driver.vmem_mg = self.setup_mock_concerto()

        result = self.driver._process_extra_specs(vol)
        self.assertDictEqual(expected, result)

    def test_process_extra_specs_thin_no_dedup_or_encrypt_extra_specs(self):
        '''With san_thin_provision / voltype, no extra specs: thin LUN.'''
        vol = VOLUME.copy()
        vol['volume_type_id'] = 1
        self.conf.san_thin_provision = True
        expected = {
            'pool_type': 'thin',
            'size_mb': (vol['size'] * units.Ki) // 10,
            'thick': False,
            'thin': True,
            'dedup': False,
            'lun_encryption': False,
        }

        self.driver.vmem_mg = self.setup_mock_concerto()
        self.driver._get_volume_type_extra_spec = mock.Mock(
            return_value=None)

        result = self.driver._process_extra_specs(vol)
        self.assertDictEqual(expected, result)
        self.assertEqual(3, self.driver._get_volume_type_extra_spec.call_count)

    def test_process_extra_specs_with_thin_and_thin_extra_specs(self):
        '''With san_thin_provision and thin extra spec: thin LUN.'''
        vol = VOLUME.copy()
        vol['volume_type_id'] = 1
        self.conf.san_thin_provision = True
        expected = {
            'pool_type': 'thin',
            'size_mb': (vol['size'] * units.Ki) // 10,
            'thick': False,
            'thin': True,
            'dedup': False,
            'lun_encryption': False,
        }
        responses = ['True', None, 'False']

        self.driver.vmem_mg = self.setup_mock_concerto()
        self.driver._get_volume_type_extra_spec = mock.Mock(
            side_effect=responses)

        result = self.driver._process_extra_specs(vol)
        self.assertDictEqual(expected, result)
        self.assertEqual(3, self.driver._get_volume_type_extra_spec.call_count)

    def test_process_extra_specs_thin_with_dedup_extra_specs(self):
        '''With san_thin_provision and dedup extra spec: dedup LUN.'''
        vol = VOLUME.copy()
        vol['volume_type_id'] = 1
        self.conf.san_thin_provision = True
        expected = {
            'pool_type': 'dedup',
            'size_mb': (vol['size'] * units.Ki) // 10,
            'thick': False,
            'thin': True,
            'dedup': True,
            'lun_encryption': False,
        }
        responses = [None, 'True', 'False']

        self.driver.vmem_mg = self.setup_mock_concerto()
        self.driver._get_volume_type_extra_spec = mock.Mock(
            side_effect=responses)

        result = self.driver._process_extra_specs(vol)
        self.assertDictEqual(expected, result)
        self.assertEqual(3, self.driver._get_volume_type_extra_spec.call_count)

    def test_process_extra_specs_no_thin_with_encrypt_extra_specs(self):
        '''With san_thin_provision and encrypt extra spec: thin LUN.'''
        vol = VOLUME.copy()
        vol['volume_type_id'] = 1
        self.conf.san_thin_provision = True
        expected = {
            'pool_type': 'dedup',
            'size_mb': (vol['size'] * units.Ki) // 10,
            'thick': False,
            'thin': True,
            'dedup': True,
            'lun_encryption': True,
        }
        responses = [None, 'True', 'True']

        self.driver.vmem_mg = self.setup_mock_concerto()
        self.driver._get_volume_type_extra_spec = mock.Mock(
            side_effect=responses)

        result = self.driver._process_extra_specs(vol)
        self.assertDictEqual(expected, result)
        self.assertEqual(3, self.driver._get_volume_type_extra_spec.call_count)

    def test_process_extra_specs_with_thin_with_all_extra_specs(self):
        '''With san_thin_provision and thin & dedup extra spec: dedup LUN.'''
        vol = VOLUME.copy()
        vol['volume_type_id'] = 1
        self.conf.san_thin_provision = True
        expected = {
            'pool_type': 'dedup',
            'size_mb': (vol['size'] * units.Ki) // 10,
            'thick': False,
            'thin': True,
            'dedup': True,
            'lun_encryption': True,
        }

        self.driver.vmem_mg = self.setup_mock_concerto()
        self.driver._get_volume_type_extra_spec = mock.Mock(
            return_value='True')

        result = self.driver._process_extra_specs(vol)
        self.assertDictEqual(expected, result)
        self.assertEqual(3, self.driver._get_volume_type_extra_spec.call_count)

    def test_get_storage_pool_with_extra_specs(self):
        '''Select a suitable pool based on specified extra specs.'''
        vol = VOLUME.copy()
        vol['volume_type_id'] = 1
        pool_type = "thick"

        self.conf.violin_dedup_only_pools = ['PoolA', 'PoolB']
        self.conf.violin_dedup_capable_pools = ['PoolC', 'PoolD']

        selected_pool = {
            'storage_pool': 'StoragePoolA',
            'storage_pool_id': 99,
            'dedup': False,
            'thin': False,
        }

        conf = {
            'pool.select_storage_pool.return_value': selected_pool,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._get_violin_extra_spec = mock.Mock(
            return_value="StoragePoolA")

        result = self.driver._get_storage_pool(
            vol, 100, pool_type, "create_lun")

        self.assertDictEqual(result, selected_pool)

    def test_get_storage_pool_configured_pools(self):
        '''Select a suitable pool based on configured pools.'''
        vol = VOLUME.copy()
        pool_type = "dedup"

        self.conf.violin_dedup_only_pools = ['PoolA', 'PoolB']
        self.conf.violin_dedup_capable_pools = ['PoolC', 'PoolD']

        selected_pool = {
            'dedup': True,
            'storage_pool': 'PoolA',
            'storage_pool_id': 123,
            'thin': True,
        }

        conf = {
            'pool.select_storage_pool.return_value': selected_pool,
        }

        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._get_violin_extra_spec = mock.Mock(
            return_value="StoragePoolA")

        result = self.driver._get_storage_pool(
            vol, 100, pool_type, "create_lun")

        self.assertEqual(result, selected_pool)
        self.driver.vmem_mg.pool.select_storage_pool.assert_called_with(
            100,
            pool_type,
            None,
            self.conf.violin_dedup_only_pools,
            self.conf.violin_dedup_capable_pools,
            "random",
            "create_lun",
        )

    def test_wait_run_delete_lun_snapshot(self):
        response = {'success': True, 'msg': 'Delete TimeMark successfully'}
        compressed_snap_id = 'abcdabcd1234abcd1234abcdeffedcbb'
        oid = 'abc123-abc123abc123-abc123'

        conf = {
            'snapshot.snapshot_comment_to_object_id.return_value': oid,
            'snapshot.delete_lun_snapshot.return_value': response,
        }

        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._compress_snapshot_id = mock.Mock(
            return_value=compressed_snap_id)

        result = self.driver._wait_run_delete_lun_snapshot(SNAPSHOT)

        v = self.driver.vmem_mg
        self.assertTrue(result is None)
        self.driver._compress_snapshot_id.assert_called_once_with(
            SNAPSHOT['id'])
        v.snapshot.delete_lun_snapshot.assert_called_once_with(
            snapshot_object_id=oid)

    def test_wait_run_delete_lun_snapshot_with_retry(self):
        response = [
            {'success': False, 'msg': 'Error 0x50f7564c'},
            {'success': True, 'msg': 'Delete TimeMark successfully'}]
        compressed_snap_id = 'abcdabcd1234abcd1234abcdeffedcbb'
        oid = 'abc123-abc123abc123-abc123'

        conf = {
            'snapshot.snapshot_comment_to_object_id.return_value': oid,
            'snapshot.delete_lun_snapshot.side_effect': response,
        }

        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._compress_snapshot_id = mock.Mock(
            return_value=compressed_snap_id)

        result = self.driver._wait_run_delete_lun_snapshot(SNAPSHOT)

        self.assertIsNone(result)
        self.driver._compress_snapshot_id.assert_called_once_with(
            SNAPSHOT['id'])
        self.assertEqual(
            len(response),
            self.driver.vmem_mg.snapshot.delete_lun_snapshot.call_count)

    @mock.patch('socket.getfqdn')
    def test_get_volume_stats(self, m_getfqdn):
        expected_answers = {
            'vendor_name': 'Violin Memory, Inc.',
            'reserved_percentage': 0,
            'QoS_support': False,
            'free_capacity_gb': 2781,
            'total_capacity_gb': 14333,
            'consistencygroup_support': True,
        }
        owner = 'lab-host1'

        def lookup(value):
            return str(value) + '.example.com'
        m_getfqdn.side_effect = lookup

        conf = {
            'pool.get_storage_pools.return_value': STATS_STORAGE_POOL_RESPONSE,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        result = self.driver._get_volume_stats(owner)

        self.assertDictEqual(expected_answers, result)

    def test_create_consistencygroup(self):
        response = {'success': True, 'msg': 'success'}
        context = None

        conf = {
            'snapshot.create_snapgroup.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        result = self.driver._create_consistencygroup(context, GROUP)

        self.driver.vmem_mg.snapshot.create_snapgroup.assert_called_once_with(
            GROUP['id'])
        self.assertIsNone(result)

    def test_create_consistencygroup_fails(self):
        response = {'success': False, 'msg': 'failed'}
        context = None

        conf = {
            'snapshot.create_snapgroup.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        self.assertRaises(exception.ViolinBackendErr,
                          self.driver._create_consistencygroup,
                          context, GROUP)

    def test_delete_consistencygroup(self):
        expected_model_update = {'status': 'deleted'}
        expected_volumes = [GROUP_VOLUME.copy(), ]
        expected_volumes[0]['status'] = 'deleted'

        response = {'success': True, 'msg': 'success'}
        group = GROUP.copy()
        group['status'] = 'deleted'
        vol = GROUP_VOLUME.copy()
        group_info = {
            'timemarkEnabled': True,
            'members': [
                {'name': vol['id']},
            ],
        }

        conf = {
            'snapshot.get_snapgroup_info.return_value': group_info,
            'snapshot.delete_snapgroup_policy.return_value': response,
            'snapshot.delete_snapgroup.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        conf = {
            'volume_get_all_by_group.return_value': [vol.copy(), ],
        }
        db = self.setup_mock_db(conf)

        self.driver._remove_from_consistencygroup = mock.Mock()
        self.driver._delete_lun = mock.Mock()

        model_update, volumes = self.driver._delete_consistencygroup(
            None, group, db)

        v = self.driver.vmem_mg
        self.assertDictEqual(expected_model_update, model_update)
        self.assertEqual(expected_volumes, volumes)
        v.snapshot.get_snapgroup_info.assert_called_once_with(
            group['id'])
        v.snapshot.delete_snapgroup_policy.assert_called_once_with(
            group['id'])
        self.driver._remove_from_consistencygroup.assert_called_once_with(
            group['id'], [vol['id'], ])
        v.snapshot.delete_snapgroup.assert_called_once_with(
            group['id'])
        self.driver._delete_lun.assert_called_once_with(
            {'id': vol['id']})

    def test_delete_consistencygroup_no_sra_policy(self):
        expected_model_update = {'status': 'deleted'}
        expected_volumes = [GROUP_VOLUME.copy(), ]
        expected_volumes[0]['status'] = 'deleted'

        response = {'success': True, 'msg': 'success'}
        group = GROUP.copy()
        group['status'] = 'deleted'
        vol = GROUP_VOLUME.copy()
        group_info = {
            'timemarkEnabled': False,
            'members': [
                {'name': vol['id']},
            ],
        }

        conf = {
            'snapshot.get_snapgroup_info.return_value': group_info,
            'snapshot.delete_snapgroup.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        conf = {
            'volume_get_all_by_group.return_value': [vol.copy(), ],
        }
        db = self.setup_mock_db(conf)

        self.driver._remove_from_consistencygroup = mock.Mock()
        self.driver._delete_lun = mock.Mock()

        model_update, volumes = self.driver._delete_consistencygroup(
            None, group, db)

        v = self.driver.vmem_mg
        self.assertDictEqual(expected_model_update, model_update)
        self.assertEqual(expected_volumes, volumes)
        v.snapshot.get_snapgroup_info.assert_called_once_with(
            group['id'])
        self.driver._remove_from_consistencygroup.assert_called_once_with(
            group['id'], [vol['id'], ])
        v.snapshot.delete_snapgroup.assert_called_once_with(
            group['id'])
        self.driver._delete_lun.assert_called_once_with(
            {'id': vol['id']})

    def test_delete_consistencygroup_empty_consistencygroup(self):
        expected_model_update = {'status': 'deleted'}
        expected_volumes = []

        response = {'success': True, 'msg': 'success'}
        group = GROUP.copy()
        group['status'] = 'deleted'
        group_info = {
            'timemarkEnabled': False,
            'members': [],
        }

        conf = {
            'snapshot.get_snapgroup_info.return_value': group_info,
            'snapshot.delete_snapgroup.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        conf = {
            'volume_get_all_by_group.return_value': [],
        }
        db = self.setup_mock_db(conf)

        model_update, volumes = self.driver._delete_consistencygroup(
            None, group, db)

        v = self.driver.vmem_mg
        self.assertDictEqual(expected_model_update, model_update)
        self.assertEqual(expected_volumes, volumes)
        v.snapshot.get_snapgroup_info.assert_called_once_with(
            group['id'])
        v.snapshot.delete_snapgroup.assert_called_once_with(
            group['id'])

    def test_delete_consistencygroup_delete_volume_failure(self):
        expected_model_update = {'status': 'deleted'}
        expected_volumes = [GROUP_VOLUME.copy(), ]
        expected_volumes[0]['status'] = 'error_deleting'

        response = {'success': True, 'msg': 'success'}
        group = GROUP.copy()
        group['status'] = 'deleted'
        vol = GROUP_VOLUME.copy()
        group_info = {
            'timemarkEnabled': True,
            'members': [
                {'name': vol['id']},
            ],
        }

        conf = {
            'snapshot.get_snapgroup_info.return_value': group_info,
            'snapshot.delete_snapgroup_policy.return_value': response,
            'snapshot.delete_snapgroup.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        conf = {
            'volume_get_all_by_group.return_value': [vol.copy(), ],
        }
        db = self.setup_mock_db(conf)

        self.driver._remove_from_consistencygroup = mock.Mock()
        self.driver._delete_lun = mock.Mock(
            side_effect=exception.ViolinBackendErr)

        model_update, volumes = self.driver._delete_consistencygroup(
            None, group, db)

        v = self.driver.vmem_mg
        self.assertDictEqual(expected_model_update, model_update)
        self.assertEqual(expected_volumes, volumes)
        v.snapshot.get_snapgroup_info.assert_called_once_with(
            group['id'])
        v.snapshot.delete_snapgroup_policy.assert_called_once_with(
            group['id'])
        self.driver._remove_from_consistencygroup.assert_called_once_with(
            group['id'], [vol['id'], ])
        v.snapshot.delete_snapgroup.assert_called_once_with(
            group['id'])
        self.driver._delete_lun.assert_called_once_with(
            {'id': vol['id']})

    def test_update_consistencygroup(self):
        expected = (None, None, None)
        context = None
        add_volumes = [GROUP_VOLUME, ]
        remove_volumes = [GROUP_VOLUME, ]

        self.driver._add_to_consistencygroup = mock.Mock()
        self.driver._remove_from_consistencygroup = mock.Mock()

        result = self.driver._update_consistencygroup(
            context, GROUP, add_volumes, remove_volumes)

        self.assertEqual(expected, result)
        self.driver._add_to_consistencygroup.assert_called_once_with(
            GROUP_ID, [VOLUME_ID, ])
        self.driver._remove_from_consistencygroup.assert_called_once_with(
            GROUP_ID, [VOLUME_ID, ])

    def test_add_to_consistencygroup_with_string_input(self):
        response = {'success': True, 'msg': 'ok'}

        self.driver._ensure_snapshot_resource_area = mock.Mock(
            return_value=None)

        conf = {
            'snapshot.add_luns_to_snapgroup.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        result = self.driver._add_to_consistencygroup(GROUP_ID, VOLUME_ID)

        v = self.driver.vmem_mg
        self.assertIsNone(result)
        self.driver._ensure_snapshot_resource_area.assert_called_once_with(
            VOLUME_ID)
        v.snapshot.add_luns_to_snapgroup.assert_called_once_with(
            GROUP_ID, [VOLUME_ID, ])

    def test_add_to_consistencygroup_with_list_input(self):
        response = {'success': True, 'msg': 'ok'}

        self.driver._ensure_snapshot_resource_area = mock.Mock(
            return_value=None)

        conf = {
            'snapshot.add_luns_to_snapgroup.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        result = self.driver._add_to_consistencygroup(GROUP_ID, [VOLUME_ID, ])

        v = self.driver.vmem_mg
        self.assertIsNone(result)
        self.driver._ensure_snapshot_resource_area.assert_called_once_with(
            VOLUME_ID)
        v.snapshot.add_luns_to_snapgroup.assert_called_once_with(
            GROUP_ID, [VOLUME_ID, ])

    def test_add_to_consistencygroup_fails(self):
        response = {'success': False, 'msg': 'failed'}
        self.driver._ensure_snapshot_resource_area = mock.Mock(
            return_value=None)

        conf = {
            'snapshot.add_luns_to_snapgroup.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        self.assertRaises(exception.ViolinBackendErr,
                          self.driver._add_to_consistencygroup,
                          GROUP_ID, VOLUME_ID)

    def test_remove_from_consistencygroup(self):
        response = {'success': True, 'msg': 'ok'}
        conf = {
            'snapshot.remove_luns_from_snapgroup.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        result = self.driver._remove_from_consistencygroup(GROUP_ID, VOLUME_ID)

        v = self.driver.vmem_mg
        self.assertIsNone(result)
        v.snapshot.remove_luns_from_snapgroup.assert_called_once_with(
            GROUP_ID, VOLUME_ID)

    def test_remove_from_consistencygroup_fails(self):
        response = {'success': False, 'msg': 'failed'}
        conf = {
            'snapshot.remove_luns_from_snapgroup.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        self.assertRaises(exception.ViolinBackendErr,
                          self.driver._remove_from_consistencygroup,
                          GROUP_ID, VOLUME_ID)

    def test_create_cgsnapshot(self):
        expected_model_update = {'status': 'available'}
        expected_snapshots = [SNAPSHOT.copy(), ]
        expected_snapshots[0]['status'] = 'available'

        context = None
        response = {'success': True, 'msg': 'success'}
        compressed_snap_id = 'aabbccdd1221dcba4334abcdeffedcba'
        snaps = [SNAPSHOT.copy(), ]

        conf = {
            'snapshot.create_snapgroup_snapshot.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        conf = {
            'snapshot_get_all_for_cgsnapshot.return_value': snaps,
        }
        db = self.setup_mock_db(conf)

        self.driver._compress_snapshot_id = mock.Mock(
            return_value=compressed_snap_id)
        self.driver._ensure_consistencygroup_policy = mock.Mock()
        self.driver._wait_for_cgsnapshot = mock.Mock()

        model_update, snapshots = self.driver._create_cgsnapshot(
            context, CGSNAPSHOT, db)

        v = self.driver.vmem_mg
        self.assertDictEqual(expected_model_update, model_update)
        self.assertEqual(expected_snapshots, snapshots)
        self.driver._compress_snapshot_id.assert_called_once_with(
            CGSNAPSHOT_ID)
        self.driver._ensure_consistencygroup_policy.assert_called_once_with(
            GROUP_ID)
        v.snapshot.create_snapgroup_snapshot.assert_called_once_with(
            name=GROUP_ID,
            comment=compressed_snap_id,
            priority=v7000_common.CONCERTO_DEFAULT_PRIORITY,
            enable_notification=False,
        )
        db.snapshot_get_all_for_cgsnapshot.assert_called_once_with(
            context, CGSNAPSHOT_ID)
        self.driver._wait_for_cgsnapshot.assert_called_once_with(
            GROUP_ID, compressed_snap_id, snaps)

    def test_create_cgsnapshot_fails(self):
        context = None
        response = {'success': False, 'msg': 'failed'}
        compressed_snap_id = 'aabbccdd1221dcba4334abcdeffedcba'

        conf = {
            'snapshot.create_snapgroup_snapshot.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        db = self.setup_mock_db()

        self.driver._compress_snapshot_id = mock.Mock(
            return_value=compressed_snap_id)
        self.driver._ensure_consistencygroup_policy = mock.Mock()

        self.assertRaises(exception.ViolinBackendErr,
                          self.driver._create_cgsnapshot,
                          context, CGSNAPSHOT, db)

    def test_wait_for_cgsnapshot(self):
        group = GROUP.copy()
        comment = 'aabbccdd1221dcba4334abcdeffedcba'
        snapshots = [SNAPSHOT.copy(), ]
        oid = '123456_654321'
        snap_info = {'id': oid, 'totalUsedBlocks': '384.00 KB'}

        conf = {
            'snapshot.snapshot_comment_to_object_id.return_value': oid,
            'snapshot.get_snapshot_info.return_value': snap_info,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        result = self.driver._wait_for_cgsnapshot(group, comment, snapshots)

        v = self.driver.vmem_mg
        self.assertIsNone(result)
        v.snapshot.snapshot_comment_to_object_id.assert_called_once_with(
            SNAPSHOT['volume_id'], comment)

    def test_delete_cgsnapshot(self):
        expected_model_update = {'status': 'deleted'}
        expected_snapshots = [SNAPSHOT.copy(), ]
        expected_snapshots[0]['status'] = 'deleted'

        response = {'success': True, 'msg': 'success'}
        group = GROUP.copy()
        cgsnapshot = CGSNAPSHOT.copy()
        cgsnapshot['status'] = 'deleted'
        comment = 'aabbccdd1221dcba4334abcdeffedcba'
        snapshots = [SNAPSHOT.copy(), ]
        oid = '123456_654321'
        snap_info = {'id': oid, 'totalUsedBlocks': '384.00 KB'}
        context = None
        snaps = [
            SNAPSHOT.copy(),
        ]

        conf = {
            'snapshot.snapgroup_snapshot_comment_to_object_id.return_value':
                oid,
            'snapshot.delete_snapgroup_snapshot.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        conf = {
            'snapshot_get_all_for_cgsnapshot.return_value': snaps,
        }
        db = self.setup_mock_db(conf)
        self.driver._compress_snapshot_id = mock.Mock(
            return_value=comment)

        model_update, snapshots = self.driver._delete_cgsnapshot(
            context, cgsnapshot, db)

        v = self.driver.vmem_mg
        self.assertDictEqual(expected_model_update, model_update)
        self.assertEqual(expected_snapshots, snapshots)
        v.snapshot.delete_snapgroup_snapshot.assert_called_once_with(
            snapshot_object_id=oid)
        db.snapshot_get_all_for_cgsnapshot.assert_called_once_with(
            context, CGSNAPSHOT_ID)

    def test_delete_cgsnapshot_with_retry(self):
        expected_model_update = {'status': 'deleted'}
        expected_snapshots = [SNAPSHOT.copy(), ]
        expected_snapshots[0]['status'] = 'deleted'

        responses = [
            {'success': False, 'msg': 'failed'},
            {'success': True, 'msg': 'success'},
        ]
        group = GROUP.copy()
        cgsnapshot = CGSNAPSHOT.copy()
        cgsnapshot['status'] = 'deleted'
        comment = 'aabbccdd1221dcba4334abcdeffedcba'
        snapshots = [SNAPSHOT.copy(), ]
        oid = '123456_654321'
        snap_info = {'id': oid, 'totalUsedBlocks': '384.00 KB'}
        context = None
        snaps = [
            SNAPSHOT.copy(),
        ]

        conf = {
            'snapshot.snapgroup_snapshot_comment_to_object_id.return_value':
                oid,
            'snapshot.delete_snapgroup_snapshot.side_effect': responses,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        conf = {
            'snapshot_get_all_for_cgsnapshot.return_value': snaps,
        }
        db = self.setup_mock_db(conf)
        self.driver._compress_snapshot_id = mock.Mock(
            return_value=comment)

        model_update, snapshots = self.driver._delete_cgsnapshot(
            context, cgsnapshot, db)

        v = self.driver.vmem_mg
        self.assertDictEqual(expected_model_update, model_update)
        self.assertEqual(expected_snapshots, snapshots)
        v.snapshot.delete_snapgroup_snapshot.assert_called_with(
            snapshot_object_id=oid)
        self.assertEqual(
            len(responses),
            v.snapshot.delete_snapgroup_snapshot.call_count)
        db.snapshot_get_all_for_cgsnapshot.assert_called_once_with(
            context, CGSNAPSHOT_ID)

    def test_ensure_consistencygroup_policy(self):
        response = {'success': True, 'msg': 'success'}
        group_info = {'timemarkEnabled': False}
        conf = {
            'snapshot.get_snapgroup_info.return_value': group_info,
            'snapshot.create_snapgroup_policy.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        result = self.driver._ensure_consistencygroup_policy(
            CGSNAPSHOT_ID)

        v = self.driver.vmem_mg
        x = v7000_common
        self.assertIsNone(result)
        v.snapshot.create_snapgroup_policy.assert_called_once_with(
            snapgroup=CGSNAPSHOT_ID,
            max_snapshots=x.CONCERTO_DEFAULT_POLICY_MAX_SNAPSHOTS,
            enable_replication=False,
            enable_snapshot_schedule=False,
            enable_cdp=False,
            retention_mode=x.CONCERTO_DEFAULT_POLICY_RETENTION_MODE,
        )

    def test_ensure_consistencygroup_policy_noop(self):
        group_info = {'timemarkEnabled': True}
        conf = {
            'snapshot.get_snapgroup_info.return_value': group_info,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        result = self.driver._ensure_consistencygroup_policy(
            CGSNAPSHOT_ID)

        v = self.driver.vmem_mg
        x = v7000_common
        self.assertIsNone(result)
        v.snapshot.get_snapgroup_info.assert_called_once_with(CGSNAPSHOT_ID)

    def test_create_consistencygroup_from_src_for_cgsnapshot(self):
        expected = (None, None)
        context = None
        group = GROUP.copy()
        volumes = [GROUP_VOLUME.copy(), ]
        cgsnapshot = CGSNAPSHOT.copy()
        snapshots = [SNAPSHOT.copy(), ]
        source_cg = None
        source_vols = None

        self.driver._create_consistencygroup_from_cgsnapshot = mock.Mock(
            return_value=expected)

        result = self.driver._create_consistencygroup_from_src(
            context, group, volumes, cgsnapshot,
            snapshots, source_cg, source_vols)

        v = self.driver._create_consistencygroup_from_cgsnapshot
        v.assert_called_once_with(context, group, volumes,
                                  cgsnapshot, snapshots)
        self.assertEqual(expected, result)

    def test_create_consistencygroup_from_src_for_consistencygroup(self):
        expected = (None, None)
        context = None
        group = GROUP.copy()
        volumes = [GROUP_VOLUME.copy(), ]
        cgsnapshot = None
        snapshots = None
        source_cg = GROUP.copy()
        source_vols = [GROUP_VOLUME.copy(), ]

        self.driver._create_consistencygroup_from_consistencygroup = mock.Mock(
            return_value=expected)

        result = self.driver._create_consistencygroup_from_src(
            context, group, volumes, cgsnapshot,
            snapshots, source_cg, source_vols)

        v = self.driver._create_consistencygroup_from_consistencygroup
        v.assert_called_once_with(context, group, volumes,
                                  source_cg, source_vols)
        self.assertEqual(expected, result)

    def test_create_consistencygroup_from_cgsnapshot(self):
        expected = (None, None)
        context = None
        group = GROUP.copy()
        volumes = [GROUP_VOLUME.copy(), ]
        cgsnapshot = CGSNAPSHOT.copy()
        snapshots = [SNAPSHOT.copy(), ]
        modified_snapshot = SNAPSHOT.copy()
        modified_snapshot['id'] = cgsnapshot['id']

        self.driver._create_consistencygroup = mock.Mock(
            return_value=None)
        self.driver._create_volume_from_snapshot = mock.Mock(
            return_value=None)

        result = self.driver._create_consistencygroup_from_cgsnapshot(
            context, group, volumes, cgsnapshot, snapshots)

        self.driver._create_consistencygroup.assert_called_once_with(
            context, group)
        self.driver._create_volume_from_snapshot.assert_called_once_with(
            modified_snapshot, volumes[0])
        self.assertEqual(expected, result)

    @mock.patch('uuid.uuid4')
    def test_create_consistencygroup_from_consistencygroup(self, m_uuid4):
        expected = (None, None)

        context = None
        group = GROUP.copy()
        volumes = [VOLUME.copy(), ]
        for x in volumes:
            x['consistencygroup_id'] = group['id']
        source_cg = SRC_GROUP.copy()
        source_vols = [SRC_VOL.copy(), ]
        for x in source_vols:
            x['consistencygroup_id'] = source_cg['id']
        oid = 'abc123-abc123abc123-abc123'
        snapshots = [
            {'volume_id': x['id'], 'display_name': None, 'id': UUID4}
            for x in source_vols]
        cgsnapshot = {'id': UUID4}

        response = {'success': True, 'msg': 'success'}
        m_uuid4.return_value = UUID4
        self.driver._compress_snapshot_id = mock.Mock(
            return_value=UUID4_COMPRESSED)
        self.driver._ensure_consistencygroup_policy = mock.Mock(
            return_value=None)
        self.driver._wait_for_cgsnapshot = mock.Mock(return_value=None)
        self.driver._create_consistencygroup_from_cgsnapshot = mock.Mock(
            return_value=None)
        conf = {
            'snapshot.create_snapgroup_snapshot.return_value': response,
            'snapshot.snapgroup_snapshot_comment_to_object_id.return_value':
                oid,
            'snapshot.delete_snapgroup_snapshot.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        result = self.driver._create_consistencygroup_from_consistencygroup(
            context, group, volumes, source_cg, source_vols)

        x = self.driver
        v = self.driver.vmem_mg.snapshot
        self.assertEqual(expected, result)
        x._compress_snapshot_id.assert_called_once_with(UUID4)
        x._ensure_consistencygroup_policy.assert_called_once_with(
            SRC_GROUP_ID)
        v.create_snapgroup_snapshot.assert_called_once_with(
            name=SRC_GROUP_ID,
            comment=UUID4_COMPRESSED,
            priority=v7000_common.CONCERTO_DEFAULT_PRIORITY,
            enable_notification=False)
        x._wait_for_cgsnapshot.assert_called_once_with(
            SRC_GROUP_ID, UUID4_COMPRESSED, snapshots)
        x._create_consistencygroup_from_cgsnapshot.assert_called_once_with(
            context, group, volumes, cgsnapshot, snapshots)
        v.snapgroup_snapshot_comment_to_object_id.assert_called_once_with(
            SRC_GROUP_ID, UUID4_COMPRESSED)
        v.delete_snapgroup_snapshot.assert_called_once_with(
            snapshot_object_id=oid)

    @mock.patch('uuid.uuid4')
    def test_create_consistencygroup_from_consistencygroup_raises_error(
            self, m_uuid4):
        context = None
        group = GROUP.copy()
        volumes = [VOLUME.copy(), ]
        for x in volumes:
            x['consistencygroup_id'] = group['id']
        source_cg = SRC_GROUP.copy()
        source_vols = [SRC_VOL.copy(), ]
        for x in source_vols:
            x['consistencygroup_id'] = source_cg['id']

        response = {'success': False, 'msg': 'failed'}
        m_uuid4.return_value = UUID4
        self.driver._compress_snapshot_id = mock.Mock(
            return_value=UUID4_COMPRESSED)
        self.driver._ensure_consistencygroup_policy = mock.Mock(
            return_value=None)
        conf = {
            'snapshot.create_snapgroup_snapshot.return_value': response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        x = self.driver
        self.assertRaises(exception.ViolinBackendErr,
                          x._create_consistencygroup_from_consistencygroup,
                          context, group, volumes, source_cg, source_vols)
        x._compress_snapshot_id.assert_called_once_with(UUID4)
        x._ensure_consistencygroup_policy.assert_called_once_with(
            SRC_GROUP_ID)

    @mock.patch('uuid.uuid4')
    def test_create_consistencygroup_from_consistencygroup_retry(self,
                                                                 m_uuid4):
        expected = (None, None)

        context = None
        group = GROUP.copy()
        volumes = [VOLUME.copy(), ]
        for x in volumes:
            x['consistencygroup_id'] = group['id']
        source_cg = SRC_GROUP.copy()
        source_vols = [SRC_VOL.copy(), ]
        for x in source_vols:
            x['consistencygroup_id'] = source_cg['id']
        oid = 'abc123-abc123abc123-abc123'
        snapshots = [
            {'volume_id': x['id'], 'display_name': None, 'id': UUID4}
            for x in source_vols]
        cgsnapshot = {'id': UUID4}

        response = {'success': True, 'msg': 'success'}
        retry_response = [
          {'success': False, 'msg': 'failed'},
          response,
        ]
        m_uuid4.return_value = UUID4
        self.driver._compress_snapshot_id = mock.Mock(
            return_value=UUID4_COMPRESSED)
        self.driver._ensure_consistencygroup_policy = mock.Mock(
            return_value=None)
        self.driver._wait_for_cgsnapshot = mock.Mock(return_value=None)
        self.driver._create_consistencygroup_from_cgsnapshot = mock.Mock(
            return_value=None)
        conf = {
            'snapshot.create_snapgroup_snapshot.return_value': response,
            'snapshot.snapgroup_snapshot_comment_to_object_id.return_value':
                oid,
            'snapshot.delete_snapgroup_snapshot.side_effect':
                retry_response,
        }
        self.driver.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        result = self.driver._create_consistencygroup_from_consistencygroup(
            context, group, volumes, source_cg, source_vols)

        x = self.driver
        v = self.driver.vmem_mg.snapshot
        self.assertEqual(expected, result)
        x._compress_snapshot_id.assert_called_once_with(UUID4)
        x._ensure_consistencygroup_policy.assert_called_once_with(
            SRC_GROUP_ID)
        v.create_snapgroup_snapshot.assert_called_once_with(
            name=SRC_GROUP_ID,
            comment=UUID4_COMPRESSED,
            priority=v7000_common.CONCERTO_DEFAULT_PRIORITY,
            enable_notification=False)
        x._wait_for_cgsnapshot.assert_called_once_with(
            SRC_GROUP_ID, UUID4_COMPRESSED, snapshots)
        x._create_consistencygroup_from_cgsnapshot.assert_called_once_with(
            context, group, volumes, cgsnapshot, snapshots)
        v.snapgroup_snapshot_comment_to_object_id.assert_called_once_with(
            SRC_GROUP_ID, UUID4_COMPRESSED)
        self.assertEqual(len(retry_response),
                         v.delete_snapgroup_snapshot.call_count)
