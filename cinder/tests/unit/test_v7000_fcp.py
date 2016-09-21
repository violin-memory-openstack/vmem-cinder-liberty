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
Tests for Violin Memory 7000 Series All-Flash Array Fibrechannel Driver
"""

import mock

from cinder import exception
from cinder import test
from cinder.tests.unit import fake_vmem_client as vmemclient
from cinder.volume import configuration as conf
from cinder.volume.drivers.violin import v7000_common
from cinder.volume.drivers.violin import v7000_fcp

VOLUME_ID = "abcdabcd-1234-abcd-1234-abcdeffedcba"
VOLUME = {
    "name": "volume-" + VOLUME_ID,
    "id": VOLUME_ID,
    "display_name": "fake_volume",
    "size": 2,
    "host": "myhost",
    "volume_type": None,
    "volume_type_id": None,
}
SNAPSHOT_ID = "abcdabcd-1234-abcd-1234-abcdeffedcbb"
SNAPSHOT = {
    "name": "snapshot-" + SNAPSHOT_ID,
    "id": SNAPSHOT_ID,
    "volume_id": VOLUME_ID,
    "volume_name": "volume-" + VOLUME_ID,
    "volume_size": 2,
    "display_name": "fake_snapshot",
    "volume": VOLUME,
}
SRC_VOL_ID = "abcdabcd-1234-abcd-1234-abcdeffedcbc"
SRC_VOL = {
    "name": "volume-" + SRC_VOL_ID,
    "id": SRC_VOL_ID,
    "display_name": "fake_src_vol",
    "size": 2,
    "host": "myhost",
    "volume_type": None,
    "volume_type_id": None,
}
INITIATOR_IQN = "iqn.1111-22.org.debian:11:222"
CONNECTOR = {
    "initiator": INITIATOR_IQN,
    "host": "irrelevant",
    'wwpns': ['50014380186b3f65', '50014380186b3f67'],
}
FC_TARGET_WWPNS = [
    '31000024ff45fb22', '21000024ff45fb23',
    '51000024ff45f1be', '41000024ff45f1bf'
]
FC_INITIATOR_WWPNS = [
    '50014380186b3f65', '50014380186b3f67'
]
FC_FABRIC_MAP = {
    'fabricA':
    {'target_port_wwn_list': [FC_TARGET_WWPNS[0], FC_TARGET_WWPNS[1]],
     'initiator_port_wwn_list': [FC_INITIATOR_WWPNS[0]]},
    'fabricB':
    {'target_port_wwn_list': [FC_TARGET_WWPNS[2], FC_TARGET_WWPNS[3]],
     'initiator_port_wwn_list': [FC_INITIATOR_WWPNS[1]]}
}
FC_INITIATOR_TARGET_MAP = {
    FC_INITIATOR_WWPNS[0]: [FC_TARGET_WWPNS[0], FC_TARGET_WWPNS[1]],
    FC_INITIATOR_WWPNS[1]: [FC_TARGET_WWPNS[2], FC_TARGET_WWPNS[3]]
}

# The FC_INFO dict returned by the backend is keyed on
# object_id of the FC adapter and the values are the
# wwmns
FC_INFO = {
    '1a3cdb6a-383d-5ba6-a50b-4ba598074510': ['2100001b9745e25e'],
    '4a6bc10a-5547-5cc0-94f2-76222a8f8dff': ['2100001b9745e230'],
    'b21bfff5-d89e-51ff-9920-d990a061d722': ['2100001b9745e25f'],
    'b508cc6b-f78a-51f9-81cf-47c1aaf53dd1': ['2100001b9745e231']
}

CLIENT_INFO = {
    'FCPolicy':
    {'AS400enabled': False,
     'VSAenabled': False,
     'initiatorWWPNList': ['50-01-43-80-18-6b-3f-66',
                           '50-01-43-80-18-6b-3f-64']},
    'FibreChannelDevices':
    [{'access': 'ReadWrite',
      'id': 'v0000004',
      'initiatorWWPN': '*',
      'lun': '8',
      'name': 'abcdabcd-1234-abcd-1234-abcdeffedcba',
      'sizeMB': 10240,
      'targetWWPN': '*',
      'type': 'SAN'}]
}

CLIENT_INFO1 = {
    'FCPolicy':
    {'AS400enabled': False,
     'VSAenabled': False,
     'initiatorWWPNList': ['50-01-43-80-18-6b-3f-66',
                           '50-01-43-80-18-6b-3f-64']},
    'FibreChannelDevices': []
}

GROUP_ID = "abcdabcd-1234-abcd-5678-abcdeffedcba"
GROUP = {'id': GROUP_ID}

CGSNAPSHOT_ID = "abcdabcd-8765-abcd-4321-abcdeffedcba"
CGSNAPSHOT = {
    'consistencygroup_id': GROUP_ID,
    'id': CGSNAPSHOT_ID,
}


class V7000FCPDriverTestCase(test.TestCase):
    """Test cases for VMEM FCP driver."""
    def setUp(self):
        super(V7000FCPDriverTestCase, self).setUp()
        self.conf = self.setup_configuration()
        self.driver = v7000_fcp.V7000FCPDriver(configuration=self.conf)
        self.driver.common.container = 'myContainer'
        self.driver.device_id = 'ata-VIOLIN_MEMORY_ARRAY_23109R00000022'
        self.driver.gateway_fc_wwns = FC_TARGET_WWPNS
        self.stats = {}
        self.driver.set_initialized()

    def tearDown(self):
        super(V7000FCPDriverTestCase, self).tearDown()

    def setup_configuration(self):
        config = mock.Mock(spec=conf.Configuration)
        config.volume_backend_name = 'v7000_fcp'
        config.san_ip = '8.8.8.8'
        config.san_login = 'admin'
        config.san_password = ''
        config.san_thin_provision = False
        config.san_is_local = False
        config.request_timeout = 300
        config.container = 'myContainer'
        return config

    def setup_mock_concerto(self, m_conf=None):
        """Create a fake Concerto communication object."""
        _m_concerto = mock.Mock(name='Concerto',
                                version='1.1.1',
                                spec=vmemclient.mock_client_conf)

        if m_conf:
            _m_concerto.configure_mock(**m_conf)

        return _m_concerto

    @mock.patch.object(v7000_common.V7000Common, 'check_for_setup_error')
    def test_check_for_setup_error(self, m_setup_func):
        """No setup errors are found."""
        result = self.driver.check_for_setup_error()
        m_setup_func.assert_called_with()
        self.assertIsNone(result)

    @mock.patch.object(v7000_common.V7000Common, 'check_for_setup_error')
    def test_check_for_setup_error_no_wwn_config(self, m_setup_func):
        """No wwns were found during setup."""
        self.driver.gateway_fc_wwns = []
        failure = exception.ViolinInvalidBackendConfig
        self.assertRaises(failure, self.driver.check_for_setup_error)

    def test_create_volume(self):
        """Volume created successfully."""
        self.driver.common._create_lun = mock.Mock()

        result = self.driver.create_volume(VOLUME)

        self.driver.common._create_lun.assert_called_with(VOLUME)
        self.assertIsNone(result)

    def test_create_volume_from_snapshot(self):
        self.driver.common._create_volume_from_snapshot = mock.Mock()

        result = self.driver.create_volume_from_snapshot(VOLUME, SNAPSHOT)

        self.driver.common._create_volume_from_snapshot.assert_called_with(
            SNAPSHOT, VOLUME)

        self.assertIsNone(result)

    def test_create_cloned_volume(self):
        self.driver.common._create_lun_from_lun = mock.Mock()

        result = self.driver.create_cloned_volume(VOLUME, SRC_VOL)

        self.driver.common._create_lun_from_lun.assert_called_with(
            SRC_VOL, VOLUME)
        self.assertIsNone(result)

    def test_delete_volume(self):
        """Volume deleted successfully."""
        self.driver.common._delete_lun = mock.Mock()

        result = self.driver.delete_volume(VOLUME)

        self.driver.common._delete_lun.assert_called_with(VOLUME)
        self.assertIsNone(result)

    def test_extend_volume(self):
        """Volume extended successfully."""
        new_size = 10
        self.driver.common._extend_lun = mock.Mock()

        result = self.driver.extend_volume(VOLUME, new_size)

        self.driver.common._extend_lun.assert_called_with(VOLUME, new_size)
        self.assertIsNone(result)

    def test_create_snapshot(self):
        self.driver.common._create_lun_snapshot = mock.Mock()

        result = self.driver.create_snapshot(SNAPSHOT)
        self.driver.common._create_lun_snapshot.assert_called_with(SNAPSHOT)
        self.assertIsNone(result)

    def test_delete_snapshot(self):
        self.driver.common._delete_lun_snapshot = mock.Mock()

        result = self.driver.delete_snapshot(SNAPSHOT)
        self.driver.common._delete_lun_snapshot.assert_called_with(SNAPSHOT)
        self.assertIsNone(result)

    def test_get_volume_stats(self):
        self.driver._update_volume_stats = mock.Mock()
        self.driver._update_volume_stats()

        result = self.driver.get_volume_stats(True)

        self.driver._update_volume_stats.assert_called_with()
        self.assertEqual(self.driver.stats, result)

    def test_get_active_fc_targets(self):
        """Test Get Active FC Targets.

        Makes a mock query to the backend to collect all the physical
        adapters and extract the WWNs.
        """

        conf = {
            'adapter.get_fc_info.return_value': FC_INFO,
        }

        self.driver.common.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        result = self.driver._get_active_fc_targets()

        self.assertEqual(['2100001b9745e230', '2100001b9745e25f',
                          '2100001b9745e231', '2100001b9745e25e'],
                         result)

    def test_initialize_connection(self):
        lun_id = 1
        target_wwns = self.driver.gateway_fc_wwns
        init_targ_map = {}

        conf = {
            'client.create_client.return_value': None,
        }
        self.driver.common.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._export_lun = mock.Mock(return_value=lun_id)
        self.driver._build_initiator_target_map = mock.Mock(
            return_value=(target_wwns, init_targ_map))

        props = self.driver.initialize_connection(VOLUME, CONNECTOR)

        self.driver.common.vmem_mg.client.create_client.assert_called_with(
            name=CONNECTOR['host'], proto='FC', fc_wwns=CONNECTOR['wwpns'])
        self.driver._export_lun.assert_called_with(VOLUME, CONNECTOR)
        self.driver._build_initiator_target_map.assert_called_with(
            CONNECTOR)
        self.assertEqual("fibre_channel", props['driver_volume_type'])
        self.assertEqual(True, props['data']['target_discovered'])
        self.assertEqual(self.driver.gateway_fc_wwns,
                         props['data']['target_wwn'])
        self.assertEqual(lun_id, props['data']['target_lun'])

    def test_terminate_connection(self):
        target_wwns = self.driver.gateway_fc_wwns
        init_targ_map = {}

        self.driver.common.vmem_mg = self.setup_mock_concerto()
        self.driver._unexport_lun = mock.Mock()
        self.driver._is_initiator_connected_to_array = mock.Mock(
            return_value=False)
        self.driver._build_initiator_target_map = mock.Mock(
            return_value=(target_wwns, init_targ_map))

        props = self.driver.terminate_connection(VOLUME, CONNECTOR)

        self.driver._unexport_lun.assert_called_with(VOLUME, CONNECTOR)
        self.driver._is_initiator_connected_to_array.assert_called_with(
            CONNECTOR)
        self.driver._build_initiator_target_map.assert_called_with(
            CONNECTOR)
        self.assertEqual("fibre_channel", props['driver_volume_type'])
        self.assertEqual(target_wwns, props['data']['target_wwn'])
        self.assertEqual(init_targ_map, props['data']['initiator_target_map'])

    def test_export_lun(self):
        lun_id = '1'
        response = {'success': True, 'msg': 'Assign SAN client successfully'}

        conf = {
            'client.get_client_info.return_value': CLIENT_INFO,
        }
        self.driver.common.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        self.driver.common._send_cmd_and_verify = mock.Mock(
            return_value=response)

        self.driver._get_lun_id = mock.Mock(return_value=lun_id)

        result = self.driver._export_lun(VOLUME, CONNECTOR)

        self.driver.common._send_cmd_and_verify.assert_called_with(
            self.driver.common.vmem_mg.lun.assign_lun_to_client,
            self.driver._is_lun_id_ready,
            'Assign SAN client successfully',
            [VOLUME['id'], CONNECTOR['host'], "ReadWrite"],
            [VOLUME['id'], CONNECTOR['host']])
        self.driver._get_lun_id.assert_called_with(
            VOLUME['id'], CONNECTOR['host'])
        self.assertEqual(lun_id, result)

    def test_export_lun_fails_with_exception(self):
        lun_id = '1'
        response = {'status': False, 'msg': 'Generic error'}
        failure = exception.ViolinBackendErr

        self.driver.common.vmem_mg = self.setup_mock_concerto()
        self.driver.common._send_cmd_and_verify = mock.Mock(
            side_effect=exception.ViolinBackendErr(response['msg']))
        self.driver._get_lun_id = mock.Mock(return_value=lun_id)

        self.assertRaises(failure, self.driver._export_lun, VOLUME, CONNECTOR)

    def test_unexport_lun(self):
        response = {'success': True, 'msg': 'Unassign SAN client successfully'}

        self.driver.common.vmem_mg = self.setup_mock_concerto()
        self.driver.common._send_cmd = mock.Mock(
            return_value=response)

        result = self.driver._unexport_lun(VOLUME, CONNECTOR)

        self.driver.common._send_cmd.assert_called_with(
            self.driver.common.vmem_mg.lun.unassign_client_lun,
            "Unassign SAN client successfully",
            VOLUME['id'], CONNECTOR['host'], True)
        self.assertIsNone(result)

    def test_get_lun_id(self):

        conf = {
            'client.get_client_info.return_value': CLIENT_INFO,
        }
        self.driver.common.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        result = self.driver._get_lun_id(VOLUME['id'], CONNECTOR['host'])

        self.assertEqual(8, result)

    def test_is_lun_id_ready(self):
        lun_id = '1'
        self.driver.common.vmem_mg = self.setup_mock_concerto()

        self.driver._get_lun_id = mock.Mock(return_value=lun_id)

        result = self.driver._is_lun_id_ready(
            VOLUME['id'], CONNECTOR['host'])
        self.assertTrue(result)

    def test_build_initiator_target_map(self):
        """Successfully build a map when zoning is enabled."""
        expected_targ_wwns = FC_TARGET_WWPNS

        self.driver.lookup_service = mock.Mock()
        (self.driver.lookup_service.get_device_mapping_from_network.
         return_value) = FC_FABRIC_MAP

        result = self.driver._build_initiator_target_map(CONNECTOR)
        (targ_wwns, init_targ_map) = result

        (self.driver.lookup_service.get_device_mapping_from_network.
         assert_called_with(CONNECTOR['wwpns'], self.driver.gateway_fc_wwns))
        self.assertEqual(set(expected_targ_wwns), set(targ_wwns))

        i = FC_INITIATOR_WWPNS[0]
        self.assertIn(FC_TARGET_WWPNS[0], init_targ_map[i])
        self.assertIn(FC_TARGET_WWPNS[1], init_targ_map[i])
        self.assertEqual(2, len(init_targ_map[i]))

        i = FC_INITIATOR_WWPNS[1]
        self.assertIn(FC_TARGET_WWPNS[2], init_targ_map[i])
        self.assertIn(FC_TARGET_WWPNS[3], init_targ_map[i])
        self.assertEqual(2, len(init_targ_map[i]))

        self.assertEqual(2, len(init_targ_map))

    def test_build_initiator_target_map_no_lookup_service(self):
        """Successfully build a map when zoning is disabled."""
        expected_targ_wwns = FC_TARGET_WWPNS
        expected_init_targ_map = {
            CONNECTOR['wwpns'][0]: FC_TARGET_WWPNS,
            CONNECTOR['wwpns'][1]: FC_TARGET_WWPNS
        }
        self.driver.lookup_service = None

        targ_wwns, init_targ_map = self.driver._build_initiator_target_map(
            CONNECTOR)

        self.assertEqual(expected_targ_wwns, targ_wwns)
        self.assertEqual(expected_init_targ_map, init_targ_map)

    def test_is_initiator_connected_to_array(self):
        """Successfully finds an initiator with remaining active session."""
        conf = {
            'client.get_client_info.return_value': CLIENT_INFO,
        }
        self.driver.common.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        self.assertTrue(self.driver._is_initiator_connected_to_array(
            CONNECTOR))
        self.driver.common.vmem_mg.client.get_client_info.assert_called_with(
            CONNECTOR['host'])

    def test_is_initiator_connected_to_array_empty_response(self):
        """Successfully finds no initiators with remaining active sessions."""
        conf = {
            'client.get_client_info.return_value': CLIENT_INFO1
        }
        self.driver.common.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        self.assertFalse(self.driver._is_initiator_connected_to_array(
            CONNECTOR))

    def test_create_consistencygroup(self):
        self.driver.common._create_consistencygroup = mock.Mock(
            return_value=None)
        context = None

        result = self.driver.create_consistencygroup(
            context, GROUP)

        self.driver.common._create_consistencygroup.assert_called_once_with(
            context, GROUP)
        self.assertIsNone(result)

    def test_delete_consistencygroup(self):
        expected = (
            {'status': 'deleted'},
            [])
        self.driver.common._delete_consistencygroup = mock.Mock(
            return_value=expected)
        context = None

        result = self.driver.delete_consistencygroup(
            context, GROUP)

        self.driver.common._delete_consistencygroup.assert_called_once_with(
            context, GROUP, self.driver.db)
        self.assertEqual(expected, result)

    def test_update_consistencygroup(self):
        expected = (None, None, None)
        self.driver.common._update_consistencygroup = mock.Mock(
            return_value=expected)
        context = None
        add_volumes = ['volume-to-add', ]
        remove_volumes = ['volume-to-remove', ]

        result = self.driver.update_consistencygroup(
            context, GROUP, add_volumes, remove_volumes)

        self.driver.common._update_consistencygroup.assert_called_once_with(
            context, GROUP, add_volumes, remove_volumes)
        self.assertEqual(expected, result)

    def test_create_cgsnapshot(self):
        expected = (
            {'status': 'available'},
            [])
        self.driver.common._create_cgsnapshot = mock.Mock(
            return_value=expected)
        context = None

        result = self.driver.create_cgsnapshot(context, CGSNAPSHOT)

        self.driver.common._create_cgsnapshot.assert_called_once_with(
            context, CGSNAPSHOT, self.driver.db)
        self.assertEqual(expected, result)

    def test_delete_cgsnapshot(self):
        expected = (
            {'status': 'deleted'},
            [])
        self.driver.common._delete_cgsnapshot = mock.Mock(
            return_value=expected)
        context = None

        result = self.driver.delete_cgsnapshot(context, CGSNAPSHOT)

        self.driver.common._delete_cgsnapshot.assert_called_once_with(
            context, CGSNAPSHOT, self.driver.db)
        self.assertEqual(expected, result)

    def test_create_consistencygroup_from_src(self):
        expected = (None, None)

        self.driver.common._create_consistencygroup_from_src = mock.Mock(
            return_value=expected)
        context = None
        group = GROUP.copy()
        volumes = [VOLUME.copy(), ]
        cgsnapshot = CGSNAPSHOT.copy()
        snapshots = [SNAPSHOT.copy(), ]
        source_cg = None
        source_vols = None

        result = self.driver.create_consistencygroup_from_src(
            context, group, volumes, cgsnapshot,
            snapshots, source_cg, source_vols)

        v = self.driver.common
        v._create_consistencygroup_from_src.assert_called_once_with(
            context, group, volumes, cgsnapshot,
            snapshots, source_cg, source_vols)
        self.assertEqual(expected, result)
