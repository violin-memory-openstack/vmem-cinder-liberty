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
Tests for Violin Memory 7000 Series All-Flash Array ISCSI Driver
"""

import mock

from cinder import exception
from cinder import test
from cinder.tests.unit import fake_vmem_client as vmemclient
from cinder.volume import configuration as conf
from cinder.volume.drivers.violin import v7000_common
from cinder.volume.drivers.violin import v7000_iscsi

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
    "ip": "1.2.3.4",
}
TARGET = "iqn.2004-02.com.vmem:%s" % VOLUME['id']

GET_VOLUME_STATS_RESPONSE = {
    'vendor_name': 'Violin Memory, Inc.',
    'reserved_percentage': 0,
    'QoS_support': False,
    'free_capacity_gb': 4094,
    'total_capacity_gb': 2558,
}

CLIENT_INFO = {
    'issanip_enabled': False,
    'sanclient_id': 7,
    'ISCSIDevices':
    [{'category': 'Virtual Device',
      'sizeMB': VOLUME['size'] * 1024,
      'name': VOLUME['id'],
      'object_id': 'v0000058',
      'access': 'ReadWrite',
      'ISCSITarget':
      {'name': TARGET,
       'startingLun': '0',
       'ipAddr': '192.168.91.1 192.168.92.1 192.168.93.1 192.168.94.1',
       'object_id': '2c68c1a4-67bb-59b3-93df-58bcdf422a66',
       'access': 'ReadWrite',
       'isInfiniBand': 'false',
       'iscsiurl': ''},
      'type': 'SAN',
      'lun': '8',
      'size': VOLUME['size'] * 1024 * 1024}],
    'name': 'lab-srv3377',
    'isiscsi_enabled': True,
    'clusterName': '',
    'ipAddress': '',
    'isclustered': False,
    'username': '',
    'isbmr_enabled': False,
    'useracl': None,
    'isfibrechannel_enabled': False,
    'iSCSIPolicy':
    {'initiators': ['iqn.1993-08.org.debian:01:1ebcd244a059'],
     'authentication':
     {'mutualCHAP':
      {'enabled': False,
       'user': ''},
      'enabled': False,
      'defaultUser': ''},
     'accessType': 'stationary'},
    'ISCSITargetList':
    [{'name': 'iqn.2004-02.com.vmem:lab-fsp-mga.openstack',
      'startingLun': '0',
      'ipAddr': '192.168.91.1 192.168.92.1 192.168.93.1 192.168.94.1',
      'object_id': '716cc60a-576a-55f1-bfe3-af4a21ca5554',
      'access': 'ReadWrite',
      'isInfiniBand': 'false',
      'iscsiurl': ''}],
    'type': 'Windows',
    'persistent_reservation': True,
    'isxboot_enabled': False}

GROUP_ID = "abcdabcd-1234-abcd-5678-abcdeffedcba"
GROUP = {'id': GROUP_ID}

CGSNAPSHOT_ID = "abcdabcd-8765-abcd-4321-abcdeffedcba"
CGSNAPSHOT = {
    'consistencygroup_id': GROUP_ID,
    'id': CGSNAPSHOT_ID,
}


class V7000ISCSIDriverTestCase(test.TestCase):
    """Test cases for VMEM ISCSI driver."""
    def setUp(self):
        super(V7000ISCSIDriverTestCase, self).setUp()
        self.conf = self.setup_configuration()
        self.driver = v7000_iscsi.V7000ISCSIDriver(configuration=self.conf)
        self.driver.gateway_iscsi_ip_addresses = [
            '192.168.91.1', '192.168.92.1', '192.168.93.1', '192.168.94.1']
        self.stats = {}
        self.driver.set_initialized()

    def tearDown(self):
        super(V7000ISCSIDriverTestCase, self).tearDown()

    def setup_configuration(self):
        config = mock.Mock(spec=conf.Configuration)
        config.volume_backend_name = 'v7000_iscsi'
        config.san_ip = '8.8.8.8'
        config.san_login = 'admin'
        config.san_password = ''
        config.san_thin_provision = False
        config.san_is_local = False
        config.use_igroups = False
        config.request_timeout = 300
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
        self.assertTrue(result is None)

    @mock.patch.object(v7000_common.V7000Common, 'check_for_setup_error')
    def test_check_for_setup_error_no_iscsi_config(self, m_setup_func):
        """No wwns were found during setup."""
        self.driver.gateway_iscsi_ip_addresses = []
        failure = exception.ViolinInvalidBackendConfig
        self.assertRaises(failure, self.driver.check_for_setup_error)
        m_setup_func.assert_called_once_with()

    def test_create_volume(self):
        """Volume created successfully."""
        self.driver.common._create_lun = mock.Mock()

        result = self.driver.create_volume(VOLUME)

        self.driver.common._create_lun.assert_called_with(VOLUME)
        self.assertTrue(result is None)

    def test_create_volume_from_snapshot(self):
        self.driver.common._create_volume_from_snapshot = mock.Mock()

        result = self.driver.create_volume_from_snapshot(VOLUME, SNAPSHOT)

        self.driver.common._create_volume_from_snapshot.assert_called_with(
            SNAPSHOT, VOLUME)

        self.assertTrue(result is None)

    def test_create_cloned_volume(self):
        self.driver.common._create_lun_from_lun = mock.Mock()

        result = self.driver.create_cloned_volume(VOLUME, SRC_VOL)

        self.driver.common._create_lun_from_lun.assert_called_with(
            SRC_VOL, VOLUME)
        self.assertTrue(result is None)

    def test_delete_volume(self):
        """Volume deleted successfully."""
        self.driver.common._delete_lun = mock.Mock()

        result = self.driver.delete_volume(VOLUME)

        self.driver.common._delete_lun.assert_called_with(VOLUME)
        self.assertTrue(result is None)

    def test_extend_volume(self):
        """Volume extended successfully."""
        new_size = 10
        self.driver.common._extend_lun = mock.Mock()

        result = self.driver.extend_volume(VOLUME, new_size)

        self.driver.common._extend_lun.assert_called_with(VOLUME, new_size)
        self.assertTrue(result is None)

    def test_create_snapshot(self):
        self.driver.common._create_lun_snapshot = mock.Mock()

        result = self.driver.create_snapshot(SNAPSHOT)
        self.driver.common._create_lun_snapshot.assert_called_with(SNAPSHOT)
        self.assertTrue(result is None)

    def test_delete_snapshot(self):
        self.driver.common._delete_lun_snapshot = mock.Mock()

        result = self.driver.delete_snapshot(SNAPSHOT)
        self.driver.common._delete_lun_snapshot.assert_called_with(SNAPSHOT)
        self.assertTrue(result is None)

    def test_get_volume_stats(self):
        self.driver._update_volume_stats = mock.Mock()
        self.driver._update_volume_stats()

        result = self.driver.get_volume_stats(True)

        self.driver._update_volume_stats.assert_called_with()
        self.assertEqual(self.driver.stats, result)

    def test_update_volume_stats(self):
        """Mock query to the backend collects stats on all physical devices."""
        backend_name = self.conf.volume_backend_name

        self.driver.common._get_volume_stats = mock.Mock(
            return_value=GET_VOLUME_STATS_RESPONSE,
        )

        result = self.driver._update_volume_stats()

        self.driver.common._get_volume_stats.assert_called_with(
            self.conf.san_ip)
        self.assertEqual(backend_name,
                         self.driver.stats['volume_backend_name'])
        self.assertEqual('iSCSI',
                         self.driver.stats['storage_protocol'])
        self.assertIsNone(result)

    def test_initialize_connection(self):
        lun_id = 1
        response = {'success': True, 'msg': 'None'}

        conf = {
            'client.create_client.return_value': response,
            'client.create_iscsi_target.return_value': response,
        }
        self.driver.common.vmem_mg = self.setup_mock_concerto(m_conf=conf)
        self.driver._get_iqn = mock.Mock(return_value=TARGET)
        self.driver._export_lun = mock.Mock(return_value=lun_id)

        props = self.driver.initialize_connection(VOLUME, CONNECTOR)

        self.driver._export_lun.assert_called_with(VOLUME, TARGET, CONNECTOR)
        self.assertEqual(props['driver_volume_type'], "iscsi")
        self.assertEqual(props['data']['target_discovered'], False)
        self.assertEqual(props['data']['target_iqn'], TARGET)
        self.assertEqual(props['data']['target_lun'], lun_id)
        self.assertEqual(props['data']['volume_id'], VOLUME['id'])
        self.assertEqual(props['data']['access_mode'], 'rw')

    def test_terminate_connection(self):
        self.driver.common.vmem_mg = self.setup_mock_concerto()
        self.driver._get_iqn = mock.Mock(return_value=TARGET)
        self.driver._unexport_lun = mock.Mock()

        result = self.driver.terminate_connection(VOLUME, CONNECTOR)

        self.driver._unexport_lun.assert_called_with(VOLUME, TARGET, CONNECTOR)
        self.assertEqual(result, None)

    def test_export_lun(self):
        lun_id = '1'
        response = {'success': True, 'msg': 'Assign device successfully'}

        self.driver.common.vmem_mg = self.setup_mock_concerto()

        self.driver.common._send_cmd_and_verify = mock.Mock(
            return_value=response)
        self.driver._get_lun_id = mock.Mock(return_value=lun_id)

        result = self.driver._export_lun(VOLUME, TARGET, CONNECTOR)

        self.driver.common._send_cmd_and_verify.assert_called_with(
            self.driver.common.vmem_mg.lun.assign_lun_to_iscsi_target,
            self.driver._is_lun_id_ready,
            'Assign device successfully',
            [VOLUME['id'], TARGET],
            [VOLUME['id'], CONNECTOR['host']])
        self.driver._get_lun_id.assert_called_with(
            VOLUME['id'], CONNECTOR['host'])
        self.assertEqual(lun_id, result)

    def test_export_lun_fails_with_exception(self):
        lun_id = '1'
        response = {'success': False, 'msg': 'Generic error'}

        self.driver.common.vmem_mg = self.setup_mock_concerto()
        self.driver.common._send_cmd_and_verify = mock.Mock(
            side_effect=exception.ViolinBackendErr(response['msg']))
        self.driver._get_lun_id = mock.Mock(return_value=lun_id)

        self.assertRaises(exception.ViolinBackendErr,
                          self.driver._export_lun,
                          VOLUME, TARGET, CONNECTOR)

    def test_unexport_lun(self):
        response = {'success': True, 'msg': 'Unassign device successfully'}

        self.driver.common.vmem_mg = self.setup_mock_concerto()
        self.driver.common._send_cmd = mock.Mock(
            return_value=response)

        result = self.driver._unexport_lun(VOLUME, TARGET, CONNECTOR)

        self.driver.common._send_cmd.assert_called_with(
            self.driver.common.vmem_mg.lun.unassign_lun_from_iscsi_target,
            "Unassign device successfully",
            VOLUME['id'], TARGET, True)
        self.assertTrue(result is None)

    def test_is_lun_id_ready(self):
        lun_id = '1'
        self.driver.common.vmem_mg = self.setup_mock_concerto()

        self.driver._get_lun_id = mock.Mock(return_value=lun_id)

        result = self.driver._is_lun_id_ready(
            VOLUME['id'], CONNECTOR['host'])
        self.assertTrue(result)

    def test_get_lun_id(self):

        conf = {
            'client.get_client_info.return_value': CLIENT_INFO,
        }
        self.driver.common.vmem_mg = self.setup_mock_concerto(m_conf=conf)

        result = self.driver._get_lun_id(VOLUME['id'], CONNECTOR['host'])

        self.assertEqual(8, result)

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
