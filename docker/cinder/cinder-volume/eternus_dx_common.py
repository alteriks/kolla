# Copyright (c) 2016 FUJITSU LIMITED
# Copyright (c) 2012 EMC Corporation.
# Copyright (c) 2012 OpenStack Foundation
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
#

"""
Cinder Volume driver for Fujitsu ETERNUS DX S3 series.
"""

import base64
import hashlib
import six
import time
from xml.etree.ElementTree import parse

from cinder import db
from cinder import exception
from cinder import utils
from cinder.i18n import _, _LE, _LI, _LW
from cinder.volume.configuration import Configuration
from cinder.volume.drivers.fujitsu import eternus_dx_utils as dx_utils
from cinder.volume import utils as volume_utils
from oslo_config import cfg
from oslo_log import log as logging
from oslo_service import loopingcall
from oslo_utils import units

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

try:
    import pywbem
except ImportError:
    msg = _LE('import pywbem failed!! '
              'pywbem is necessary for this volume driver.')
    LOG.error(msg)

VOL_PREFIX = "FJosv_"
RAIDGROUP = 2
TPPOOL = 5
SNAPOPC = 4
OPC = 5
RETURN_TO_RESOURCEPOOL = 19
DETACH = 8
INITIALIZED = 2
UNSYNCHRONIZED = 3
BROKEN = 5
PREPARED = 11
REPL = "FUJITSU_ReplicationService"
STOR_CONF = "FUJITSU_StorageConfigurationService"
CTRL_CONF = "FUJITSU_ControllerConfigurationService"
STOR_HWID = "FUJITSU_StorageHardwareIDManagementService"
FJ_VOL_REPL_KEY = "type:replication_enabled"
FJ_VOL_REPL_VALUE = "<is> True"
TYPE_OLU = 0
TYPE_TPPC = 6

UNDEF_MSG = 'Undefined Error!!'
JOB_RETRIES = 60
JOB_INTERVAL_SEC = 10

# Error code keyword.
VOLUMENAME_IN_USE = 32788
COPYSESSION_NOT_EXIST = 32793
LUNAME_IN_USE = 4102
LUNAME_NOT_EXIST = 4097  # Only for InvokeMethod(HidePaths).
EC_REC = 3
FJ_ETERNUS_DX_OPT_opts = [
    cfg.StrOpt('cinder_eternus_config_file',
               default='/etc/cinder/cinder_fujitsu_eternus_dx.xml',
               help='config file for cinder eternus_dx volume driver'),
]

POOL_TYPE_dic = {
    RAIDGROUP: 'RAID_GROUP',
    TPPOOL: 'Thinporvisioning_POOL',
}

OPERATION_dic = {
    SNAPOPC: RETURN_TO_RESOURCEPOOL,
    OPC: DETACH,
    EC_REC: DETACH,
}

RETCODE_dic = {
    '0': 'Success',
    '1': 'Method Not Supported',
    '4': 'Failed',
    '5': 'Invalid Parameter',
    '4096': 'Method Parameters Checked - Job Started',
    '4097': 'Size Not Supported',
    '4101': 'Target/initiator combination already exposed',
    '4102': 'Requested logical unit number in use',
    '32769': 'Maximum number of Logical Volume in a RAID group '
             'has been reached',
    '32770': 'Maximum number of Logical Volume in the storage device '
             'has been reached',
    '32771': 'Maximum number of registered Host WWN '
             'has been reached',
    '32772': 'Maximum number of affinity group has been reached',
    '32773': 'Maximum number of host affinity has been reached',
    '32781': 'Not available under current system configuration',
    '32782': 'Controller firmware update in process',
    '32785': 'The RAID group is in busy state',
    '32786': 'The Logical Volume is in busy state',
    '32787': 'The device is in busy state',
    '32788': 'Element Name is in use',
    '32791': 'Maximum number of copy session has been reached',
    '32792': 'No Copy License',
    '32793': 'Session does not exist',
    '32794': 'Phase is not correct',
    '32796': 'Quick Format Error',
    '32801': 'The CA port is in invalid setting',
    '32802': 'The Logical Volume is Mainframe volume',
    '32803': 'The RAID group is not operative',
    '32804': 'The Logical Volume is not operative',
    '32805': 'The Logical Element is Thin provisioning Pool Volume',
    '32806': 'The Logical Volume is pool for copy volume',
    '32807': 'The Logical Volume is unknown volume',
    '32808': 'No Thin Provisioning License',
    '32809': 'The Logical Element is ODX volume',
    '32810': 'The specified volume is under use as NAS volume',
    '32811': 'This operation cannot be performed to the NAS resources',
    '32813': 'This operation cannot be performed to the '
             'VVOL resources',
    '32816': 'Generic fatal error',
    '32817': 'Inconsistent State with 1Step Restore '
             'operation',
    '32818': 'REC Path failure during copy',
    '32819': 'RAID failure during EC/REC copy',
    '32820': 'Previous command in process',
    '32821': 'Cascade local copy session exist',
    '32822': 'Cascade EC/REC session is not suspended',
    '35302': 'Invalid LogicalElement',
    '35304': 'LogicalElement state error',
    '35316': 'Multi-hop error',
    '35318': 'Maximum number of multi-hop has been reached',
    '35324': 'RAID is broken',
    '35331': 'Maximum number of session has been reached(per device)',
    '35333': 'Maximum number of session has been reached(per SourceElement)',
    '35334': 'Maximum number of session has been reached(per TargetElement)',
    '35335': 'Maximum number of Snapshot generation has been reached '
             '(per SourceElement)',
    '35346': 'Copy table size is not setup',
    '35347': 'Copy table size is not enough',
}

CONF.register_opts(FJ_ETERNUS_DX_OPT_opts)


class FJDXCommon(object):
    """Common code that does not depend on protocol."""

    VERSION = "1.4.0"
    stats = {
        'driver_version': VERSION,
        'free_capacity_gb': 0,
        'reserved_percentage': 0,
        'storage_protocol': None,
        'total_capacity_gb': 0,
        'vendor_name': 'FUJITSU',
        'QoS_support': False,
        'volume_backend_name': None,
    }

    def __init__(self, prtcl, configuration=None):
        self.protocol = prtcl
        self.configuration = configuration
        self.configuration.append_config_values(FJ_ETERNUS_DX_OPT_opts)
        (self.remote_host, remote_backend_name, self.remote_eternus_config) = (
            self._prepare_replica())
        self.storage_name = self._get_drvcfg('EternusIP')
        self.scheduler = dx_utils.FJDXPoolManager(self.configuration,
                                                  remote_backend_name)
        self.conn = None
        self.remote_conn = None

        if prtcl == 'iSCSI':
            # Get iSCSI ipaddress from driver configuration file.
            self.configuration.iscsi_ip_address = (
                self._get_drvcfg('EternusISCSIIP'))

    def create_volume(self, volume):
        """Create volume on ETERNUS."""
        LOG.debug('create_volume, '
                  'volume id: %(vid)s, volume size: %(vsize)s.',
                  {'vid': volume['id'], 'vsize': volume['size']})

        self.conn = self._get_eternus_connection()

        d_metadata = dx_utils.get_metadata(volume)

        (element_path, metadata) = self._create_volume(volume)

        d_metadata.update(metadata)

        model_update = {
            'provider_location': six.text_type(element_path),
            'metadata': d_metadata,
        }

        repl_value = dx_utils.get_extra_specs(volume, FJ_VOL_REPL_KEY)

        if repl_value == FJ_VOL_REPL_VALUE:
            repl_data = None
            repl_status = 'error'
            try:
                # Do replication.
                src_host = volume_utils.extract_host(volume['host'], 'host')
                if self.remote_host != src_host:
                    LOG.warning(_LW('Do replication, '
                                    'remote host:%(remote)s '
                                    'and local host:%(local)s '
                                    'must be the same one.'),
                                {'remote': self.remote_host,
                                 'local': src_host})
                    model_update['replication_status'] = repl_status
                    model_update['replication_driver_data'] = (
                        six.text_type(repl_data))
                    model_update['metadata']['Replication'] = repl_status
                    return model_update

                self.remote_conn = self._get_eternus_connection(
                    self.remote_eternus_config)

                # Create remote volume then do REC copy.
                (repl_data, repl_status, error_msg) = (
                    self._create_replica_volume(volume, d_metadata))

                if repl_status == 'error':
                    # Create replica failed.
                    LOG.warning(_LW('%s'), error_msg)

                model_update['replication_status'] = repl_status
                model_update['replication_driver_data'] = (
                    six.text_type(repl_data))
                if repl_data:
                    # Add remote volume information to metadata.
                    model_update['metadata'].update(repl_data)
                # Add the result of replication to metadata.
                model_update['metadata']['Replication'] = repl_status

            except Exception as ex:
                model_update['replication_status'] = repl_status
                model_update['replication_driver_data'] = (
                    six.text_type(repl_data))

                if repl_data:
                    model_update['metadata'].update(repl_data)
                model_update['metadata']['Replication'] = repl_status

                LOG.warning(_LW('create_volume, '
                                'create remote volume'
                                'undefined error was occured (%s).'), ex)

        return model_update

    def _create_volume(self, volume, remote=False):
        LOG.debug('_create_volume, '
                  'volume id: %(vid)s, volume size: %(vsize)s, ',
                  'remote: %(remote)s.',
                  {'vid': volume['id'], 'vsize': volume['size'],
                   'remote': remote})

        if remote:
            conn = self.remote_conn
        else:
            conn = self.conn

        volumesize = int(volume['size']) * units.Gi
        volumename = self._get_volume_name(volume, use_id=True)

        LOG.debug('_create_volume, volumename: %(volumename)s, '
                  'volumesize: %(volumesize)u.',
                  {'volumename': volumename,
                   'volumesize': volumesize})

        # Get pool instance from PoolManager.
        pool_instance = self._get_pool_from_scheduler(
            volume['size'], use_tpp=True, remote=remote)
        eternus_pool = pool_instance['name']
        pool = pool_instance['path']

        if 'RSP' in pool['InstanceID']:
            pooltype = RAIDGROUP
        else:
            pooltype = TPPOOL

        configservice = self._find_eternus_service(STOR_CONF, conn=conn)
        if not configservice:
            msg = (_('_create_volume, volume: %(volume)s, '
                     'volumename: %(volumename)s, '
                     'eternus_pool: %(eternus_pool)s, '
                     'Storage Configuration Service not found.')
                   % {'volume': volume,
                      'volumename': volumename,
                      'eternus_pool': eternus_pool})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        # Get eternus model name.
        try:
            systemnamelist = self._enum_eternus_instances(
                'FUJITSU_StorageProduct', conn=conn)
        except Exception:
            msg = (_('_create_volume, '
                     'volume: %(volume)s, '
                     'EnumerateInstances, '
                     'cannot connect to ETERNUS.')
                   % {'volume': volume})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_create_volume, '
                  'volumename: %(volumename)s, '
                  'Backend: %(backend)s, '
                  'Pool Name: %(eternus_pool)s, '
                  'Pool Type: %(pooltype)s.',
                  {'volumename': volumename,
                   'backend': systemnamelist[0]['IdentifyingNumber'],
                   'eternus_pool': eternus_pool,
                   'pooltype': POOL_TYPE_dic[pooltype]})

        LOG.debug('_create_volume, '
                  'CreateOrModifyElementFromStoragePool, '
                  'ConfigService: %(service)s, '
                  'ElementName: %(volumename)s, '
                  'InPool: %(eternus_pool)s, '
                  'ElementType: %(pooltype)u, '
                  'Size: %(volumesize)u.',
                  {'service': configservice,
                   'volumename': volumename,
                   'eternus_pool': eternus_pool,
                   'pooltype': pooltype,
                   'volumesize': volumesize})
        # Invoke method for create volume.
        rc, errordesc, job = self._exec_eternus_service(
            'CreateOrModifyElementFromStoragePool',
            configservice,
            conn=conn,
            ElementName=volumename,
            InPool=pool,
            ElementType=self._pywbem_uint(pooltype, '16'),
            Size=self._pywbem_uint(volumesize, '64'))

        if rc == VOLUMENAME_IN_USE:  # Element Name is in use.
            if not remote:
                LOG.warning(_LW('_create_volume, '
                                'volumename: %(volumename)s, '
                                'Element Name is in use.'),
                            {'volumename': volumename})
                vol_instance = self._find_lun(volume)
            else:
                LOG.warning(_LW('_create_volume, '
                                'volumename: %(volumename)s, '
                                'Remote Element Name is in use.'),
                            {'volumename': volumename})
                vol_name = {'source-name': volumename}
                vol_instance = self._find_lun_with_listup(
                    conn=conn, **vol_name)
            element = vol_instance
        elif rc != 0:
            msg = (_('_create_volume, '
                     'volumename: %(volumename)s, '
                     'poolname: %(eternus_pool)s, '
                     'Return code: %(rc)lu, '
                     'Error: %(errordesc)s.')
                   % {'volumename': volumename,
                      'eternus_pool': eternus_pool,
                      'rc': rc,
                      'errordesc': errordesc})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        else:
            element = job['TheElement']

        # Create return value.
        element_path = {
            'classname': element.classname,
            'keybindings': {
                'SystemName': element['SystemName'],
                'DeviceID': element['DeviceID'],
            },
            'vol_name': volumename,
        }

        volume_no = "0x" + element['DeviceID'][24:28]

        metadata = {
            'FJ_Backend': systemnamelist[0]['IdentifyingNumber'],
            'FJ_Volume_Name': volumename,
            'FJ_Volume_No': volume_no,
            'FJ_Pool_Name': eternus_pool,
            'FJ_Pool_Type': POOL_TYPE_dic[pooltype],
        }

        if not remote:
            return (element_path, metadata)
        else:
            repl_data = {
                'Remote_Backend': systemnamelist[0]['IdentifyingNumber'],
                'Remote_Volume_No': volume_no,
                'Remote_Pool_Name': eternus_pool,
                'Remote_Device_ID': element['DeviceID'],
            }
            return (element_path, metadata, repl_data, 'enabled')

    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates a volume from a snapshot."""
        LOG.debug('create_volume_from_snapshot, '
                  'volume id: %(vid)s, volume size: %(vsize)s, '
                  'snapshot id: %(sid)s.',
                  {'vid': volume['id'], 'vsize': volume['size'],
                   'sid': snapshot['id']})

        self.conn = self._get_eternus_connection()
        source_volume_instance = self._find_lun(snapshot)

        # Check the existence of source volume.
        if not source_volume_instance:
            msg = _('create_volume_from_snapshot, '
                    'Source Volume does not exist in ETERNUS.')
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        # Create volume for the target volume.
        model_update = self.create_volume(volume)
        element_path = eval(model_update.get('provider_location'))
        target_volume_instancename = self._create_eternus_instance_name(
            element_path['classname'], element_path['keybindings'])

        try:
            target_volume_instance = (
                self._get_eternus_instance(target_volume_instancename))
        except Exception:
            msg = (_('create_volume_from_snapshot, '
                     'target volume instancename: %(volume_instancename)s, '
                     'Get Instance Failed.')
                   % {'volume_instancename': target_volume_instancename})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        self._create_local_cloned_volume(target_volume_instance,
                                         source_volume_instance)

        return model_update

    def create_cloned_volume(self, volume, src_vref):
        """Create clone of the specified volume."""
        LOG.debug('create_cloned_volume, '
                  'tgt: (%(tid)s, %(tsize)s), src: (%(sid)s, %(ssize)s).',
                  {'tid': volume['id'], 'tsize': volume['size'],
                   'sid': src_vref['id'], 'ssize': src_vref['size']})

        self.conn = self._get_eternus_connection()
        source_volume_instance = self._find_lun(src_vref)

        if not source_volume_instance:
            msg = _('create_cloned_volume, '
                    'Source Volume does not exist in ETERNUS.')
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        model_update = self.create_volume(volume)
        element_path = eval(model_update.get('provider_location'))
        target_volume_instancename = self._create_eternus_instance_name(
            element_path['classname'], element_path['keybindings'])

        try:
            target_volume_instance = (
                self._get_eternus_instance(target_volume_instancename))
        except Exception:
            msg = (_('create_cloned_volume, '
                     'target volume instancename: %(volume_instancename)s, '
                     'Get Instance Failed.')
                   % {'volume_instancename': target_volume_instancename})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        self._create_local_cloned_volume(target_volume_instance,
                                         source_volume_instance)

        return model_update

    @dx_utils.FJDXLockutils('vol', 'cinder-', True)
    def _create_local_cloned_volume(self, tgt_vol_instance, src_vol_instance):
        """Create local clone of the specified volume."""
        s_volumename = src_vol_instance['ElementName']
        t_volumename = tgt_vol_instance['ElementName']

        LOG.debug('_create_local_cloned_volume, '
                  'tgt volume name: %(t_volumename)s, '
                  'src volume name: %(s_volumename)s, ',
                  {'t_volumename': t_volumename,
                   's_volumename': s_volumename})

        # Get replicationservice for CreateElementReplica.
        repservice = self._find_eternus_service(REPL)

        if not repservice:
            msg = _('_create_local_cloned_volume, '
                    'Replication Service not found.')
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        # Invoke method for create cloned volume from volume.
        rc, errordesc, job = self._exec_eternus_service(
            'CreateElementReplica',
            repservice,
            SyncType=self._pywbem_uint(8, '16'),
            SourceElement=src_vol_instance.path,
            TargetElement=tgt_vol_instance.path)

        if rc != 0:
            msg = (_('_create_local_cloned_volume, '
                     'volumename: %(volumename)s, '
                     'sourcevolumename: %(sourcevolumename)s, '
                     'source volume instance: %(source_volume)s, '
                     'target volume instance: %(target_volume)s, '
                     'Return code: %(rc)lu, '
                     'Error: %(errordesc)s.')
                   % {'volumename': t_volumename,
                      'sourcevolumename': s_volumename,
                      'source_volume': src_vol_instance.path,
                      'target_volume': tgt_vol_instance.path,
                      'rc': rc,
                      'errordesc': errordesc})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_create_local_cloned_volume, out: %(rc)s, %(job)s.',
                  {'rc': rc, 'job': job})

    def delete_volume(self, volume):
        """Delete volume on ETERNUS."""
        LOG.debug('delete_volume, volume id: %s.', volume['id'])

        self.conn = self._get_eternus_connection()
        repl_status = volume.get('replication_status', None)
        if repl_status and repl_status != 'disabled':
            remote_vol = None
            try:
                remote_vol = self._find_replica_lun(volume)
            except Exception:
                LOG.warning(_LW('delete_volume, '
                                'remote volume does not found.'))

            if remote_vol:
                if repl_status == 'enabled':
                    metadata = dx_utils.get_metadata(volume)
                    self._exec_ccm_script(
                        "stop", source=metadata, source_volume=volume)
                src_host = volume_utils.extract_host(volume['host'], 'host')
                if self.remote_host != src_host:
                    msg = (_('Do replication, '
                             'remote host:%(remote)s '
                             'and local host:%(local)s '
                             'must be the same one.')
                           % {'remote': self.remote_host,
                              'local': src_host})
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)

                self.remote_conn = self._get_eternus_connection(
                    self.remote_eternus_config)

                repl_vol_exist = self._delete_volume_setting(volume,
                                                             remote=True)

                if not repl_vol_exist:
                    LOG.debug('delete_repl_volume, '
                              'volume not found in 1st check.')
                else:
                    self._delete_volume(volume, self.remote_conn, remote=True)

        vol_exist = self._delete_volume_setting(volume)

        if not vol_exist:
            LOG.debug('delete_volume, volume not found in 1st check.')
            return False

        self._delete_volume(volume)
        return True

    @dx_utils.FJDXLockutils('vol', 'cinder-', True)
    def _delete_volume_setting(self, volume, remote=False):
        """Delete volume setting (HostAffinity, CopySession) on ETERNUS."""
        LOG.debug('_delete_volume_setting, '
                  'volume id: %(vid)s, '
                  'remote: %(remote)s.',
                  {'vid': volume['id'], 'remote': remote})

        # Check the existence of volume.
        volumename = self._get_volume_name(volume)
        if not remote:
            vol_instance = self._find_lun(volume)
            conn = self.conn
        else:
            vol_instance = self._find_replica_lun(volume)
            conn = self.remote_conn

        if not vol_instance:
            LOG.info(_LI('_delete_volume_setting, volumename:%(volumename)s, '
                         'volume not found on ETERNUS.'),
                     {'volumename': volumename})
            return False

        # Delete host-affinity setting remained by unexpected error.
        self._unmap_lun(volume, None, force=True, remote=remote)

        # Check copy session relating to target volume.
        #cpsessionlist = self._find_copysession(vol_instance, conn=conn)
        cpsessionlist = []
        delete_copysession_list = []
        wait_copysession_list = []

        for cpsession in cpsessionlist:
            LOG.debug('_delete_volume_setting, '
                      'volumename: %(volumename)s, '
                      'cpsession: %(cpsession)s.',
                      {'volumename': volumename,
                       'cpsession': cpsession})

            if cpsession['SyncedElement'] == vol_instance.path:
                # Copy target : other_volume --(copy)--> vol_instance
                delete_copysession_list.append(cpsession)
            elif cpsession['SystemElement'] == vol_instance.path:
                # Copy source : vol_instance --(copy)--> other volume
                wait_copysession_list.append(cpsession)

        LOG.debug('_delete_volume_setting, '
                  'wait_cpsession: %(wait_cpsession)s, '
                  'delete_cpsession: %(delete_cpsession)s.',
                  {'wait_cpsession': wait_copysession_list,
                   'delete_cpsession': delete_copysession_list})

        for cpsession in wait_copysession_list:
            complete = self._wait_for_copy_complete(cpsession, conn=conn)
            if not complete:
                # Cpsession is EC/REC and it is needed to be deleted.
                delete_copysession_list.append(cpsession)

        for cpsession in delete_copysession_list:
            self._delete_copysession(cpsession, conn=conn)

        LOG.debug('_delete_volume_setting, '
                  'wait_cpsession: %(wait_cpsession)s, '
                  'delete_cpsession: %(delete_cpsession)s, complete.',
                  {'wait_cpsession': wait_copysession_list,
                   'delete_cpsession': delete_copysession_list})
        return True

    @dx_utils.FJDXLockutils('vol', 'cinder-', True)
    def _delete_volume(self, volume, conn=None, remote=False):
        """Delete volume on ETERNUS."""
        LOG.debug('_delete_volume, volume id: %s.',
                  volume['id'])

        if remote:
            vol_instance = self._find_replica_lun(volume)
        else:
            vol_instance = self._find_lun(volume)

        if not vol_instance:
            LOG.debug('_delete_volume, volume not found in 2nd check, '
                      'but no problem.')
            return

        volumename = vol_instance['ElementName']

        configservice = self._find_eternus_service(STOR_CONF, conn=conn)
        if not configservice:
            msg = (_('_delete_volume, volumename: %(volumename)s, '
                     'Storage Configuration Service not found.')
                   % {'volumename': volumename})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_delete_volume, volumename: %(volumename)s, '
                  'vol_instance: %(vol_instance)s, '
                  'Method: ReturnToStoragePool.',
                  {'volumename': volumename,
                   'vol_instance': vol_instance.path})

        # Invoke method for delete volume.
        rc, errordesc, job = self._exec_eternus_service(
            'ReturnToStoragePool',
            configservice,
            conn=conn,
            TheElement=vol_instance.path)

        if rc != 0:
            msg = (_('_delete_volume, volumename: %(volumename)s, '
                     'Return code: %(rc)lu, '
                     'Error: %(errordesc)s.')
                   % {'volumename': volumename,
                      'rc': rc,
                      'errordesc': errordesc})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_delete_volume, volumename: %(volumename)s, '
                  'Return code: %(rc)lu, '
                  'Error: %(errordesc)s.',
                  {'volumename': volumename,
                   'rc': rc,
                   'errordesc': errordesc})

    @dx_utils.FJDXLockutils('vol', 'cinder-', True)
    def create_snapshot(self, snapshot):
        """Create snapshot using SnapOPC."""
        LOG.debug('create_snapshot, '
                  'snapshot id: %(sid)s, volume id: %(vid)s.',
                  {'sid': snapshot['id'], 'vid': snapshot['volume_id']})

        self.conn = self._get_eternus_connection()
        snapshotname = snapshot['name']
        volumename = snapshot['volume_name']
        volume = snapshot['volume']
        d_volumename = self._get_volume_name(snapshot, use_id=True)
        s_volumename = self._get_volume_name(volume)
        vol_instance = self._find_lun(volume)
        repservice = self._find_eternus_service(REPL)

        # Check the existence of volume.
        if not vol_instance:
            # Volume not found on ETERNUS.
            msg = (_('create_snapshot, '
                     'volumename: %(s_volumename)s, '
                     'source volume not found on ETERNUS.')
                   % {'s_volumename': s_volumename})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        if not repservice:
            msg = (_('create_snapshot, '
                     'volumename: %(volumename)s, '
                     'Replication Service not found.')
                   % {'volumename': volumename})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        # Get poolname from driver configuration file.
        eternus_pool = self._get_snap_pool_name(snapshot)
        pool = self._find_pool(eternus_pool)
        if not pool:
            msg = (_('create_snapshot, '
                     'eternus_pool: %(eternus_pool)s, '
                     'pool not found.')
                   % {'eternus_pool': eternus_pool})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('create_snapshot, '
                  'snapshotname: %(snapshotname)s, '
                  'source volume name: %(volumename)s, '
                  'vol_instance.path: %(vol_instance)s, '
                  'dest_volumename: %(d_volumename)s, '
                  'pool: %(pool)s, '
                  'Invoke CreateElementReplica.',
                  {'snapshotname': snapshotname,
                   'volumename': volumename,
                   'vol_instance': vol_instance.path,
                   'd_volumename': d_volumename,
                   'pool': pool})

        # Invoke method for create snapshot
        rc, errordesc, job = self._exec_eternus_service(
            'CreateElementReplica',
            repservice,
            ElementName=d_volumename,
            TargetPool=pool,
            SyncType=self._pywbem_uint(7, '16'),
            SourceElement=vol_instance.path)

        if rc != 0:
            msg = (_('create_snapshot, '
                     'snapshotname: %(snapshotname)s, '
                     'source volume name: %(volumename)s, '
                     'vol_instance.path: %(vol_instance)s, '
                     'dest volume name: %(d_volumename)s, '
                     'pool: %(pool)s, '
                     'Return code: %(rc)lu, '
                     'Error: %(errordesc)s.')
                   % {'snapshotname': snapshotname,
                      'volumename': volumename,
                      'vol_instance': vol_instance.path,
                      'd_volumename': d_volumename,
                      'pool': pool,
                      'rc': rc,
                      'errordesc': errordesc})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        else:
            element = job['TargetElement']

        LOG.debug('create_snapshot, '
                  'volumename:%(volumename)s, '
                  'Return code:%(rc)lu, '
                  'Error:%(errordesc)s.',
                  {'volumename': volumename,
                   'rc': rc,
                   'errordesc': errordesc})

        # Create return value.
        element_path = {
            'classname': element.classname,
            'keybindings': {
                'SystemName': element['SystemName'],
                'DeviceID': element['DeviceID'],
            },
            'vol_name': d_volumename,
        }

        sdv_no = "0x" + element['DeviceID'][24:28]
        metadata = {
            'FJ_SDV_Name': d_volumename,
            'FJ_SDV_No': sdv_no,
            'FJ_Pool_Name': eternus_pool,
        }
        return (element_path, metadata)

    def delete_snapshot(self, snapshot):
        """Delete snapshot."""
        LOG.debug('delete_snapshot, '
                  'snapshot id: %(sid)s, volume id: %(vid)s.',
                  {'sid': snapshot['id'], 'vid': snapshot['volume_id']})

        vol_exist = self.delete_volume(snapshot)
        LOG.debug('delete_snapshot, vol_exist: %s.', vol_exist)
        return vol_exist

    def initialize_connection(self, volume, connector):
        """Allow connection to connector and return connection info."""
        LOG.debug('initialize_connection, ',
                  'volume id: %(vid)s, protocol: %(prtcl)s.',
                  {'vid': volume['id'], 'prtcl': self.protocol})

        self.conn = self._get_eternus_connection()
        vol_instance = self._find_lun(volume)
        # Check the existence of volume.
        if not vol_instance:
            # Volume not found
            msg = (_('initialize_connection, '
                     'volume: %(volume)s, '
                     'Volume not found.')
                   % {'volume': volume['name']})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        target_portlist = self._get_target_port()
        mapdata = self._get_mapdata(vol_instance, connector, target_portlist)

        if mapdata:
            # Volume is already mapped.
            target_lun = mapdata.get('target_lun', None)
            target_luns = mapdata.get('target_luns', None)

            LOG.info(_LI('initialize_connection, '
                         'volume: %(volume)s, '
                         'target_lun: %(target_lun)s, '
                         'target_luns: %(target_luns)s, '
                         'Volume is already mapped.'),
                     {'volume': volume['name'],
                      'target_lun': target_lun,
                      'target_luns': target_luns})
        else:
            self._map_lun(vol_instance, connector, target_portlist)
            mapdata = self._get_mapdata(vol_instance,
                                        connector, target_portlist)

        mapdata['target_discovered'] = True
        mapdata['volume_id'] = volume['id']

        if self.protocol == 'fc':
            device_info = {'driver_volume_type': 'fibre_channel',
                           'data': mapdata}
        elif self.protocol == 'iSCSI':
            device_info = {'driver_volume_type': 'iscsi',
                           'data': mapdata}

        LOG.debug('initialize_connection, '
                  'device_info:%(info)s.',
                  {'info': device_info})
        return device_info

    def terminate_connection(self, volume, connector, **kwargs):
        """Disallow connection from connector."""
        LOG.debug('terminate_connection, '
                  'volume id: %(vid)s, protocol: %(prtcl)s, '
                  'options: %(options)s.',
                  {'vid': volume['id'], 'prtcl': self.protocol,
                   'options': kwargs})

        self.conn = self._get_eternus_connection()
        self._unmap_lun(volume, connector)

        aglist = self._find_affinity_group(connector)
        if not aglist:
            map_num = 0
        else:
            map_num = len(aglist)

        LOG.debug('terminate_connection, map_num: %s.', map_num)
        return map_num

    def build_fc_init_tgt_map(self, connector, target_wwn=None):
        """Build parameter for Zone Manager."""
        LOG.debug('build_fc_init_tgt_map, target_wwn: %s.', target_wwn)

        initiatorlist = self._find_initiator_names(connector)

        if not target_wwn:
            target_wwn = []
            target_portlist = self._get_target_port()
            for target_port in target_portlist:
                target_wwn.append(target_port['Name'])

        init_tgt_map = {initiator: target_wwn for initiator in initiatorlist}

        LOG.debug('build_fc_init_tgt_map, '
                  'initiator target mapping: %s.', init_tgt_map)
        return init_tgt_map

    @dx_utils.FJDXLockutils('vol', 'cinder-', True)
    def extend_volume(self, volume, new_size):
        """Extend volume on ETERNUS."""
        LOG.debug('extend_volume, volume id: %(vid)s, '
                  'size: %(size)s, new_size: %(nsize)s.',
                  {'vid': volume['id'],
                   'size': volume['size'], 'nsize': new_size})

        self.conn = self._get_eternus_connection()
        volumesize = new_size * units.Gi
        volumename = self._get_volume_name(volume)

        # Get source volume instance.
        vol_instance = self._find_lun(volume)
        if not vol_instance:
            msg = (_('extend_volume, '
                     'volumename: %(volumename)s, '
                     'volume not found.')
                   % {'volumename': volumename})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('extend_volume, volumename: %(volumename)s, '
                  'volumesize: %(volumesize)u, '
                  'volume instance: %(vol_instance)s.',
                  {'volumename': volumename,
                   'volumesize': volumesize,
                   'vol_instance': vol_instance.path})

        # Get poolname from driver configuration file.
        (pool_name, pool) = self._find_pool_from_volume(vol_instance)

        if not pool:
            msg = (_('extend_volume, '
                     'vol_instance: %(vol_instance)s, '
                     'pool not found.')
                   % {'vol_instance': vol_instance.path})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        # Set pooltype.
        if 'RSP' in pool['InstanceID']:
            pooltype = RAIDGROUP
        else:
            pooltype = TPPOOL

        configservice = self._find_eternus_service(STOR_CONF)
        if not configservice:
            msg = (_('extend_volume, volume: %(volume)s, '
                     'volumename: %(volumename)s, '
                     'eternus_pool: %(eternus_pool)s, '
                     'Storage Configuration Service not found.')
                   % {'volume': volume,
                      'volumename': volumename,
                      'eternus_pool': pool_name})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('extend_volume, '
                  'CreateOrModifyElementFromStoragePool, '
                  'ConfigService: %(service)s, '
                  'ElementName: %(volumename)s, '
                  'InPool: %(eternus_pool)s, '
                  'ElementType: %(pooltype)u, '
                  'Size: %(volumesize)u, '
                  'TheElement: %(vol_instance)s.',
                  {'service': configservice,
                   'volumename': volumename,
                   'eternus_pool': pool_name,
                   'pooltype': pooltype,
                   'volumesize': volumesize,
                   'vol_instance': vol_instance.path})

        # Invoke method for extend volume.
        rc, errordesc, job = self._exec_eternus_service(
            'CreateOrModifyElementFromStoragePool',
            configservice,
            ElementName=volumename,
            InPool=pool,
            ElementType=self._pywbem_uint(pooltype, '16'),
            Size=self._pywbem_uint(volumesize, '64'),
            TheElement=vol_instance.path)

        if rc != 0:
            msg = (_('extend_volume, '
                     'volumename: %(volumename)s, '
                     'Return code: %(rc)lu, '
                     'Error: %(errordesc)s, '
                     'PoolType: %(pooltype)s.')
                   % {'volumename': volumename,
                      'rc': rc,
                      'errordesc': errordesc,
                      'pooltype': POOL_TYPE_dic[pooltype]})

            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('extend_volume, '
                  'volumename: %(volumename)s, '
                  'Return code: %(rc)lu, '
                  'Error: %(errordesc)s, '
                  'Pool Name: %(eternus_pool)s, '
                  'Pool Type: %(pooltype)s.',
                  {'volumename': volumename,
                   'rc': rc,
                   'errordesc': errordesc,
                   'eternus_pool': pool_name,
                   'pooltype': POOL_TYPE_dic[pooltype]})

        return pool_name

    @dx_utils.FJDXLockutils('update', 'cinder-', True)
    def update_volume_stats(self, remote=False):
        """Get pool capacity."""
        LOG.debug('update_volume_stats, remote: %(remote)s.',
                  {'remote': remote})
        filename = None

        if remote:
            filename = self.remote_eternus_config

        conn = self._get_eternus_connection(filename)

        poolname_list = self._get_drvcfg('EternusPool',
                                         filename=filename,
                                         multiple=True)

        LOG.debug('update_volume_stats, pool names: %s.', poolname_list)

        pools, notfound_poolnames = self._find_pools(poolname_list, conn)
        # Logging the name of not-found pools.
        if notfound_poolnames:
            LOG.info(_LI('update_volume_stats,'
                         'pool names: %(notfound_poolnames)s, '
                         'some pools are not found.'),
                     {'notfound_poolnames': notfound_poolnames})

        poolinfo = self.scheduler.update_pools(pools, remote=remote)

        if remote:
            return

        self.stats['total_capacity_gb'] = poolinfo['total_capacity_gb']
        self.stats['free_capacity_gb'] = poolinfo['free_capacity_gb']
        self.stats['thin_provisioning_support'] = (
            poolinfo['thin_provisioning_support'])
        self.stats['thick_provisioning_support'] = (
            poolinfo['thick_provisioning_support'])
        self.stats['provisioned_capacity_gb'] = (
            poolinfo['total_capacity_gb'] - poolinfo['free_capacity_gb'])

        self.stats['max_over_subscription_ratio'] = (
            self.configuration.max_over_subscription_ratio)
        self.stats['multiattach'] = True

        LOG.debug('update_volume_stats, '
                  'pool names: %(poolnames)s, '
                  'total capacity[%(total)s], '
                  'free capacity[%(free)s], '
                  'thin_provisioning_support[%(thin)s], '
                  'thick_provisioning_support[%(thick)s], '
                  'provisioned_capacity[%(provisioned)s], '
                  'max_over_subscription_ratio[%(max_over)s].',
                  {'poolnames': poolname_list,
                   'total': self.stats['total_capacity_gb'],
                   'free': self.stats['free_capacity_gb'],
                   'thin': self.stats['thin_provisioning_support'],
                   'thick': self.stats['thick_provisioning_support'],
                   'provisioned': self.stats['provisioned_capacity_gb'],
                   'max_over': self.stats['max_over_subscription_ratio']})

        return (self.stats, poolname_list)

    def _get_mapdata(self, vol_instance, connector, target_portlist):
        """Return mapping information."""
        mapdata = None
        multipath = connector.get('multipath', False)

        LOG.debug('_get_mapdata, volume name: %(vname)s, '
                  'protocol: %(prtcl)s, multipath: %(mpath)s.',
                  {'vname': vol_instance['ElementName'],
                   'prtcl': self.protocol, 'mpath': multipath})

        # Find affinity group.
        # Attach the connector and include the volume.
        aglist = self._find_affinity_group(connector, vol_instance)
        if not aglist:
            LOG.debug('_get_mapdata, ag_list: %s.', aglist)
        else:
            if self.protocol == 'fc':
                mapdata = self._get_mapdata_fc(aglist, vol_instance,
                                               target_portlist)
            elif self.protocol == 'iSCSI':
                mapdata = self._get_mapdata_iscsi(aglist, vol_instance,
                                                  multipath)

        LOG.debug('_get_mapdata, mapdata: %s.', mapdata)
        return mapdata

    def _get_mapdata_fc(self, aglist, vol_instance, target_portlist):
        """_get_mapdata for FibreChannel."""
        target_wwn = []

        try:
            ag_volmaplist = self._reference_eternus_names(
                aglist[0],
                ResultClass='CIM_ProtocolControllerForUnit')
            vo_volmaplist = self._reference_eternus_names(
                vol_instance.path,
                ResultClass='CIM_ProtocolControllerForUnit')
        except Exception:
            msg = (_('_get_mapdata_fc, '
                     'affinitygroup: %(ag)s, '
                     'ReferenceNames, '
                     'cannot connect to ETERNUS.')
                   % {'ag': aglist[0]})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        volmap = None
        for vo_volmap in vo_volmaplist:
            if vo_volmap in ag_volmaplist:
                volmap = vo_volmap
                break

        try:
            volmapinstance = self._get_eternus_instance(
                volmap,
                LocalOnly=False)
        except Exception:
            msg = (_('_get_mapdata_fc, '
                     'volmap: %(volmap)s, '
                     'GetInstance, '
                     'cannot connect to ETERNUS.')
                   % {'volmap': volmap})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        target_lun = int(volmapinstance['DeviceNumber'], 16)

        for target_port in target_portlist:
            target_wwn.append(target_port['Name'])

        mapdata = {
            'target_wwn': target_wwn,
            'target_lun': target_lun,
        }
        LOG.debug('_get_mapdata_fc, mapdata: %s.', mapdata)
        return mapdata

    def _get_mapdata_iscsi(self, aglist, vol_instance, multipath):
        """_get_mapdata for iSCSI."""
        target_portals = []
        target_iqns = []
        target_luns = []

        try:
            vo_volmaplist = self._reference_eternus_names(
                vol_instance.path,
                ResultClass='CIM_ProtocolControllerForUnit')
        except Exception:
            msg = (_('_get_mapdata_iscsi, '
                     'vol_instance: %(vol_instance)s, '
                     'ReferenceNames: CIM_ProtocolControllerForUnit, '
                     'cannot connect to ETERNUS.')
                   % {'vol_instance': vol_instance})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        target_properties_list = self._get_eternus_iscsi_properties()
        target_list = [prop[0] for prop in target_properties_list]
        properties_list = (
            [(prop[1], prop[2]) for prop in target_properties_list])

        for ag in aglist:
            try:
                iscsi_endpointlist = (
                    self._assoc_eternus_names(
                        ag,
                        AssocClass='FUJITSU_SAPAvailableForElement',
                        ResultClass='FUJITSU_iSCSIProtocolEndpoint'))
            except Exception:
                msg = _('_get_mapdata_iscsi, '
                        'Associators: FUJITSU_SAPAvailableForElement, '
                        'cannot connect to ETERNUS.')
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            iscsi_endpoint = iscsi_endpointlist[0]
            if iscsi_endpoint not in target_list:
                continue

            idx = target_list.index(iscsi_endpoint)
            target_portal, target_iqn = properties_list[idx]

            try:
                ag_volmaplist = self._reference_eternus_names(
                    ag,
                    ResultClass='CIM_ProtocolControllerForUnit')
            except Exception:
                msg = (_('_get_mapdata_iscsi, '
                         'affinitygroup: %(ag)s, '
                         'ReferenceNames, '
                         'cannot connect to ETERNUS.')
                       % {'ag': ag})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            volmap = None
            for vo_volmap in vo_volmaplist:
                if vo_volmap in ag_volmaplist:
                    volmap = vo_volmap
                    break

            if not volmap:
                continue

            try:
                volmapinstance = self._get_eternus_instance(
                    volmap,
                    LocalOnly=False)
            except Exception:
                msg = (_('_get_mapdata_iscsi, '
                         'volmap: %(volmap)s, '
                         'GetInstance, '
                         'cannot connect to ETERNUS.')
                       % {'volmap': volmap})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            target_lun = int(volmapinstance['DeviceNumber'], 16)

            target_portals.append(target_portal)
            target_iqns.append(target_iqn)
            target_luns.append(target_lun)

        if multipath:
            mapdata = {
                'target_portals': target_portals,
                'target_iqns': target_iqns,
                'target_luns': target_luns,
            }
        else:
            if target_portals and target_iqns and target_luns:
                mapdata = {
                    'target_portal': target_portals[0],
                    'target_iqn': target_iqns[0],
                    'target_lun': target_luns[0],
                }
            else:
                mapdata = {
                    'target_portal': None,
                    'target_iqn': None,
                    'target_lun': None,
                }

        LOG.debug('_get_mapdata_iscsi, mapdata: %s.', mapdata)
        return mapdata

    def _get_drvcfg(self, tagname, filename=None, multiple=False):
        """Read from driver configuration file."""
        if not filename:
            # Set default configuration file name.
            filename = self.configuration.cinder_eternus_config_file

        LOG.debug('_get_drvcfg, input[%(filename)s][%(tagname)s].',
                  {'filename': filename, 'tagname': tagname})

        tree = parse(filename)
        elem = tree.getroot()

        ret = None
        if not multiple:
            ret = elem.findtext(".//" + tagname)
        else:
            ret = []
            for e in elem.findall(".//" + tagname):
                if e.text and (e.text not in ret):
                    ret.append(e.text)

        if not ret:
            msg = (_('_get_drvcfg, '
                     'filename: %(filename)s, '
                     'tagname: %(tagname)s, '
                     'data is None!! '
                     'Please edit driver configuration file and correct.')
                   % {'filename': filename,
                      'tagname': tagname})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        return ret

    def _get_eternus_connection(self, filename=None):
        """Return WBEM connection."""
        LOG.debug('_get_eternus_connection, filename: %s.', filename)

        ip = self._get_drvcfg('EternusIP', filename)
        port = self._get_drvcfg('EternusPort', filename)
        user = self._get_drvcfg('EternusUser', filename)
        passwd = self._get_drvcfg('EternusPassword', filename)
        url = 'http://' + ip + ':' + port

        conn = pywbem.WBEMConnection(url, (user, passwd),
                                     default_namespace='root/eternus')

        if not conn:
            msg = (_('_get_eternus_connection, '
                     'filename: %(filename)s, '
                     'ip: %(ip)s, '
                     'port: %(port)s, '
                     'user: %(user)s, '
                     'passwd: ****, '
                     'url: %(url)s, '
                     'FAILED!!.')
                   % {'filename': filename,
                      'ip': ip,
                      'port': port,
                      'user': user,
                      'url': url})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_get_eternus_connection, conn: %s.', conn)
        return conn

    def _get_volume_name(self, volume, use_id=False):
        """Get volume_name on ETERNUS from volume on OpenStack."""
        LOG.debug('_get_volume_name, volume_id: %s.', volume['id'])

        if not use_id and volume['provider_location']:
            location = eval(volume['provider_location'])
            if 'vol_name' in location:
                LOG.debug('_get_volume_name, by provider_location, '
                          'vol_name: %s.', location['vol_name'])
                return location['vol_name']

        id_code = volume['id']

        m = hashlib.md5()
        m.update(id_code.encode('utf-8'))

        # pylint: disable=E1121
        volumename = base64.urlsafe_b64encode(m.digest()).decode()
        vol_name = VOL_PREFIX + six.text_type(volumename)[0:8]

        LOG.debug('_get_volume_name, by volume id, '
                  'vol_name: %s.', vol_name)
        return vol_name

    def _get_snap_pool_name(self, snapshot):
        """Get pool name for SDV."""
        LOG.debug('_get_snap_pool_name, snapshot: %s.', snapshot)

        try:
            snap_pool_name = self._get_drvcfg('EternusSnapPool')
        except Exception:
            # When user does not configure,
            # it uses pools defined by EternusPool.
            pool_instance = self._get_pool_from_scheduler(
                snapshot['volume_size'], use_tpp=False)
            snap_pool_name = pool_instance['name']

        LOG.debug('_get_snap_pool_name, snap pool name: %s.', snap_pool_name)
        return snap_pool_name

    def _find_pools(self, poolname_list, conn):
        """Find pool instances by using pool name on ETERNUS."""
        LOG.debug('_find_pools, pool names: %s.', poolname_list)
        target_poolname = list(poolname_list)
        pools = []

        # Get pools info form CIM instance(include info about instance path).
        try:
            rgpoollist = self._enum_eternus_instances(
                'FUJITSU_RAIDStoragePool', conn=conn)
        except Exception:
            msg = (_('_find_pools, '
                     'pool names: %(eternus_pool)s, '
                     'EnumerateInstances, '
                     'cannot connect to ETERNUS.')
                   % {'eternus_pool': target_poolname})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        # Make total pools list.
        rgpools = [(rgpool, 'RAID') for rgpool in rgpoollist]
        poollist = rgpools

        # Get pools with treatable style for PoolManager.
        for pool, ptype in poollist:
            poolname = pool['ElementName']

            LOG.debug('_find_pools, '
                      'pool: %(pool)s, ptype: %(ptype)s.',
                      {'pool': poolname, 'ptype': ptype})
            if poolname in target_poolname:
                try:
                    volume_list = self._assoc_eternus_names(
                        pool.path,
                        conn=conn,
                        AssocClass='CIM_AllocatedFromStoragePool',
                        ResultClass='FUJITSU_StorageVolume')

                    volume_count = len(volume_list)
                except Exception:
                    msg = (_('_find_pools,'
                             'poolname: %(poolname)s, '
                             'pooltype: %(ptype)s, '
                             'Associator Names, '
                             'cannot connect to ETERNUS.')
                           % {'ptype': ptype,
                              'poolname': poolname})
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)

                poolinfo = self.scheduler.create_pool_info(pool,
                                                           volume_count,
                                                           ptype)
                target_poolname.remove(poolname)
                pools.append((poolinfo, poolname))

        if not pools:
            LOG.warning(_LW('_find_pools, all the EternusPools in driver '
                            'configuration file are not exist. '
                            'Please edit driver configuration file.'))

        # Sort pools in the order defined in driver configuration file.
        sorted_pools = [pool for name in poolname_list for pool, pname in pools
                        if name == pname]

        LOG.debug('_find_pools,'
                  'pools: %(pools)s,'
                  'notfound_pools: %(notfound_pools)s.',
                  {'pools': pools,
                   'notfound_pools': target_poolname})

        return (sorted_pools, target_poolname)

    def _find_pool(self, eternus_pool, detail=False):
        """Find Instance or InstanceName of pool by pool name on ETERNUS."""
        LOG.debug('_find_pool, pool name: %s.', eternus_pool)

        # Get pools info form CIM instance(include info about instance path).
        try:
            rgpoollist = self._enum_eternus_instances(
                'FUJITSU_RAIDStoragePool')
        except Exception:
            msg = (_('_find_pool, '
                     'eternus_pool: %(eternus_pool)s, '
                     'EnumerateInstances, '
                     'cannot connect to ETERNUS.')
                   % {'eternus_pool': eternus_pool})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        # Make total pools list.
        poollist = rgpoollist

        # One eternus backend has only one special pool name,
        # so just use pool name can get the target pool.
        for pool in poollist:
            if pool['ElementName'] == eternus_pool:
                poolinstance = pool
                break
        else:
            poolinstance = None

        if not poolinstance:
            ret = None
        elif detail is True:
            ret = poolinstance
        else:
            ret = poolinstance.path

        LOG.debug('_find_pool, pool: %s.', ret)
        return ret

    def _find_eternus_service(self, classname, conn=None):
        """Find CIM instance about service information."""
        LOG.debug('_find_eternus_service, classname: %s.', classname)

        try:
            services = self._enum_eternus_instance_names(
                six.text_type(classname), conn=conn)
        except Exception:
            msg = (_('_find_eternus_service, '
                     'classname: %(classname)s, '
                     'EnumerateInstanceNames, '
                     'cannot connect to ETERNUS.')
                   % {'classname': classname})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        ret = services[0]
        LOG.debug('_find_eternus_service, '
                  'classname: %(classname)s, '
                  'ret: %(ret)s.',
                  {'classname': classname, 'ret': ret})
        return ret

    @dx_utils.FJDXLockutils('SMIS-exec', 'cinder-', True)
    def _exec_eternus_service(self, classname, instanceNameList,
                              conn=None, **param_dict):
        """Execute SMI-S Method."""
        LOG.debug('_exec_eternus_service, '
                  'classname: %(a)s, '
                  'instanceNameList: %(b)s, '
                  'parameters: %(c)s.',
                  {'a': classname,
                   'b': instanceNameList,
                   'c': param_dict})
        rc = None

        if not conn:
            conn = self.conn

        # Use InvokeMethod.
        try:
            rc, retdata = conn.InvokeMethod(
                classname,
                instanceNameList,
                **param_dict)
        except Exception:
            if rc is None:
                msg = (_('_exec_eternus_service, '
                         'classname: %(classname)s, '
                         'InvokeMethod, '
                         'cannot connect to ETERNUS.')
                       % {'classname': classname})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        # If the result has job information, wait for completion.
        if "Job" in retdata:
            rc = self._wait_for_job_complete(conn, retdata)

        errordesc = RETCODE_dic.get(six.text_type(rc), UNDEF_MSG)

        ret = (rc, errordesc, retdata)

        LOG.debug('_exec_eternus_service, '
                  'classname: %(a)s, '
                  'instanceNameList: %(b)s, '
                  'parameters: %(c)s, '
                  'Return code: %(rc)s, '
                  'Error: %(errordesc)s, '
                  'Retrun data: %(retdata)s.',
                  {'a': classname,
                   'b': instanceNameList,
                   'c': param_dict,
                   'rc': rc,
                   'errordesc': errordesc,
                   'retdata': retdata})
        return ret

    @dx_utils.FJDXLockutils('SMIS-other', 'cinder-', True)
    def _enum_eternus_instances(self, classname, conn=None,
                                retry=20, retry_interval=5):
        """Enumerate Instances."""
        LOG.debug('_enum_eternus_instances, classname: %s.', classname)

        if not conn:
            conn = self.conn

        for retry_num in range(retry):
            try:
                ret = conn.EnumerateInstances(classname)
                break
            except Exception as e:
                LOG.info(_LI('_enum_eternus_instances, '
                             'reason: %(reason)s, try (%(retrynum)d).'),
                         {'reason': e.args,
                          'retrynum': (retry_num + 1)})
                time.sleep(retry_interval)
                continue
        else:
            msg = _('_enum_eternus_instances, Error.')
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_enum_eternus_instances, enum %d instances.', len(ret))
        return ret

    @dx_utils.FJDXLockutils('SMIS-other', 'cinder-', True)
    def _enum_eternus_instance_names(self, classname, conn=None,
                                     retry=20, retry_interval=5):
        """Enumerate Instance Names."""
        LOG.debug('_enum_eternus_instance_names, classname: %s.', classname)

        if not conn:
            conn = self.conn

        for retry_num in range(retry):
            try:
                ret = conn.EnumerateInstanceNames(classname)
                break
            except Exception as e:
                LOG.info(_LI('_enum_eternus_instance_names, '
                             'reason: %(reason)s, try (%(retrynum)d).'),
                         {'reason': e.args,
                          'retrynum': (retry_num + 1)})
                time.sleep(retry_interval)
                continue
        else:
            msg = _('_enum_eternus_instance_names, Error.')
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_enum_eternus_instance_names, enum %d names.', len(ret))
        return ret

    @dx_utils.FJDXLockutils('SMIS-getinstance', 'cinder-', True)
    def _get_eternus_instance(self, classname, conn=None, AllowNone=False,
                              retry=20, retry_interval=5, **param_dict):
        """Get Instance."""
        LOG.debug('_get_eternus_instance, '
                  'classname: %(cls)s, param: %(param)s.',
                  {'cls': classname, 'param': param_dict})

        if not conn:
            conn = self.conn

        for retry_num in range(retry):
            try:
                ret = conn.GetInstance(classname, **param_dict)
                break
            except Exception as e:
                if (e.args[0] == 6) and (AllowNone is True):
                    break
                else:
                    LOG.info(_LI('_get_eternus_instance, '
                                 'reason: %(reason)s, try (%(retrynum)d).'),
                             {'reason': e.args,
                              'retrynum': (retry_num + 1)})
                    time.sleep(retry_interval)
                    continue
        else:
            msg = ('_get_eternus_instance, Error.')
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_get_eternus_instance, ret: %s.', ret)
        return ret

    @dx_utils.FJDXLockutils('SMIS-other', 'cinder-', True)
    def _assoc_eternus(self, classname, conn=None,
                       retry=20, retry_interval=5, **param_dict):
        """Associator."""
        LOG.debug('_assoc_eternus, '
                  'classname: %(cls)s, param: %(param)s.',
                  {'cls': classname, 'param': param_dict})

        if not conn:
            conn = self.conn

        for retry_num in range(retry):
            try:
                ret = conn.Associators(classname, **param_dict)
                break
            except Exception as e:
                LOG.info(_LI('_assoc_eternus, '
                             'reason: %(reason)s, retry (%(retrynum)d).'),
                         {'reason': (e.args),
                          'retrynum': (retry_num + 1)})
                time.sleep(retry_interval)
                continue
        else:
            msg = ('_assoc_eternus, Error')
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_assoc_eternus, enum %d instances.', len(ret))
        return ret

    @dx_utils.FJDXLockutils('SMIS-other', 'cinder-', True)
    def _assoc_eternus_names(self, classname, conn=None,
                             retry=20, retry_interval=5, **param_dict):
        """Associator Names."""
        LOG.debug('_assoc_eternus_names, '
                  'classname: %(cls)s, param: %(param)s.',
                  {'cls': classname, 'param': param_dict})

        if not conn:
            conn = self.conn

        for retry_num in range(retry):
            try:
                ret = conn.AssociatorNames(classname, **param_dict)
                break
            except Exception as e:
                LOG.info(_LI('_assoc_eternus_names, '
                             'reason: %(reason)s, try (%(retrynum)d).'),
                         {'reason': e.args,
                          'retrynum': (retry_num + 1)})
                time.sleep(retry_interval)
                continue
        else:
            msg = ('_assoc_eternus_names, Error.')
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_assoc_eternus_names, enum %d names.', len(ret))
        return ret

    @dx_utils.FJDXLockutils('SMIS-other', 'cinder-', True)
    def _reference_eternus_names(self, classname, conn=None,
                                 retry=20, retry_interval=5, **param_dict):
        """Refference Names."""
        LOG.debug('_reference_eternus_names, '
                  'classname: %(cls)s, param: %(param)s.',
                  {'cls': classname, 'param': param_dict})

        if not conn:
            conn = self.conn

        for retry_num in range(retry):
            try:
                ret = conn.ReferenceNames(classname, **param_dict)
                break
            except Exception as e:
                LOG.info(_LI('_reference_eternus_names, '
                             'reason: %(reason)s, try (%(retrynum)d).'),
                         {'reason': e.args,
                          'retrynum': (retry_num + 1)})
                time.sleep(retry_interval)
                continue
        else:
            msg = ('_reference_eternus_names, Error.')
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_reference_eternus_names, enum %d names.', len(ret))
        return ret

    def _create_eternus_instance_name(self, classname, bindings):
        """Create CIM InstanceName from classname and bindings."""
        LOG.debug('_create_eternus_instance_name, '
                  'classname: %(cls)s, bindings: %(bind)s.',
                  {'cls': classname, 'bind': bindings})

        instancename = None

        bindings['CreationClassName'] = classname
        bindings['SystemCreationClassName'] = 'FUJITSU_StorageComputerSystem'

        try:
            instancename = pywbem.CIMInstanceName(
                classname,
                namespace='root/eternus',
                keybindings=bindings)
        except NameError:
            instancename = None

        LOG.debug('_create_eternus_instance_name, ret: %s.', instancename)
        return instancename

    def _find_lun(self, volume, use_service_name=False):
        """Find lun instance from volume class or volumename on ETERNUS."""
        LOG.debug('_find_lun, volume id: %s.', volume['id'])
        volumeinstance = None
        volumename = self._get_volume_name(volume)

        if not use_service_name:
            conn = self.conn
        else:
            service_name = volume['host'].split('@', 1)[1].split('#')[0]
            conf = Configuration(
                FJ_ETERNUS_DX_OPT_opts,
                config_group=service_name,
            )
            conf_filename = conf.cinder_eternus_config_file
            conn = self._get_eternus_connection(conf_filename)

        try:
            location = eval(volume['provider_location'])
            classname = location['classname']
            bindings = location['keybindings']

            if classname and bindings:
                LOG.debug('_find_lun, '
                          'classname: %(classname)s, '
                          'bindings: %(bindings)s.',
                          {'classname': classname,
                           'bindings': bindings})
                volume_instance_name = (
                    self._create_eternus_instance_name(classname, bindings))

                LOG.debug('_find_lun, '
                          'volume_insatnce_name: %(volume_instance_name)s.',
                          {'volume_instance_name': volume_instance_name})

                vol_instance = (
                    self._get_eternus_instance(volume_instance_name,
                                               conn=conn, AllowNone=True))

                if vol_instance['ElementName'] == volumename:
                    volumeinstance = vol_instance
        except Exception:
            volumeinstance = None
            LOG.debug('_find_lun, '
                      'Cannot get volume instance from provider location, '
                      'Search all volume using EnumerateInstanceNames.')

        if not volumeinstance:
            # for old version
            LOG.debug('_find_lun, '
                      'volumename: %(volumename)s.',
                      {'volumename': volumename})

            vol_name = {'source-name': volumename}
            # Get volume instance from volumename on ETERNUS.
            volumeinstance = self._find_lun_with_listup(
                conn=conn, **vol_name)

        LOG.debug('_find_lun, ret: %s.', volumeinstance)
        return volumeinstance

    def _find_copysession(self, vol_instance, conn=None):
        """Find copysession from volumename on ETERNUS."""
        LOG.debug('_find_copysession, volume name: %s.',
                  vol_instance['ElementName'])

        if not conn:
            conn = self.conn

        try:
            cpsessionlist = conn.ReferenceNames(
                vol_instance.path,
                ResultClass='FUJITSU_StorageSynchronized')
        except Exception:
            msg = (_('_find_copysession, '
                     'ReferenceNames, '
                     'vol_instance: %(vol_instance_path)s, '
                     'Cannot connect to ETERNUS.')
                   % {'vol_instance_path': vol_instance.path})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_find_copysession, '
                  'cpsessionlist: %(cpsessionlist)s.',
                  {'cpsessionlist': cpsessionlist})

        LOG.debug('_find_copysession, ret: %s.', cpsessionlist)
        return cpsessionlist

    def _wait_for_copy_complete(self, cpsession, conn=None):
        """Wait for the completion of copy."""
        LOG.debug('_wait_for_copy_complete, cpsession: %s.', cpsession)

        cpsession_instance = None
        complete = False

        if not conn:
            conn = self.conn

        while True:
            try:
                cpsession_instance = conn.GetInstance(
                    cpsession,
                    LocalOnly=False)
            except Exception:
                cpsession_instance = None

            # If copy session is none,
            # it means copy session was finished, break and return.
            if not cpsession_instance:
                complete = True
                break

            # There is no case that copy session is SnapOPC.
            # If copy session is OPC, loop is continued.
            if (cpsession_instance['CopyType'] == EC_REC and
                    cpsession_instance['CopyState'] not in [2, 3]):
                break

            LOG.debug('_wait_for_copy_complete, '
                      'find target copysession, '
                      'wait for end of copysession.')

            if cpsession_instance['CopyState'] == BROKEN:
                msg = (_('_wait_for_copy_complete, '
                         'cpsession: %(cpsession)s, '
                         'copysession state is BROKEN.')
                       % {'cpsession': cpsession})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            time.sleep(10)

        return complete

    def _delete_copysession(self, cpsession, conn=None):
        """Delete copysession."""
        LOG.debug('_delete_copysession: cpssession: %s.', cpsession)

        try:
            cpsession_instance = self._get_eternus_instance(
                cpsession, conn=conn, LocalOnly=False)
        except Exception:
            LOG.info(_LI('_delete_copysession, '
                         'The copysession was already completed.'))
            return

        copytype = cpsession_instance['CopyType']

        # Set oparation code.
        # SnapOPC: 19 (Return To ResourcePool)
        # OPC:8 (Detach)
        # EC/REC:8 (Detach)
        operation = OPERATION_dic.get(copytype, None)
        if not operation:
            msg = (_('_delete_copysession, '
                     'copy session type is undefined! '
                     'copy session: %(cpsession)s, '
                     'copy type: %(copytype)s.')
                   % {'cpsession': cpsession,
                      'copytype': copytype})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        repservice = self._find_eternus_service(REPL, conn=conn)
        if not repservice:
            msg = (_('_delete_copysession, '
                     'Cannot find Replication Service.'))
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        # Invoke method for delete copysession.
        rc, errordesc, job = self._exec_eternus_service(
            'ModifyReplicaSynchronization',
            repservice,
            conn=conn,
            Operation=self._pywbem_uint(operation, '16'),
            Synchronization=cpsession,
            Force=True,
            WaitForCopyState=self._pywbem_uint(15, '16'))

        LOG.debug('_delete_copysession, '
                  'copysession: %(cpsession)s, '
                  'operation: %(operation)s, '
                  'Return code: %(rc)lu, '
                  'Error: %(errordesc)s.',
                  {'cpsession': cpsession,
                   'operation': operation,
                   'rc': rc,
                   'errordesc': errordesc})

        if rc == COPYSESSION_NOT_EXIST:
            LOG.debug('_delete_copysession, '
                      'cpsession: %(cpsession)s, '
                      'copysession is not exist.',
                      {'cpsession': cpsession})
        elif rc != 0:
            msg = (_('_delete_copysession, '
                     'copysession: %(cpsession)s, '
                     'operation: %(operation)s, '
                     'Return code: %(rc)lu, '
                     'Error: %(errordesc)s.')
                   % {'cpsession': cpsession,
                      'operation': operation,
                      'rc': rc,
                      'errordesc': errordesc})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

    def _get_target_port(self):
        """Return target portid."""
        LOG.debug('_get_target_port, protocol: %s.', self.protocol)

        target_portlist = []
        if self.protocol == 'fc':
            prtcl_endpoint = 'FUJITSU_SCSIProtocolEndpoint'
            connection_type = 2
        elif self.protocol == 'iSCSI':
            prtcl_endpoint = 'FUJITSU_iSCSIProtocolEndpoint'
            connection_type = 7

        try:
            tgtportlist = self._enum_eternus_instances(prtcl_endpoint)
        except Exception:
            msg = _('_get_target_port, '
                    'EnumerateInstances, '
                    'cannot connect to ETERNUS.')
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        for tgtport in tgtportlist:
            # Check protocol of tgtport.
            if tgtport['ConnectionType'] != connection_type:
                continue

            # Check: if port is for remote copy, continue.
            #if (tgtport['RAMode'] & 0x7B) != 0x00:
            #    continue

            # Check: if port is for StorageCluster, continue.
            if 'SCGroupNo' in tgtport:
                continue

            target_portlist.append(tgtport)

            #LOG.debug('_get_target_port, '
            #          'connection type: %(cont)s, '
            #          'ramode: %(ramode)s.',
            #          {'cont': tgtport['ConnectionType'],
            #           'ramode': tgtport['RAMode']})

        LOG.debug('_get_target_port, '
                  'target port: %(target_portid)s.',
                  {'target_portid': target_portlist})

        if not target_portlist:
            msg = (_('_get_target_port, '
                     'protcol: %(protocol)s, '
                     'target_port not found.')
                   % {'protocol': self.protocol})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_get_target_port, ret: %s.', target_portlist)
        return target_portlist

    @dx_utils.FJDXLockutils('connect', 'cinder-', True)
    def _map_lun(self, vol_instance, connector, targetlist=None):
        """Map volume to host."""
        volumename = vol_instance['ElementName']
        LOG.debug('_map_lun, '
                  'volume name: %(vname)s, connector: %(connector)s.',
                  {'vname': volumename, 'connector': connector})

        volume_uid = vol_instance['Name']
        initiatorlist = self._find_initiator_names(connector)
        aglist = self._find_affinity_group(connector)
        configservice = self._find_eternus_service(CTRL_CONF)

        if not targetlist:
            targetlist = self._get_target_port()

        if not configservice:
            msg = (_('_map_lun, '
                     'vol_instance.path:%(vol)s, '
                     'volumename: %(volumename)s, '
                     'volume_uid: %(uid)s, '
                     'initiator: %(initiator)s, '
                     'target: %(tgt)s, '
                     'aglist: %(aglist)s, '
                     'Storage Configuration Service not found.')
                   % {'vol': vol_instance.path,
                      'volumename': volumename,
                      'uid': volume_uid,
                      'initiator': initiatorlist,
                      'tgt': targetlist,
                      'aglist': aglist})

            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_map_lun, '
                  'vol_instance.path: %(vol_instance)s, '
                  'volumename:%(volumename)s, '
                  'initiator:%(initiator)s, '
                  'target:%(tgt)s.',
                  {'vol_instance': vol_instance.path,
                   'volumename': [volumename],
                   'initiator': initiatorlist,
                   'tgt': targetlist})

        if not aglist:
            # Create affinity group and set host-affinity.
            for target in targetlist:
                LOG.debug('_map_lun, '
                          'lun_name: %(volume_uid)s, '
                          'Initiator: %(initiator)s, '
                          'target: %(target)s.',
                          {'volume_uid': [volume_uid],
                           'initiator': initiatorlist,
                           'target': target['Name']})

                rc, errordesc, job = self._exec_eternus_service(
                    'ExposePaths',
                    configservice,
                    LUNames=[volume_uid],
                    InitiatorPortIDs=initiatorlist,
                    TargetPortIDs=[target['Name']],
                    DeviceAccesses=[self._pywbem_uint(2, '16')])

                LOG.debug('_map_lun, '
                          'Error: %(errordesc)s, '
                          'Return code: %(rc)lu, '
                          'Create affinitygroup and set host-affinity.',
                          {'errordesc': errordesc,
                           'rc': rc})

                if rc != 0 and rc != LUNAME_IN_USE:
                    LOG.warning(_LW('_map_lun, '
                                    'lun_name: %(volume_uid)s, '
                                    'Initiator: %(initiator)s, '
                                    'target: %(target)s, '
                                    'Return code: %(rc)lu, '
                                    'Error: %(errordesc)s.'),
                                {'volume_uid': [volume_uid],
                                 'initiator': initiatorlist,
                                 'target': target['Name'],
                                 'rc': rc,
                                 'errordesc': errordesc})
        else:
            # Add lun to affinity group
            devid_preset = set()
            for ag in aglist:
                LOG.debug('_map_lun, '
                          'ag: %(ag)s, lun_name: %(volume_uid)s.',
                          {'ag': ag,
                           'volume_uid': volume_uid})

                devid_pre = ag['DeviceID'][:8]

                if devid_pre in devid_preset:
                    continue

                rc, errordesc, job = self._exec_eternus_service(
                    'ExposePaths',
                    configservice, LUNames=[volume_uid],
                    DeviceAccesses=[self._pywbem_uint(2, '16')],
                    ProtocolControllers=[ag])

                LOG.debug('_map_lun, '
                          'Error: %(errordesc)s, '
                          'Return code: %(rc)lu, '
                          'Add lun to affinity group.',
                          {'errordesc': errordesc,
                           'rc': rc})

                if rc != 0:
                    LOG.warning(_LW('_map_lun, '
                                    'lun_name: %(volume_uid)s, '
                                    'Initiator: %(initiator)s, '
                                    'ag: %(ag)s, '
                                    'Return code: %(rc)lu, '
                                    'Error: %(errordesc)s.'),
                                {'volume_uid': [volume_uid],
                                 'initiator': initiatorlist,
                                 'ag': ag,
                                 'rc': rc,
                                 'errordesc': errordesc})

                devid_preset.add(devid_pre)

    def _find_initiator_names(self, connector):
        """Return initiator names."""
        initiatornamelist = []

        if self.protocol == 'fc' and connector['wwpns']:
            LOG.debug('_find_initiator_names, wwpns: %s.',
                      connector['wwpns'])
            initiatornamelist = connector['wwpns']
        elif self.protocol == 'iSCSI' and connector['initiator']:
            LOG.debug('_find_initiator_names, initiator: %s.',
                      connector['initiator'])
            initiatornamelist.append(connector['initiator'])

        if not initiatornamelist:
            msg = (_('_find_initiator_names, '
                     'connector: %(connector)s, '
                     'initiator not found.')
                   % {'connector': connector})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_find_initiator_names, '
                  'initiator list: %(initiator)s.',
                  {'initiator': initiatornamelist})

        return initiatornamelist

    def _find_affinity_group(self, connector, vol_instance=None, conn=None):
        """Find affinity group from connector."""
        LOG.debug('_find_affinity_group, vol_instance: %s.', vol_instance)

        affinity_grouplist = []
        initiatorlist = self._find_initiator_names(connector)

        if not vol_instance:
            try:
                aglist = self._enum_eternus_instance_names(
                    'FUJITSU_AffinityGroupController', conn=conn)
            except Exception:
                msg = (_('_find_affinity_group, '
                         'connector: %(connector)s, '
                         'EnumerateInstanceNames, '
                         'cannot connect to ETERNUS.')
                       % {'connector': connector})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            LOG.debug('_find_affinity_group, '
                      'affinity_groups:%s', aglist)
        else:
            try:
                aglist = self._assoc_eternus_names(
                    vol_instance.path,
                    conn=conn,
                    AssocClass='CIM_ProtocolControllerForUnit',
                    ResultClass='FUJITSU_AffinityGroupController')
            except Exception:
                msg = (_('_find_affinity_group,'
                         'connector: %(connector)s,'
                         'AssocNames: CIM_ProtocolControllerForUnit, '
                         'cannot connect to ETERNUS.')
                       % {'connector': connector})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            LOG.debug('_find_affinity_group, '
                      'vol_instance.path: %(volume)s, '
                      'affinity_groups: %(aglist)s.',
                      {'volume': vol_instance.path,
                       'aglist': aglist})

        for ag in aglist:
            try:
                hostaglist = self._assoc_eternus(
                    ag,
                    conn=conn,
                    AssocClass='CIM_AuthorizedTarget',
                    ResultClass='FUJITSU_AuthorizedPrivilege')
            except Exception:
                msg = (_('_find_affinity_group, '
                         'connector: %(connector)s, '
                         'Associators: CIM_AuthorizedTarget, '
                         'cannot connect to ETERNUS.')
                       % {'connector': connector})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            for hostag in hostaglist:
                for initiator in initiatorlist:
                    if initiator.lower() not in hostag['InstanceID'].lower():
                        continue

                    LOG.debug('_find_affinity_group, '
                              'AffinityGroup: %(ag)s.', {'ag': ag})
                    affinity_grouplist.append(ag)
                    break
                else:
                    continue
                break

        LOG.debug('_find_affinity_group, '
                  'initiators: %(initiator)s, '
                  'affinity_group: %(affinity_group)s.',
                  {'initiator': initiatorlist,
                   'affinity_group': affinity_grouplist})
        return affinity_grouplist

    @dx_utils.FJDXLockutils('connect', 'cinder-', True)
    def _unmap_lun(self, volume, connector, force=False, remote=False):
        """Unmap volume from host."""
        LOG.debug('_unmap_lun, volume id: %(vid)s, '
                  'connector: %(connector)s, force: %(frc)s, '
                  'remote: %(remote)s.',
                  {'vid': volume['id'], 'connector': connector,
                   'frc': force, 'remote': remote})

        volumename = self._get_volume_name(volume)
        if not remote:
            vol_instance = self._find_lun(volume)
            conn = self.conn
        else:
            vol_instance = self._find_replica_lun(volume)
            conn = self.remote_conn

        if not vol_instance:
            LOG.info(_LI('_unmap_lun, '
                         'volumename:%(volumename)s, '
                         'volume not found.'),
                     {'volumename': volumename})
            return False

        volume_uid = vol_instance['Name']

        if not force:
            aglist = self._find_affinity_group(connector,
                                               vol_instance,
                                               conn=conn)
            if not aglist:
                LOG.info(_LI('_unmap_lun, '
                             'volumename: %(volumename)s, '
                             'volume is not mapped.'),
                         {'volumename': volumename})
                return False
        else:
            try:
                aglist = self._assoc_eternus_names(
                    vol_instance.path,
                    conn=conn,
                    AssocClass='CIM_ProtocolControllerForUnit',
                    ResultClass='FUJITSU_AffinityGroupController')
            except Exception:
                msg = (_('_unmap_lun,'
                         'vol_instance.path: %(volume)s, '
                         'AssociatorNames: CIM_ProtocolControllerForUnit, '
                         'cannot connect to ETERNUS.')
                       % {'volume': vol_instance.path})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            LOG.debug('_unmap_lun, '
                      'vol_instance.path: %(volume)s, '
                      'affinity_groups: %(aglist)s.',
                      {'volume': vol_instance.path,
                       'aglist': aglist})

        configservice = self._find_eternus_service(CTRL_CONF, conn=conn)
        if not configservice:
            msg = (_('_unmap_lun, '
                     'vol_instance.path: %(volume)s, '
                     'volumename: %(volumename)s, '
                     'volume_uid: %(uid)s, '
                     'aglist: %(aglist)s, '
                     'Controller Configuration Service not found.')
                   % {'volume': vol_instance.path,
                      'volumename': [volumename],
                      'uid': [volume_uid],
                      'aglist': aglist})

            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        for ag in aglist:
            LOG.debug('_unmap_lun, '
                      'volumename: %(volumename)s, '
                      'volume_uid: %(volume_uid)s, '
                      'AffinityGroup: %(ag)s.',
                      {'volumename': volumename,
                       'volume_uid': volume_uid,
                       'ag': ag})

            rc, errordesc, job = self._exec_eternus_service(
                'HidePaths',
                configservice,
                conn=conn,
                LUNames=[volume_uid],
                ProtocolControllers=[ag])

            LOG.debug('_unmap_lun, '
                      'Error: %(errordesc)s, '
                      'Return code: %(rc)lu.',
                      {'errordesc': errordesc,
                       'rc': rc})

            if rc == LUNAME_NOT_EXIST:
                LOG.debug('_unmap_lun, '
                          'volumename: %(volumename)s, '
                          'Invalid LUNames.',
                          {'volumename': volumename})
            elif rc != 0:
                msg = (_('_unmap_lun, '
                         'volumename: %(volumename)s, '
                         'volume_uid: %(volume_uid)s, '
                         'AffinityGroup: %(ag)s, '
                         'Return code: %(rc)lu, '
                         'Error: %(errordesc)s.')
                       % {'volumename': volumename,
                          'volume_uid': volume_uid,
                          'ag': ag,
                          'rc': rc,
                          'errordesc': errordesc})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_unmap_lun, '
                  'volumename: %(volumename)s.',
                  {'volumename': volumename})
        return True

    def _get_eternus_iscsi_properties(self):
        """Get target port iqns and target_portals."""
        iscsi_properties_list = []
        iscsiip_list = self._get_drvcfg('EternusISCSIIP', multiple=True)
        iscsi_port = self.configuration.iscsi_port

        LOG.debug('_get_eternus_iscsi_properties, iplist: %s.', iscsiip_list)

        try:
            ip_endpointlist = self._enum_eternus_instance_names(
                'FUJITSU_IPProtocolEndpoint')
        except Exception:
            msg = (_('_get_eternus_iscsi_properties, '
                     'iscsiip: %(iscsiip)s, '
                     'EnumerateInstanceNames, '
                     'cannot connect to ETERNUS.')
                   % {'iscsiip': iscsiip_list})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        for ip_endpoint in ip_endpointlist:
            try:
                ip_endpoint_instance = self._get_eternus_instance(
                    ip_endpoint)
                ip_address = ip_endpoint_instance['IPv4Address']
                LOG.debug('_get_eternus_iscsi_properties, '
                          'instanceip: %(ip)s, '
                          'iscsiip: %(iscsiip)s.',
                          {'ip': ip_address,
                           'iscsiip': iscsiip_list})
            except Exception:
                msg = (_('_get_eternus_iscsi_properties, '
                         'iscsiip: %(iscsiip)s, '
                         'GetInstance, '
                         'cannot connect to ETERNUS.')
                       % {'iscsiip': iscsiip_list})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            if ip_address not in iscsiip_list:
                continue

            LOG.debug('_get_eternus_iscsi_properties, '
                      'find iscsiip: %(ip)s.', {'ip': ip_address})
            try:
                tcp_endpointlist = self._assoc_eternus_names(
                    ip_endpoint,
                    AssocClass='CIM_BindsTo',
                    ResultClass='FUJITSU_TCPProtocolEndpoint')
            except Exception:
                msg = (_('_get_eternus_iscsi_properties, '
                         'iscsiip: %(iscsiip)s, '
                         'AssociatorNames: CIM_BindsTo, '
                         'cannot connect to ETERNUS.')
                       % {'iscsiip': ip_address})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            for tcp_endpoint in tcp_endpointlist:
                try:
                    iscsi_endpointlist = (
                        self._assoc_eternus(tcp_endpoint,
                                            AssocClass='CIM_BindsTo',
                                            ResultClass='FUJITSU_iSCSI'
                                            'ProtocolEndpoint'))
                except Exception:
                    msg = (_('_get_eternus_iscsi_properties, '
                             'iscsiip: %(iscsiip)s, '
                             'AssociatorNames: CIM_BindsTo, '
                             'cannot connect to ETERNUS.')
                           % {'iscsiip': ip_address})
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)

                for iscsi_endpoint in iscsi_endpointlist:
                    target_portal = "%s:%s" % (ip_address, iscsi_port)
                    iqn = iscsi_endpoint['Name'].split(',')[0]
                    iscsi_properties_list.append((iscsi_endpoint.path,
                                                  target_portal,
                                                  iqn))
                    LOG.debug('_get_eternus_iscsi_properties, '
                              'target_portal: %(target_portal)s, '
                              'iqn: %(iqn)s.',
                              {'target_portal': target_portal,
                               'iqn': iqn})

        if not iscsi_properties_list:
            msg = (_('_get_eternus_iscsi_properties, '
                     'iscsiip list: %(iscsiip_list)s, '
                     'iqn not found.')
                   % {'iscsiip_list': iscsiip_list})

            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        LOG.debug('_get_eternus_iscsi_properties, '
                  'iscsi_properties_list: %(iscsi_properties_list)s.',
                  {'iscsi_properties_list': iscsi_properties_list})

        return iscsi_properties_list

    def _wait_for_job_complete(self, conn, job):
        """Given the job wait for it to complete."""
        self.retries = 0
        self.wait_for_job_called = False

        def _wait_for_job_complete():
            """Called at an interval until the job is finished."""
            if self._is_job_finished(conn, job):
                raise loopingcall.LoopingCallDone()
            if self.retries > JOB_RETRIES:
                LOG.error(_LE('_wait_for_job_complete, '
                              'failed after %(retries)d tries.'),
                          {'retries': self.retries})
                raise loopingcall.LoopingCallDone()

            try:
                self.retries += 1
                if not self.wait_for_job_called:
                    if self._is_job_finished(conn, job):
                        self.wait_for_job_called = True
            except Exception as e:
                LOG.error(_LE('Exception: %s'), e)
                exceptionMessage = _('Issue encountered waiting for job.')
                LOG.error(exceptionMessage)
                raise exception.VolumeBackendAPIException(exceptionMessage)

        self.wait_for_job_called = False
        timer = loopingcall.FixedIntervalLoopingCall(_wait_for_job_complete)
        timer.start(interval=JOB_INTERVAL_SEC).wait()

        jobInstanceName = job['Job']
        jobinstance = conn.GetInstance(jobInstanceName,
                                       LocalOnly=False)

        rc = jobinstance['ErrorCode']

        LOG.debug('_wait_for_job_complete, rc: %s.', rc)
        return rc

    def _is_job_finished(self, conn, job):
        """Check if the job is finished."""
        jobInstanceName = job['Job']
        jobinstance = conn.GetInstance(jobInstanceName,
                                       LocalOnly=False)
        jobstate = jobinstance['JobState']
        LOG.debug('_is_job_finished, state: %(state)s', {'state': jobstate})
        # From ValueMap of JobState in CIM_ConcreteJob
        # 2=New, 3=Starting, 4=Running, 32767=Queue Pending
        # ValueMap('2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13..32767,
        # 32768..65535'),
        # Values('New, Starting, Running, Suspended, Shutting Down,
        # Completed, Terminated, Killed, Exception, Service,
        # Query Pending, DMTF Reserved, Vendor Reserved')]
        # NOTE(deva): string matching based on
        #             http://ipmitool.cvs.sourceforge.net/
        #               viewvc/ipmitool/ipmitool/lib/ipmi_chassis.c

        if jobstate in [2, 3, 4]:
            job_finished = False
        else:
            job_finished = True

        LOG.debug('_is_job_finished, finish: %s.', job_finished)
        return job_finished

    def _pywbem_uint(self, num, datatype):
        try:
            result = {
                '8': pywbem.Uint8(num),
                '16': pywbem.Uint16(num),
                '32': pywbem.Uint32(num),
                '64': pywbem.Uint64(num),
            }
            result = result.get(datatype, num)
        except NameError:
            result = num

        return result

    @dx_utils.FJDXLockutils('consume_pool', 'cinder-', True)
    def _get_pool_from_scheduler(self, volume_size_gb,
                                 use_tpp=True, remote=False):
        LOG.debug('_get_pool_from_scheduler, '
                  'size: %(size)s, remote: %(remote)s.',
                  {'size': volume_size_gb, 'remote': remote})

        self.update_volume_stats(remote)
        pool_instance = self.scheduler.consume_poolspace(
            volume_size_gb, use_tpp=use_tpp, remote=remote)

        LOG.debug('_get_pool_from_scheduler, pool: %s.', pool_instance)
        return pool_instance

    def manage_existing(self, volume, existing_ref):
        """Bring an existing storage object under Cinder management.

        existing_ref can contain source-id or source-name.
        source-id: lun uuid
        source-name: lun name
        """
        vol_instance = self._get_manage_volume_instance(volume, existing_ref)

        element_path = {
            'classname': vol_instance.classname,
            'vol_name': vol_instance['ElementName'],
            'keybindings': {
                'SystemName': vol_instance['SystemName'],
                'DeviceID': vol_instance['DeviceID'],
            }
        }

        # Get eternus model name.
        try:
            systemnamelist = (
                self._enum_eternus_instances('FUJITSU_StorageProduct'))
        except Exception:
            msg = (_('manage_existing, '
                     'volume: %s, '
                     'EnumerateInstances, '
                     'cannot connect to ETERNUS.')
                   % volume)
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        volume_no = '0x' + vol_instance['DeviceID'][24:28]
        metadata = {
            'FJ_Backend': systemnamelist[0]['IdentifyingNumber'],
            'FJ_Volume_Name': vol_instance['ElementName'],
            'FJ_Volume_No': volume_no,
            'FJ_Pool_Name': vol_instance['poolname'],
            'FJ_Pool_Type': vol_instance['pooltype'],
        }

        return {'provider_location': six.text_type(element_path),
                'metadata': metadata}

    def manage_existing_get_size(self, volume, existing_ref):
        """Return size of volume to be managed by manage_existing.

        When calculating the size, round up to the next GB.
        """
        vol_instance = self._get_manage_volume_instance(volume, existing_ref)

        size_byte = vol_instance['BlockSize'] * vol_instance['NumberOfBlocks']
        vol_size = (size_byte + units.Gi - 1) / units.Gi

        LOG.debug('manage_existing_get_size, size: %s.', vol_size)
        return vol_size

    def _get_manage_volume_instance(self, volume, existing_ref):
        """Get manage volume instance from existing_ref.

        The volume must be a standard volume or TPV,
        and in EternusPool of configuration file.
        """
        self.conn = self._get_eternus_connection()
        vol_instance = self._find_lun_with_listup(self.conn, **existing_ref)

        if not vol_instance:
            msg = (_('_get_manage_volume_instance, '
                     'source name or source id: %s, '
                     'volume not found.')
                   % existing_ref)
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        LOG.debug('_get_manage_volume_instance, '
                  'volume name: %(volumename)s, '
                  'Purpose: %(Purpose)s, '
                  'volume instance: %(vol_instance)s.',
                  {'volumename': vol_instance['ElementName'],
                   'Purpose': vol_instance['Purpose'],
                   'vol_instance': vol_instance.path})

        # Get volume type.
        volume_type = int(vol_instance['Purpose'][8:10])
        if ((volume_type != TYPE_OLU) and (volume_type != TYPE_TPPC)):
            msg = (_('_get_manage_volume_instance, '
                     'volume type: %u, volume type not support.')
                   % volume_type)
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        (poolname, target_pool) = self._find_pool_from_volume(vol_instance)
        # Set poolname and pooltype.
        vol_instance['poolname'] = poolname
        if 'RSP' in target_pool['InstanceID']:
            vol_instance['pooltype'] = POOL_TYPE_dic[RAIDGROUP]
        else:
            vol_instance['pooltype'] = POOL_TYPE_dic[TPPOOL]
        return vol_instance

    def _find_lun_with_listup(self, conn=None, **kwargs):
        """Find lun instance with source name or source id on ETERNUS."""
        volumeinstance = None
        src_id = kwargs.get('source-id', None)
        src_name = kwargs.get('source-name', None)

        if (not src_id and not src_name):
            msg = (_('_find_lun_with_listup, '
                     'source-name or source-id: %s, '
                     'Must specify source-name or source-id.')
                   % kwargs)
            LOG.error(msg)
            raise exception.ManageExistingInvalidReference(data=msg)

        if (src_id and src_name):
            msg = (_('_find_lun_with_listup, '
                     'source-name or source-id: %s, '
                     'Must only specify source-name or source-id.')
                   % kwargs)
            LOG.error(msg)
            raise exception.ManageExistingInvalidReference(data=msg)

        if (src_id and not src_id.isdigit()):
            msg = (_('_find_lun_with_listup, '
                     'the specified source-id(%s) must be a decimal number.')
                   % src_id)
            LOG.error(msg)
            raise exception.ManageExistingInvalidReference(data=msg)

        # Get volume instance from volumename on ETERNUS.
        try:
            namelist = self._enum_eternus_instance_names(
                'FUJITSU_StorageVolume', conn=conn)
        except Exception:
            msg = (_('_find_lun_with_listup, '
                     'source-name or source-id: %s, '
                     'EnumerateInstanceNames.')
                   % kwargs)
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        for name in namelist:
            try:
                vol_instance = self._get_eternus_instance(
                    name, conn=conn, AllowNone=True)
                if src_id:
                    volume_no = vol_instance['DeviceID'][24:28]
                    if int(src_id) == int(volume_no, 16):
                        volumeinstance = vol_instance
                        break
                if src_name:
                    if vol_instance['ElementName'] == src_name:
                        volumeinstance = vol_instance
                        break
            except Exception:
                continue
        else:
            LOG.debug('_find_lun_with_listup, '
                      'source-name or source-id: %s, '
                      'volume not found on ETERNUS.', kwargs)

        return volumeinstance

    def _find_pool_from_volume(self, vol_instance, remote=False):
        """Find Instance or InstanceName of pool by volume instance."""
        LOG.debug('_find_pool_from_volume, remote: %(remote)s.',
                  {'remote': remote})
        poolname = None
        target_pool = None
        filename = None

        if not remote:
            conn = self.conn
        else:
            conn = self.remote_conn
            filename = self.remote_eternus_config

        # Get poolname of volume on Eternus.
        try:
            pools = self._assoc_eternus(
                vol_instance.path,
                conn=conn,
                AssocClass='CIM_AllocatedFromStoragePool',
                ResultClass='CIM_StoragePool')
        except Exception:
            msg = (_('_find_pool_from_volume, '
                     'vol_instance: %s, '
                     'Associators: CIM_AllocatedFromStoragePool, '
                     'cannot connect to ETERNUS.')
                   % vol_instance.path)
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        if not pools:
            msg = (_('_find_pool_from_volume, '
                     'vol_instance: %s, '
                     'pool not found.')
                   % vol_instance.path)
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        # Get poolname from driver configuration file.
        cfgpool_list = list(self._get_drvcfg('EternusPool',
                                             filename=filename,
                                             multiple=True))

        for pool in pools:
            if pool['ElementName'] in cfgpool_list:
                poolname = pool['ElementName']
                target_pool = pool.path
                break

        if not target_pool:
            msg = (_('_find_pool_from_volume, '
                     'vol_instance: %s, '
                     'the pool of volume not in driver configuration file.')
                   % vol_instance.path)
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        return (poolname, target_pool)

    def update_migrated_volume(self, ctxt, volume, new_volume):
        """Update migrated volume."""
        LOG.debug('update_migrated_volume, '
                  'source volume id: %(s_id)s, '
                  'target volume id: %(t_id)s.',
                  {'s_id': volume['id'], 't_id': new_volume['id']})

        dst_metadata = dx_utils.get_metadata(new_volume)
        src_metadata = dx_utils.get_metadata(volume)

        LOG.debug('source: (%(src_meta)s)(%(src_loc)s), '
                  'target: (%(dst_meta)s)(%(dst_loc)s).',
                  {'src_meta': src_metadata,
                   'src_loc': volume['provider_location'],
                   'dst_meta': dst_metadata,
                   'dst_loc': new_volume['provider_location']})

        src_location = eval(volume['provider_location'])

        if 'vol_name' not in src_location:
            src_name = self._get_volume_name(volume)

            if src_location['keybindings']['CreationClassName']:
                del src_location['keybindings']['CreationClassName']

            if src_location['keybindings']['SystemCreationClassName']:
                del src_location['keybindings']['SystemCreationClassName']

            src_location['vol_name'] = src_name

            db.volume_update(
                ctxt, volume['id'],
                {'provider_location': six.text_type(src_location)})

        db.volume_update(ctxt, new_volume['id'],
                         {'provider_location': six.text_type(src_location)})

    def _prepare_replica(self):
        """Get replication target device informations from cinder.conf."""
        remote_host = None
        remote_eternus_config = None
        remote_backend = None

        # Get replica_device in cinder.conf
        replica_dev_list = self.configuration.replication_device

        if replica_dev_list:
            # Currently we only support one replication device.
            replica_device = replica_dev_list[0]
            LOG.debug('_prepare_replica, replica_device: %s.', replica_device)

            managed_backend = replica_device.get(
                'managed_backend_name', None)

            remote_eternus_config = replica_device.get(
                'cinder_eternus_config_file', None)

            if not remote_eternus_config:
                msg = _('_prepare_replica, '
                        'No cinder_eternus_config_file was '
                        'found in replica_device.')
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

            # Currently we don't support unmanaged replication device.
            if not managed_backend:
                msg = _('_prepare_replica, '
                        'No managed_backend_name was '
                        'found in replica_device.')
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)
            else:
                try:
                    # Ensure managed_backend_name must be the form of
                    # "host@backend#backend_name"
                    (remote_host, temp) = self._get_key_pair(
                        managed_backend, '@')
                    (remote_backend, remote_backend_name) = (
                        self._get_key_pair(temp, '#'))
                except Exception:
                    msg = _('_prepare_replica, '
                            'The form of managed_backend_name is not '
                            '"host@backend#backend_name".')
                    LOG.error(msg)
                    raise exception.VolumeBackendAPIException(data=msg)

        return (remote_host, remote_backend, remote_eternus_config)

    def _create_replica_volume(self, volume, src_metadata):
        """Create replica volume."""
        LOG.debug('_create_replica_volume, '
                  'src volume id: %(v_id)s, '
                  'src metadata: %(meta)s.',
                  {'v_id': volume['id'],
                   'meta': src_metadata})

        repl_data = None
        repl_status = None
        error_msg = None

        try:
            (_, repl_metadata, repl_data, repl_status) = (
                self._create_volume(volume, remote=True))
        except Exception:
            repl_status = 'error'
            error_msg = 'Create remote volume failed.'
            return (repl_data, repl_status, error_msg)

        # Use CCM plug-in to start REC-mirror.
        try:
            copy_type = 'mirror'
            self._exec_ccm_script("start",
                                  source=src_metadata,
                                  target=repl_metadata,
                                  copy_type=copy_type)
        except Exception:
            repl_status = 'error'
            error_msg = 'Create remote copy failed.'

        return (repl_data, repl_status, error_msg)

    def _exec_ccm_script(self, command, source=None, source_volume=None,
                         target=None, target_volume=None, copy_type=None):
        """Execute CCM script."""
        LOG.debug('_exec_ccm_script, '
                  'command: %(cmd)s, src metadata: %(src_meta)s, '
                  'tgt metadata: %(tgt_meta)s, copy type: %(copy_type)s.',
                  {'cmd': command, 'src_meta': source,
                   'tgt_meta': target, 'copy_type': copy_type})

        s_option = ""
        d_option = ""
        t_option = ""

        if source:
            if 'FJ_Volume_No' in source:
                s_olu_no = source['FJ_Volume_No']
            else:
                vol_instance = self._find_lun(
                    source_volume, use_service_name=True)
                s_olu_no = "0x" + vol_instance['DeviceID'][24:28]

            s_serial_no = source['FJ_Backend'][-10:]
            s_option = " -s \"%s/%s\"" % (s_serial_no, s_olu_no)
            command += s_option

        if target:
            d_olu_no = target['FJ_Volume_No']
            d_serial_no = target['FJ_Backend'][-10:]
            d_option = " -d \"%s/%s\"" % (d_serial_no, d_olu_no)
            command += d_option

        if s_option and d_option:
            if s_option[3:] == d_option[3:]:
                msg = (_('_exec_ccm_script,'
                         'command:%(command)s,'
                         'source_vol:%(s_option)s and '
                         'target_vol:%(d_option)s are the same one.')
                       % {'command': command, 's_option': s_option[3:],
                          'd_option': d_option[3:]})
                LOG.error(msg)
                raise exception.VolumeBackendAPIException(data=msg)

        if copy_type:
            t_option = " -t %s" % ((copy_type[0]).lower() + copy_type[1:])
            command += t_option

        try:
            (out, _err) = utils.execute('acrec', command, run_as_root=True)
            LOG.debug('_exec_ccm_script,'
                      'command:%(command)s,'
                      's_option:%(s_option)s,'
                      'd_option:%(d_option)s,'
                      't_option:%(t_option)s.',
                      {'command': command,
                       's_option': s_option,
                       'd_option': d_option,
                       't_option': t_option})

        except Exception as ex:
            LOG.error(_LE('%s'), ex)
            LOG.error(_LE('_exec_ccm_script,'
                          'command:%(command)s,'
                          's_option:%(s_option)s,'
                          'd_option:%(d_option)s,'
                          't_option:%(t_option)s')
                      % {'command': command, 's_option': s_option,
                         'd_option': d_option, 't_option': t_option})
            raise exception.VolumeBackendAPIException(ex)

    def _get_key_pair(self, info, delimiter):
        key_pair = info.split(delimiter, 1)
        if len(key_pair) != 2:
            msg = (_('Cannot use %(sep)s to split %(info)s, '
                     'please check the format.')
                   % {'sep': delimiter, 'info': info})
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)
        return (key_pair[0], key_pair[1])

    def _find_replica_lun(self, volume):
        """Find replica volume by volume no."""
        LOG.debug('_find_replica_lun, '
                  'src volume id: %(v_id)s.',
                  {'v_id': volume['id']})

        volume_no = None
        volume_dic = {}

        if volume['replication_driver_data']:
            replica_data = eval(volume['replication_driver_data'])
        else:
            msg = _('_find_replica_lun, '
                    'replication_driver_data is None.')
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        if replica_data['Remote_Device_ID']:
            volume_no = int(replica_data['Remote_Device_ID'][24:28], 16)
            volume_dic = {'source-id': six.text_type(volume_no)}

        try:
            volume_instance = self._find_lun_with_listup(self.remote_conn,
                                                         **volume_dic)
        except Exception:
            msg = (_('_find_replica_lun, '
                     'Cannot find replica volume, '
                     'volume no: %s.')
                   % volume_no)
            LOG.error(msg)
            raise exception.VolumeBackendAPIException(data=msg)

        return volume_instance
