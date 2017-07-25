import grpc

from control_api import backup_control_pb2
from control_api import backup_pb2
from control_api import common_pb2


class BackupCtrl(object):
    def __init__(self, host, port):
        conn_str = '{}:{}'.format(host, port)
        self.channel = grpc.insecure_channel(conn_str)
        self.stub = backup_control_pb2.BackupControlStub(self.channel)

    # TODO
    def do(self, args):
        if args.action == 'create':
            kwargs = {
                'backup_mode': args.backup_mode,
                'backup_type': args.backup_type,
                'vol_id': args.vol_id,
                'vol_size': args.vol_size,
                'backup_id': args.backup_id
            }
            res = self.CreateBackup(**kwargs)
        elif args.action == 'restore':
            kwargs = {
                'vol_id': args.vol_id,
                'vol_size': args.vol_size,
                'backup_id': args.backup_id,
                'new_vol_id': args.new_vol_id,
                'new_vol_size': args.new_vol_size,
                'new_device': args.new_device,
                'backup_type': args.backup_type
            }
            res = self.DeleteBackup(**kwargs)
        elif args.action == 'delete':
            kwargs = {
                'vol_id': args.vol_id,
                'backup_id': args.backup_id
            }
            res = self.RestoreBackup(**kwargs)
        elif args.action == 'get':
            kwargs = {
                'vol_id': args.vol_id,
                'backup_id': args.backup_id
            }
            res = self.GetBackup(**kwargs)
        return res

    def CreateBackup(self, backup_mode, backup_type, vol_id, vol_size,
                     backup_id):
        if backup_mode is None or backup_mode == 'full':
            backup_mode = backup_pb2.BACKUP_FULL
        else:
            backup_mode = backup_pb2.BACKUP_INCR

        if backup_type is None or backup_type == 'local':
            backup_type = backup_pb2.BACKUP_LOCAL
        else:
            backup_type = backup_pb2.BACKUP_REMOTE

        res = self.stub.CreateBackup(
            backup_control_pb2.CreateBackupReq(
                vol_name=vol_id,
                vol_size=vol_size,
                backup_name=backup_id,
                backup_option=backup_pb2.BackupOption(
                    backup_mode=backup_mode,
                    backup_type=backup_type)))
        print "create backup result: %s" % res.status
        return res

    def DeleteBackup(self, backup_id, vol_id):
        res = self.stub.DeleteBackup(
            backup_control_pb2.DeleteBackupReq(
                backup_name=backup_id,
                vol_name=vol_id))
        print "delete backup result: %s" % res.status
        return res

    def RestoreBackup(self, backup_id, backup_type, vol_id, vol_size,
                      new_vol_id, new_vol_size, new_device):
        if backup_type is None or backup_type == 'local':
            backup_type = backup_pb2.BACKUP_LOCAL
        else:
            backup_type = backup_pb2.BACKUP_REMOTE
        res = self.stub.RestoreBackup(
            backup_control_pb2.RestoreBackupReq(
                vol_name=vol_id,
                vol_size=vol_size,
                backup_name=backup_id,
                backup_type=backup_type,
                new_vol_name=new_vol_id,
                new_vol_size=new_vol_size,
                new_block_device=new_device))
        print "restore backup result: %s" % res.status
        return res

    def GetBackup(self, backup_id, vol_id):
        res = self.stub.GetBackup(
            backup_control_pb2.GetBackupReq(
                vol_name=vol_id,
                backup_name=backup_id))
        print "get backup result: %s" % res.status
        if res.status == common_pb2.sOk:
            print "backup status: %s" % res.backup_status
        return res
