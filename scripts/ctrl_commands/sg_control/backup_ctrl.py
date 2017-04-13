import grpc

from control_api import backup_control_pb2
from control_api import backup_pb2
from control_api import common_pb2


class BackupCtrl():
    def __init__(self, args):
        conn_str = '{}:{}'.format(args['host'], args['port'])
        self.channel = grpc.insecure_channel(conn_str)
        self.stub = backup_control_pb2.BackupControlStub(self.channel)

    # TODO
    def do(self, args):
        print ('backup action: %s' % args.action)

    def CreateBackup(self, args):
        backup_mode = args.get('mode', None)
        if backup_mode is None or backup_mode == 'full':
            backup_mode = backup_pb2.BACKUP_FULL
        else:
            backup_mode = backup_pb2.BACKUP_INCR

        backup_type = args.get('type', None)
        if backup_type is None or backup_type == 'local':
            backup_type = backup_pb2.BACKUP_LOCAL
        else:
            backup_type = backup_pb2.BACKUP_REMOTE

        res = self.stub.CreateBackup(
            backup_control_pb2.CreateBackupReq(
                vol_name=args.vol_id,
                vol_size=args.vol_size,
                backup_name=args.backup_id,
                backup_option=backup_pb2.BackupOption(
                    backup_mode=backup_mode,
                    backup_type=backup_type
                )
            )
        )
        print "create backup result: %s" % res.status
        return res

    def DeleteBackup(self, args):
        res = self.stub.DeleteBackup(
            backup_control_pb2.DeleteBackupReq(
            backup_name=args.backup_id,
            vol_name=args.vol_id
        ))
        print "delete backup result: %s" % res.status
        return res

    def RestoreBackup(self, args):
        res = self.stub.RestoreBackup(
            backup_control_pb2.RestoreBackupReq(
                vol_name=args.vol_id,
                backup_name=args.backup_id,
                new_vol_name=args.vol_id2,
                new_vol_size=args.vol_size2,
                new_block_device=args.device2
            ))
        print "restore backup result: %s" % res.status
        return res

    def GetBackup(self, args):
        res = self.stub.GetBackup(
            backup_control_pb2.GetBackupReq(
                vol_name=args.vol_id,
                backup_name=args.backup_id
            ))
        print "get backup result: %s" % res.status
        if res.status == common_pb2.sOk:
            print "backup status: %s" % res.backup_status
        return res
