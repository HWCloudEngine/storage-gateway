import grpc

from control_api import common_pb2
from control_api import snapshot_control_pb2
from control_api import snapshot_pb2


class SnapCtrl(snapshot_control_pb2.SnapshotControlStub):
    def __init__(self, args):
        conn_str = '{}:{}'.format(args['host'], args['port'])
        self.channel = grpc.insecure_channel(conn_str)
        self.stub = snapshot_control_pb2.SnapshotControlStub(self.channel)

    def do(self, args):
        if args.action == 'create':
            res = self.CreateSnapshot(args)
        elif args.action == 'delete':
            res = self.DeleteSnapshot(args)
        elif args.action == 'list':
            res = self.ListSnapshot(args)
        elif args.action == 'rollback':
            res = self.RollbackSnapshot(args)
        elif args.action == 'diff':
            res = self.DiffSnapshot(args)
        elif args.action == 'read':
            res = self.ReadSnapshot(args)
        return res

    def CreateSnapshot(self, args):
        if args.type == 'local':
            header = snapshot_pb2.SnapReqHead(
                snap_type=snapshot_pb2.SNAP_LOCAL)
        else:
            header = snapshot_pb2.SnapReqHead(
                snap_type=snapshot_pb2.SNAP_REMOTE,
                checkpoint_uuid=args.cp_uuid)

        res = self.stub.CreateSnapshot(
            snapshot_control_pb2.CreateSnapshotReq(
                header=header,
                vol_name=args.vol_id,
                snap_name=args.snap_id))
        print ('create snapshot result:%s' % res.header.status)
        return res

    def DeleteSnapshot(self, args):
        res = self.stub.DeleteSnapshot(snapshot_control_pb2.DeleteSnapshotReq(
            header=snapshot_pb2.SnapReqHead(),
            vol_name=args.vol_id,
            snap_name=args.snap_id))
        print ('delete snapshot result:%s' % res.header.status)
        return res

    def ListSnapshot(self, args):
        res = self.stub.ListSnapshot(snapshot_control_pb2.ListSnapshotReq(
            header=snapshot_pb2.SnapReqHead(),
            vol_name=args.vol_id))
        print ('list snapshot result:%s' % res.header.status)
        if res.ret == 0 and len(res.snap_name) > 0:
            for name in res.snap_name:
                print name
        return res

    def RollbackSnapshot(self, args):
        res = self.stub.RollbackSnapshot(
            snapshot_control_pb2.RollbackSnapshotReq(
                header=snapshot_pb2.SnapReqHead(),
                vol_name=args.vol_id,
                snap_name=args.snap_id))
        print "rollback snapshot result: %s" % res.header.status
        return res

    def DiffSnapshot(self, args):
        res = self.stub.DiffSnapshot(snapshot_control_pb2.DiffSnapshotReq(
            header=snapshot_pb2.SnapReqHead(),
            vol_name=args.vol_id,
            first_snap_name=args.snap_id,
            last_snap_name=args.snap_id2))
        print "diff snapshot result: %s" % res.header.status
        return res

    def ReadSnapshot(self, args):
        res = self.stub.ReadSnapshot(snapshot_control_pb2.ReadSnapshotReq(
            header=snapshot_pb2.SnapReqHead(),
            vol_name=args.vol_id,
            snap_name=args.snap_id,
            off=args.offset,
            len=args.length))
        print "read snapshot result: %s" % res.header.status
        return res

    def GetSnapshot(self, args):
        res = self.stub.QuerySnapshot(snapshot_control_pb2.QuerySnapshotReq(
            header=snapshot_pb2.SnapReqHead(),
            vol_name=args.vol_id,
            snap_name=args.snap_id))
        print "get snapshot result: %s" % res.header.status
        if res.header.status == common_pb2.sOk:
            print "snapshot status is: %s" % res.snap_status
        return res


"""todo other snap action"""
