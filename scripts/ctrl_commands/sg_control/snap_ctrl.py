import grpc

from control_api import common_pb2
from control_api import snapshot_control_pb2
from control_api import snapshot_pb2


class SnapCtrl(object):
    def __init__(self, host, port):
        conn_str = '{}:{}'.format(host, port)
        self.channel = grpc.insecure_channel(conn_str)
        self.stub = snapshot_control_pb2.SnapshotControlStub(self.channel)

    def do(self, args):
        if args.action == 'create':
            kwargs = {
                'snap_type': args.type,
                'checkpoint_uuid': args.cp_uuid,
                'vol_id': args.vol_id,
                'snap_id': args.snap_id
            }
            res = self.CreateSnapshot(**kwargs)
        elif args.action == 'delete':
            kwargs = {
                'vol_id': args.vol_id,
                'snap_id': args.snap_id
            }
            res = self.DeleteSnapshot(**kwargs)
        elif args.action == 'list':
            kwargs = {'vol_id': args.vol_id}
            res = self.ListSnapshot(**kwargs)
        elif args.action == 'rollback':
            kwargs = {
                'vol_id': args.vol_id,
                'snap_id': args.snap_id
            }
            res = self.RollbackSnapshot(**kwargs)
        elif args.action == 'diff':
            kwargs = {
                'vol_id': args.vol_id,
                'snap_id1': args.snap_id,
                'snap_id2': args.snap_id2
            }
            res = self.DiffSnapshot(**kwargs)
        elif args.action == 'read':
            kwargs = {
                'vol_id': args.vol_id,
                'snap_id': args.snap_id,
                'offset': args.offset,
                'length': args.length
            }
            res = self.ReadSnapshot(**kwargs)
        elif args.action == 'get':
            kwargs = {
                'vol_id': args.vol_id,
                'snap_id': args.snap_id
            }
            res = self.GetSnapshot(**kwargs)
        elif args.action == 'create_volume':
            kwargs = {
                'vol_id': args.vol_id,
                'snap_id': args.snap_id,
                'new_vol_id': args.vol_id2,
                'new_device': args.device2
            }
            res = self.CreateVolumeFromSnap(**kwargs)
        elif args.action == 'query_volume':
            res = self.QueryVolumeFromSnap(args.vol_id2)
        return res

    def CreateSnapshot(self, snap_type, vol_id, snap_id, checkpoint_uuid=None):
        if snap_type == 'local':
            header = snapshot_pb2.SnapReqHead(
                snap_type=snapshot_pb2.SNAP_LOCAL)
        else:
            header = snapshot_pb2.SnapReqHead(
                snap_type=snapshot_pb2.SNAP_REMOTE,
                checkpoint_uuid=checkpoint_uuid)

        res = self.stub.CreateSnapshot(
            snapshot_control_pb2.CreateSnapshotReq(
                header=header,
                vol_name=vol_id,
                snap_name=snap_id))
        print ('create snapshot result:%s' % res.header.status)
        return res

    def DeleteSnapshot(self, vol_id, snap_id):
        res = self.stub.DeleteSnapshot(snapshot_control_pb2.DeleteSnapshotReq(
            header=snapshot_pb2.SnapReqHead(),
            vol_name=vol_id,
            snap_name=snap_id))
        print ('delete snapshot result:%s' % res.header.status)
        return res

    def ListSnapshot(self, vol_id):
        res = self.stub.ListSnapshot(snapshot_control_pb2.ListSnapshotReq(
            header=snapshot_pb2.SnapReqHead(),
            vol_name=vol_id))
        print ('list snapshot result:%s' % res.header.status)
        if res.ret == 0 and len(res.snap_name) > 0:
            for name in res.snap_name:
                print name
        return res

    def RollbackSnapshot(self, vol_id, snap_id):
        res = self.stub.RollbackSnapshot(
            snapshot_control_pb2.RollbackSnapshotReq(
                header=snapshot_pb2.SnapReqHead(),
                vol_name=vol_id,
                snap_name=snap_id))
        print "rollback snapshot result: %s" % res.header.status
        return res

    def DiffSnapshot(self, vol_id, snap_id1, snap_id2):
        res = self.stub.DiffSnapshot(snapshot_control_pb2.DiffSnapshotReq(
            header=snapshot_pb2.SnapReqHead(),
            vol_name=vol_id,
            first_snap_name=snap_id1,
            last_snap_name=snap_id2))
        print "diff snapshot result: %s" % res.header.status
        return res

    def ReadSnapshot(self, vol_id, snap_id, offset, length):
        res = self.stub.ReadSnapshot(snapshot_control_pb2.ReadSnapshotReq(
            header=snapshot_pb2.SnapReqHead(),
            vol_name=vol_id,
            snap_name=snap_id,
            off=offset,
            len=length))
        print "read snapshot result: %s" % res.header.status
        return res

    def GetSnapshot(self, vol_id, snap_id):
        res = self.stub.QuerySnapshot(snapshot_control_pb2.QuerySnapshotReq(
            header=snapshot_pb2.SnapReqHead(),
            vol_name=vol_id,
            snap_name=snap_id))
        print "get snapshot result: %s" % res.header.status
        if res.header.status == common_pb2.sOk:
            print "snapshot status is: %s" % res.snap_status
        return res

    def CreateVolumeFromSnap(self, vol_id, snap_id, new_vol_id, new_device):
        res = self.stub.CreateVolumeFromSnap(
            snapshot_control_pb2.CreateVolumeFromSnapReq(
                header=snapshot_pb2.SnapReqHead(),
                vol_name=vol_id,
                snap_name=snap_id,
                new_vol_name=new_vol_id,
                new_blk_device=new_device))
        print "create volume from snapshot result: %s" % res.header.status
        return res

    def QueryVolumeFromSnap(self, new_vol_id):
        res = self.stub.QueryVolumeFromSnap(
            snapshot_control_pb2.QuerySnapshotReq(
                header=snapshot_pb2.SnapReqHead(),
                new_vol_name=new_vol_id))
        print "create volume from snapshot result: %s" % res.header.status
        if res.header.status == common_pb2.sOk:
            print "vol status is: %s" % res.vol_status
        return res

"""todo other snap action"""
