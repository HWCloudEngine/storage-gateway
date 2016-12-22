import grpc
import control_pb2

class SnapCtrl(control_pb2.CtrlRpcSvcStub):
    def __init__(self,args):
        channel = grpc.insecure_channel(args['ip'] + ':' + args['port'])
        self.stub = control_pb2.CtrlRpcSvcStub(channel)

    def do(self,args):
 #       results = {
 #           'create':self.CreateSnapshot(args),
 #           'delete':self.DeleteSnapshot(args),
 #           'list':self.ListSnapshot(args),
 #           'rollback':self.RollbackSnapshot(args),
 #           'diff':self.DiffSnapshot(args),
 #           'read':self.ReadSnapshot(args)
 #       }
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

        return res;

    def CreateSnapshot(self,args):
        res = self.stub.CreateSnapshot(
                control_pb2.CreateSnapshotReq(vol_name=args.vol_id,   
                snap_name=args.snap_id))
        print ('create snapshot result:%s' % res.ret)
        return res

    def DeleteSnapshot(self,args):
        res = self.stub.DeleteSnapshot(control_pb2.DeleteSnapshotReq(
                vol_name=args.vol_id,
                snap_name=args.snap_id))
        print ('delete snapshot result:%s' % res.ret)
        return res

    def ListSnapshot(self,args):
        res = self.stub.ListSnapshot(control_pb2.ListSnapshotReq(
                vol_name=args.vol_id))
        print ('list snapshot result:%s' % res.ret)
        if(res.ret == 0 and len(res.snap_name) > 0):
            for name in res.snap_name:
                print name

    def RollbackSnapshot(self,args):
        res = self.stub.RollbackSnapshot(control_pb2.RollbackSnapshotReq(
                vol_name=args.vol_id,
                snap_name=args.snap_id))

    def DiffSnapshot(self,args):
        res = self.stub.DiffSnapshot(control_pb2.DiffSnapshotReq(
                vol_name=args.vol_id,
                first_snap_name=args.snap_id,
                last_snap_name=args.snap_id2))

    def ReadSnapshot(self,args):
        res = self.stub.ReadSnapshot(control_pb2.ReadSnapshotReq(
                vol_name=args.vol_id,
                snap_name=args.snap_id,
                off=args.offset,
                len=args.length))
"""todo other snap action"""
