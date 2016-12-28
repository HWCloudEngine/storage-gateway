import grpc
import replicate_control_pb2

class RepliacteCtrl():
    def __init__(self,args):
        channel = grpc.insecure_channel(args['ip'] + ':' + args['port'])
        self.stub = replicate_control_pb2.ReplicateControlStub(channel)

    def do(self,args):
        if args.action == 'create':
            res = self.CreateReplication(args)
        elif args.action == 'delete':
            res = self.DeleteReplication(args)
        elif args.action == 'list':
            res = self.ListReplication(args)
        elif args.action == 'query':
            res = self.QueryReplication(args)
        elif args.action == 'enable':
            res = self.EnableReplication(args)
        elif args.action == 'disable':
            res = self.DisableReplication(args)
        elif args.action == 'failover':
            res = self.FailoverReplication(args)
        elif args.action == 'reverse':
            res = self.ReverseReplication(args)

        return res;

    def CreateReplication(self,args):
        if args.role.upper() == 'PRIMARY':
            role_ = replicate_control_pb2.REP_PRIMARY
        else:
            role_ = replicate_control_pb2.REP_SECONDARY
        res = self.stub.CreateReplication(
            replicate_control_pb2.CreateReplicationReq(uuid=args.uuid,
                operate_id=args.op_id,
                primary_volume=args.vol_id,
                secondary_volume=args.vol_id2,
                role=role_))
        print ('create replication result:%s' % res.ret)
        return res
        
    def EnableReplication(self,args):
        res = self.stub.EnableReplication(replicate_control_pb2.ReplicationCommonReq(
            uuid=args.uuid,
            operate_id=args.op_id,
            role=args.role))
        print ('enable replication result:%s' % res.ret)
        return res

    def DisableReplication(self,args):
        res = self.stub.DisableReplication(replicate_control_pb2.ReplicationCommonReq(
            uuid=args.uuid,
            operate_id=args.op_id,
            role=args.role))
        print ('disable replication result:%s' %   res.ret)
        return res

    def FailoverReplication(self,args):
        res = self.stub.FailoverReplication(replicate_control_pb2.ReplicationCommonReq(
            uuid=args.uuid,
            operate_id=args.op_id,
            role=args.role))
        print ('failover replication result:%s' % res.ret)
        return res

    def ListReplication(self,args):
        res = self.stub.ListReplication(replicate_control_pb2.ListReplicationReq(
            operate_id=args.op_id))
        print ('list replication result:%s' % res.ret)
        if(res.ret == 0 and len(res.tuple) > 0):
            for replication in res.tuple:
                print replication
        return res

    def ReverseReplication(self,args):
        res = self.stub.ReverseReplication(replicate_control_pb2.ReplicationCommonReq(
            uuid=args.uuid,
            operate_id=args.op_id,
            role=args.role))
        print ('reverse replication result:%s' % res.ret)
        return res

    def DeleteReplication(self,args):
        res = self.stub.DeleteReplication(replicate_control_pb2.ReplicationCommonReq(
            uuid=args.uuid,
            operate_id=args.op_id,
            role=args.role))
        print ('delete replication result:%s' % res.ret)
        return res

    def QueryReplication(self,args):
        res = self.stub.QueryReplication(replicate_control_pb2.ReplicationCommonReq(
            uuid=args.uuid,
            operate_id=args.operate_id))
        print ('query replication result:%s' % res.ret)
        if(res.ret == 0):
            print res.element
        return res
