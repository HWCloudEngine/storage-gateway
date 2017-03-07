import sys,grpc
import common_pb2,replicate_pb2
from control_api import replicate_control_pb2
from journal import journal_pb2

class RepliacteCtrl():
    def __init__(self,args):
        conn_str = '{}:{}'.format(args['host'], args['port']) 
        self.channel = grpc.insecure_channel(conn_str)
        self.stub = replicate_control_pb2.ReplicateControlStub(self.channel)

    def do(self,args):
        if args.role.upper() == 'PRIMARY':
            self.role_ = replicate_pb2.REP_PRIMARY
        else:
            self.role_ = replicate_pb2.REP_SECONDARY
        if args.action == 'create':
            res = self.CreateReplication(args)
        elif args.action == 'delete':
            res = self.DeleteReplication(args)
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
        res = self.stub.CreateReplication(
            replicate_control_pb2.CreateReplicationReq(
                rep_uuid=args.uuid,
                operate_id=args.op_id,
                local_volume=args.vol_id,
                peer_volumes=[args.vol_id2],
                role=self.role_))
        print ('create replication result:%s' % res.status)
        return res
        
    def EnableReplication(self,args):
        res = self.stub.EnableReplication(replicate_control_pb2.EnableReplicationReq(
            vol_id=args.vol_id,
            operate_id=args.op_id,
            role=self.role_))
        print ('enable replication result:%s' % res.status)
        return res

    def DisableReplication(self,args):
        res = self.stub.DisableReplication(replicate_control_pb2.DisableReplicationReq(
            vol_id=args.vol_id,
            operate_id=args.op_id,
            role=self.role_))
        print ('disable replication result:%s' %   res.status)
        return res

    def FailoverReplication(self,args):
        res = self.stub.FailoverReplication(replicate_control_pb2.FailoverReplicationReq(
            vol_id=args.vol_id,
            operate_id=args.op_id,
            role=self.role_))
        print ('failover replication result:%s' % res.status)
        return res

    def ReverseReplication(self,args):
        res = self.stub.ReverseReplication(replicate_control_pb2.ReverseReplicationReq(
            vol_id=args.vol_id,
            operate_id=args.op_id,
            role=self.role_))
        print ('reverse replication result:%s' % res.status)
        return res

    def DeleteReplication(self,args):
        res = self.stub.DeleteReplication(replicate_control_pb2.DeleteReplicationReq(
            uuid=args.uuid,
            operate_id=args.op_id,
            role=self.role_))
        print ('delete replication result:%s' % res.status)
        return res

