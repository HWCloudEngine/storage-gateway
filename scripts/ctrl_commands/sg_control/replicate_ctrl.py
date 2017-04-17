import grpc
import uuid

from control_api import replicate_control_pb2
from control_api import replicate_pb2


class RepliacteCtrl(object):
    def __init__(self, host, port):
        conn_str = '{}:{}'.format(host, port)
        self.channel = grpc.insecure_channel(conn_str)
        self.stub = replicate_control_pb2.ReplicateControlStub(self.channel)

    def do(self, args):
        if args.role.upper() == 'PRIMARY':
            role = replicate_pb2.REP_PRIMARY
        else:
            role = replicate_pb2.REP_SECONDARY
        kwargs = {
            'operate_id': args.op_id,
            'vol_id': args.vol_id,
            'role': role
        }

        if args.action == 'create':
            kwargs = {
                'rep_uuid': args.uuid,
                'operate_id': args.op_id,
                'local_volume': args.vol_id,
                'peer_volumes': [args.vol_id2],
                'role': role
            }
            res = self.CreateReplication(**kwargs)
        elif args.action == 'delete':
            res = self.DeleteReplication(**kwargs)
        elif args.action == 'enable':
            res = self.EnableReplication(**kwargs)
        elif args.action == 'disable':
            res = self.DisableReplication(**kwargs)
        elif args.action == 'failover':
            kwargs['checkpoint_id'] = args.cp_uuid
            res = self.FailoverReplication(**kwargs)
        elif args.action == 'reverse':
            res = self.ReverseReplication(**kwargs)
        return res

    def CreateReplication(self, rep_uuid, local_volume, role, peer_volumes,
                          operate_id=str(uuid.uuid4())):
        res = self.stub.CreateReplication(
            replicate_control_pb2.CreateReplicationReq(
                rep_uuid=rep_uuid,
                operate_id=operate_id,
                local_volume=local_volume,
                peer_volumes=peer_volumes,
                role=role))
        print ('create replication result:%s' % res.status)
        return res

    def EnableReplication(self, vol_id, role, operate_id=str(uuid.uuid4())):
        res = self.stub.EnableReplication(
            replicate_control_pb2.EnableReplicationReq(
                vol_id=vol_id,
                operate_id=operate_id,
                role=role))
        print ('enable replication result:%s' % res.status)
        return res

    def DisableReplication(self, vol_id, role, operate_id=str(uuid.uuid4())):
        res = self.stub.DisableReplication(
            replicate_control_pb2.DisableReplicationReq(
                vol_id=vol_id,
                operate_id=operate_id,
                role=role))
        print ('disable replication result:%s' % res.status)
        return res

    def FailoverReplication(self, vol_id, role, checkpoint_id=None,
                            operate_id=str(uuid.uuid4())):
        res = self.stub.FailoverReplication(
            replicate_control_pb2.FailoverReplicationReq(
                vol_id=vol_id,
                operate_id=operate_id,
                checkpoint_id=checkpoint_id,
                role=role))
        print ('failover replication result:%s' % res.status)
        return res

    def ReverseReplication(self, vol_id, role, operate_id=str(uuid.uuid4())):
        res = self.stub.ReverseReplication(
            replicate_control_pb2.ReverseReplicationReq(
                vol_id=vol_id,
                operate_id=operate_id,
                role=role))
        print ('reverse replication result:%s' % res.status)
        return res

    def DeleteReplication(self, vol_id, role, operate_id=str(uuid.uuid4())):
        res = self.stub.DeleteReplication(
            replicate_control_pb2.DeleteReplicationReq(
                vol_id=vol_id,
                operate_id=operate_id,
                role=role))
        print ('delete replication result:%s' % res.status)
        return res
