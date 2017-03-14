import sys,grpc
import common_pb2,backup_pb2
from control_api import backup_control_pb2

class BackupCtrl():
    def __init__(self,args):
        conn_str = '{}:{}'.format(args['host'], args['port']) 
        self.channel = grpc.insecure_channel(conn_str)
        self.stub = backup_control_pb2.BackupControlStub(self.channel)

    # TODO
    def do(self,args):
        print ('backup action: %s' % args.action)

