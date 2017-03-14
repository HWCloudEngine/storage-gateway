import sys,grpc
import common_pb2,volume_pb2
from control_api import volume_control_pb2

class VolumeCtrl():
    def __init__(self,args):
        conn_str = '{}:{}'.format(args['host'], args['port']) 
        self.channel = grpc.insecure_channel(conn_str)
        self.stub = volume_control_pb2.VolumeControlStub(self.channel)

    def do(self,args):
        if args.action == 'list':
            if args.list_type == 'volume':
                res = self.ListVolumes(args)
            else:
                res = self.ListDevices(args)
        elif args.action == 'get':
            res = self.GetVolume(args)
        elif args.action == 'enable':
            res = self.EnableSG(args)
        elif args.action == 'disable':
            res = self.DisableSG(args)

        return res

    def ListVolumes(self,args):
        res = self.stub.ListVolumes(volume_control_pb2.ListVolumesReq())
        print ('list volumes result:%s\n' % res.status)
        for i in res.volumes:
            print ('volumes: %s' % res.volumes.get(i))
        return res

    def ListDevices(self,args):
        res = self.stub.ListDevices(volume_control_pb2.ListDevicesReq())
        print ('list devices result:%s\n' % res.status)
        print ('devices: %s' % res.devices)
        return res

    def EnableSG(self,args):
        res = self.stub.EnableSG(volume_control_pb2.EnableSGReq(
            volume_id = args.vol_id,
            size = args.vol_size,
            device = args.device_path))
        print ('enable SG result:%s' % res.status)
        return res

    def DisableSG(self,args):
        res = self.stub.DisableSG(volume_control_pb2.DisableSGReq(
            volume_id = args.vol_id))
        print ('disable SG result:%s' % res.status)
        return res

    def GetVolume(self,args):
        res = self.stub.GetVolume(volume_control_pb2.GetVolumeReq(
            volume_id = args.vol_id))
        print ('get volume result:%s\n' % res.status)
        print ('volume info: %s' % res.volume)
        return res