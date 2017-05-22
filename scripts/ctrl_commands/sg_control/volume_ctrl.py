import grpc

from control_api import volume_control_pb2


class VolumeCtrl(object):
    def __init__(self, host, port):
        conn_str = '{}:{}'.format(host, port)
        self.channel = grpc.insecure_channel(conn_str)
        self.stub = volume_control_pb2.VolumeControlStub(self.channel)

    def do(self, args):
        if args.action == 'list':
            if args.list_type == 'volume':
                res = self.ListVolumes()
            else:
                res = self.ListDevices()
        elif args.action == 'get':
            res = self.GetVolume(args.vol_id)
        elif args.action == 'enable':
            kwargs = {'vol_id': args.vol_id,
                      'vol_size': args.vol_size,
                      'device': args.device_path}
            res = self.EnableSG(**kwargs)
        elif args.action == 'disable':
            res = self.DisableSG(args.vol_id)
        elif args.action == 'initialize':
            res = self.InitializeConnection(args.vol_id)
        elif args.action == 'terminate':
            res = self.TerminateConnection(args.vol_id)
        elif args.action == 'attach':
            res = self.AttachVolume(args.vol_id,args.device_path)
        elif args.action == 'detach':
            res = self.AttachVolume(args.vol_id)
        else:
            res = 1
        return res

    def ListVolumes(self):
        res = self.stub.ListVolumes(volume_control_pb2.ListVolumesReq())
        print ('list volumes result:%s\n' % res.status)
        for info in res.volumes:
            print ('volumes: %s' % info)
            print "\n"
        return res

    def ListDevices(self):
        res = self.stub.ListDevices(volume_control_pb2.ListDevicesReq())
        print ('list devices result:%s\n' % res.status)
        print ('devices: %s' % res.devices)
        return res

    def EnableSG(self, vol_id, vol_size, device):
        res = self.stub.EnableSG(volume_control_pb2.EnableSGReq(
            volume_id=vol_id,
            size=vol_size,
            device=device))
        print ('enable SG result:%s' % res.status)
        return res

    def DisableSG(self, vol_id):
        res = self.stub.DisableSG(volume_control_pb2.DisableSGReq(
            volume_id=vol_id))
        print ('disable SG result:%s' % res.status)
        return res

    def GetVolume(self, vol_id):
        res = self.stub.GetVolume(volume_control_pb2.GetVolumeReq(
            volume_id=vol_id))
        print ('get volume result:%s\n' % res.status)
        print ('volume info: %s' % res.volume)
        return res

    def InitializeConnection(self, vol_id):
        res = self.stub.InitializeConnection(
            volume_control_pb2.InitializeConnectionReq(volume_id=vol_id))
        print ('initialize connection result:%s' % res.status)
        return res

    def TerminateConnection(self, vol_id):
        res = self.stub.TerminateConnection(
            volume_control_pb2.TerminateConnectionReq(volume_id=vol_id))
        print ('terminate connectionn result:%s' % res.status)
        return res

    def AttachVolume(self, vol_id, device):
        res = self.stub.AttachVolume(
            volume_control_pb2.AttachVolumeReq(volume_id=vol_id,
                                               device=device))
        print ('attach volume result:%s' % res.status)
        return res

    def DetachVolume(self, vol_id):
        res = self.stub.DetachVolume(
            volume_control_pb2.DetachVolumeReq(volume_id=vol_id))
        print ('detach volume result:%s' % res.status)
        return res
