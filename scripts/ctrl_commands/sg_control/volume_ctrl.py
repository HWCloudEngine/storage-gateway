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
