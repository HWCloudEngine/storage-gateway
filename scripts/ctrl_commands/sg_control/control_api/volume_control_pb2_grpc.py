import grpc
from grpc.framework.common import cardinality
from grpc.framework.interfaces.face import utilities as face_utilities

import control_api.volume_control_pb2 as control__api_dot_volume__control__pb2
import control_api.volume_control_pb2 as control__api_dot_volume__control__pb2
import control_api.volume_control_pb2 as control__api_dot_volume__control__pb2
import control_api.volume_control_pb2 as control__api_dot_volume__control__pb2
import control_api.volume_control_pb2 as control__api_dot_volume__control__pb2
import control_api.volume_control_pb2 as control__api_dot_volume__control__pb2
import control_api.volume_control_pb2 as control__api_dot_volume__control__pb2
import control_api.volume_control_pb2 as control__api_dot_volume__control__pb2
import control_api.volume_control_pb2 as control__api_dot_volume__control__pb2
import control_api.volume_control_pb2 as control__api_dot_volume__control__pb2


class VolumeControlStub(object):
  """northern oriented rpc service interface
  """

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.ListDevices = channel.unary_unary(
        '/huawei.proto.control.VolumeControl/ListDevices',
        request_serializer=control__api_dot_volume__control__pb2.ListDevicesReq.SerializeToString,
        response_deserializer=control__api_dot_volume__control__pb2.ListDevicesRes.FromString,
        )
    self.EnableSG = channel.unary_unary(
        '/huawei.proto.control.VolumeControl/EnableSG',
        request_serializer=control__api_dot_volume__control__pb2.EnableSGReq.SerializeToString,
        response_deserializer=control__api_dot_volume__control__pb2.EnableSGRes.FromString,
        )
    self.DisableSG = channel.unary_unary(
        '/huawei.proto.control.VolumeControl/DisableSG',
        request_serializer=control__api_dot_volume__control__pb2.DisableSGReq.SerializeToString,
        response_deserializer=control__api_dot_volume__control__pb2.DisableSGRes.FromString,
        )
    self.GetVolume = channel.unary_unary(
        '/huawei.proto.control.VolumeControl/GetVolume',
        request_serializer=control__api_dot_volume__control__pb2.GetVolumeReq.SerializeToString,
        response_deserializer=control__api_dot_volume__control__pb2.GetVolumeRes.FromString,
        )
    self.ListVolumes = channel.unary_unary(
        '/huawei.proto.control.VolumeControl/ListVolumes',
        request_serializer=control__api_dot_volume__control__pb2.ListVolumesReq.SerializeToString,
        response_deserializer=control__api_dot_volume__control__pb2.ListVolumesRes.FromString,
        )


class VolumeControlServicer(object):
  """northern oriented rpc service interface
  """

  def ListDevices(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def EnableSG(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def DisableSG(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def GetVolume(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def ListVolumes(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_VolumeControlServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'ListDevices': grpc.unary_unary_rpc_method_handler(
          servicer.ListDevices,
          request_deserializer=control__api_dot_volume__control__pb2.ListDevicesReq.FromString,
          response_serializer=control__api_dot_volume__control__pb2.ListDevicesRes.SerializeToString,
      ),
      'EnableSG': grpc.unary_unary_rpc_method_handler(
          servicer.EnableSG,
          request_deserializer=control__api_dot_volume__control__pb2.EnableSGReq.FromString,
          response_serializer=control__api_dot_volume__control__pb2.EnableSGRes.SerializeToString,
      ),
      'DisableSG': grpc.unary_unary_rpc_method_handler(
          servicer.DisableSG,
          request_deserializer=control__api_dot_volume__control__pb2.DisableSGReq.FromString,
          response_serializer=control__api_dot_volume__control__pb2.DisableSGRes.SerializeToString,
      ),
      'GetVolume': grpc.unary_unary_rpc_method_handler(
          servicer.GetVolume,
          request_deserializer=control__api_dot_volume__control__pb2.GetVolumeReq.FromString,
          response_serializer=control__api_dot_volume__control__pb2.GetVolumeRes.SerializeToString,
      ),
      'ListVolumes': grpc.unary_unary_rpc_method_handler(
          servicer.ListVolumes,
          request_deserializer=control__api_dot_volume__control__pb2.ListVolumesReq.FromString,
          response_serializer=control__api_dot_volume__control__pb2.ListVolumesRes.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'huawei.proto.control.VolumeControl', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
