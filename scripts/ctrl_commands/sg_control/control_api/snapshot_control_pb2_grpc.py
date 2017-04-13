import grpc
from grpc.framework.common import cardinality
from grpc.framework.interfaces.face import utilities as face_utilities

import control_api.snapshot_control_pb2 as control__api_dot_snapshot__control__pb2
import control_api.snapshot_control_pb2 as control__api_dot_snapshot__control__pb2
import control_api.snapshot_control_pb2 as control__api_dot_snapshot__control__pb2
import control_api.snapshot_control_pb2 as control__api_dot_snapshot__control__pb2
import control_api.snapshot_control_pb2 as control__api_dot_snapshot__control__pb2
import control_api.snapshot_control_pb2 as control__api_dot_snapshot__control__pb2
import control_api.snapshot_control_pb2 as control__api_dot_snapshot__control__pb2
import control_api.snapshot_control_pb2 as control__api_dot_snapshot__control__pb2
import control_api.snapshot_control_pb2 as control__api_dot_snapshot__control__pb2
import control_api.snapshot_control_pb2 as control__api_dot_snapshot__control__pb2
import control_api.snapshot_control_pb2 as control__api_dot_snapshot__control__pb2
import control_api.snapshot_control_pb2 as control__api_dot_snapshot__control__pb2
import control_api.snapshot_control_pb2 as control__api_dot_snapshot__control__pb2
import control_api.snapshot_control_pb2 as control__api_dot_snapshot__control__pb2


class SnapshotControlStub(object):
  """northern oriented rpc service interface
  """

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.CreateSnapshot = channel.unary_unary(
        '/huawei.proto.control.SnapshotControl/CreateSnapshot',
        request_serializer=control__api_dot_snapshot__control__pb2.CreateSnapshotReq.SerializeToString,
        response_deserializer=control__api_dot_snapshot__control__pb2.CreateSnapshotAck.FromString,
        )
    self.ListSnapshot = channel.unary_unary(
        '/huawei.proto.control.SnapshotControl/ListSnapshot',
        request_serializer=control__api_dot_snapshot__control__pb2.ListSnapshotReq.SerializeToString,
        response_deserializer=control__api_dot_snapshot__control__pb2.ListSnapshotAck.FromString,
        )
    self.QuerySnapshot = channel.unary_unary(
        '/huawei.proto.control.SnapshotControl/QuerySnapshot',
        request_serializer=control__api_dot_snapshot__control__pb2.QuerySnapshotReq.SerializeToString,
        response_deserializer=control__api_dot_snapshot__control__pb2.QuerySnapshotAck.FromString,
        )
    self.RollbackSnapshot = channel.unary_unary(
        '/huawei.proto.control.SnapshotControl/RollbackSnapshot',
        request_serializer=control__api_dot_snapshot__control__pb2.RollbackSnapshotReq.SerializeToString,
        response_deserializer=control__api_dot_snapshot__control__pb2.RollbackSnapshotAck.FromString,
        )
    self.DeleteSnapshot = channel.unary_unary(
        '/huawei.proto.control.SnapshotControl/DeleteSnapshot',
        request_serializer=control__api_dot_snapshot__control__pb2.DeleteSnapshotReq.SerializeToString,
        response_deserializer=control__api_dot_snapshot__control__pb2.DeleteSnapshotAck.FromString,
        )
    self.DiffSnapshot = channel.unary_unary(
        '/huawei.proto.control.SnapshotControl/DiffSnapshot',
        request_serializer=control__api_dot_snapshot__control__pb2.DiffSnapshotReq.SerializeToString,
        response_deserializer=control__api_dot_snapshot__control__pb2.DiffSnapshotAck.FromString,
        )
    self.ReadSnapshot = channel.unary_unary(
        '/huawei.proto.control.SnapshotControl/ReadSnapshot',
        request_serializer=control__api_dot_snapshot__control__pb2.ReadSnapshotReq.SerializeToString,
        response_deserializer=control__api_dot_snapshot__control__pb2.ReadSnapshotAck.FromString,
        )


class SnapshotControlServicer(object):
  """northern oriented rpc service interface
  """

  def CreateSnapshot(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def ListSnapshot(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def QuerySnapshot(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def RollbackSnapshot(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def DeleteSnapshot(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def DiffSnapshot(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def ReadSnapshot(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_SnapshotControlServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'CreateSnapshot': grpc.unary_unary_rpc_method_handler(
          servicer.CreateSnapshot,
          request_deserializer=control__api_dot_snapshot__control__pb2.CreateSnapshotReq.FromString,
          response_serializer=control__api_dot_snapshot__control__pb2.CreateSnapshotAck.SerializeToString,
      ),
      'ListSnapshot': grpc.unary_unary_rpc_method_handler(
          servicer.ListSnapshot,
          request_deserializer=control__api_dot_snapshot__control__pb2.ListSnapshotReq.FromString,
          response_serializer=control__api_dot_snapshot__control__pb2.ListSnapshotAck.SerializeToString,
      ),
      'QuerySnapshot': grpc.unary_unary_rpc_method_handler(
          servicer.QuerySnapshot,
          request_deserializer=control__api_dot_snapshot__control__pb2.QuerySnapshotReq.FromString,
          response_serializer=control__api_dot_snapshot__control__pb2.QuerySnapshotAck.SerializeToString,
      ),
      'RollbackSnapshot': grpc.unary_unary_rpc_method_handler(
          servicer.RollbackSnapshot,
          request_deserializer=control__api_dot_snapshot__control__pb2.RollbackSnapshotReq.FromString,
          response_serializer=control__api_dot_snapshot__control__pb2.RollbackSnapshotAck.SerializeToString,
      ),
      'DeleteSnapshot': grpc.unary_unary_rpc_method_handler(
          servicer.DeleteSnapshot,
          request_deserializer=control__api_dot_snapshot__control__pb2.DeleteSnapshotReq.FromString,
          response_serializer=control__api_dot_snapshot__control__pb2.DeleteSnapshotAck.SerializeToString,
      ),
      'DiffSnapshot': grpc.unary_unary_rpc_method_handler(
          servicer.DiffSnapshot,
          request_deserializer=control__api_dot_snapshot__control__pb2.DiffSnapshotReq.FromString,
          response_serializer=control__api_dot_snapshot__control__pb2.DiffSnapshotAck.SerializeToString,
      ),
      'ReadSnapshot': grpc.unary_unary_rpc_method_handler(
          servicer.ReadSnapshot,
          request_deserializer=control__api_dot_snapshot__control__pb2.ReadSnapshotReq.FromString,
          response_serializer=control__api_dot_snapshot__control__pb2.ReadSnapshotAck.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'huawei.proto.control.SnapshotControl', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
