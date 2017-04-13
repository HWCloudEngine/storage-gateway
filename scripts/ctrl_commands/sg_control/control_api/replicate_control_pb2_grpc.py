import grpc
from grpc.framework.common import cardinality
from grpc.framework.interfaces.face import utilities as face_utilities

import control_api.replicate_control_pb2 as control__api_dot_replicate__control__pb2
import control_api.replicate_control_pb2 as control__api_dot_replicate__control__pb2
import control_api.replicate_control_pb2 as control__api_dot_replicate__control__pb2
import control_api.replicate_control_pb2 as control__api_dot_replicate__control__pb2
import control_api.replicate_control_pb2 as control__api_dot_replicate__control__pb2
import control_api.replicate_control_pb2 as control__api_dot_replicate__control__pb2
import control_api.replicate_control_pb2 as control__api_dot_replicate__control__pb2
import control_api.replicate_control_pb2 as control__api_dot_replicate__control__pb2
import control_api.replicate_control_pb2 as control__api_dot_replicate__control__pb2
import control_api.replicate_control_pb2 as control__api_dot_replicate__control__pb2
import control_api.replicate_control_pb2 as control__api_dot_replicate__control__pb2
import control_api.replicate_control_pb2 as control__api_dot_replicate__control__pb2


class ReplicateControlStub(object):
  """northern oriented rpc service interface
  """

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.CreateReplication = channel.unary_unary(
        '/huawei.proto.control.ReplicateControl/CreateReplication',
        request_serializer=control__api_dot_replicate__control__pb2.CreateReplicationReq.SerializeToString,
        response_deserializer=control__api_dot_replicate__control__pb2.ReplicationCommonRes.FromString,
        )
    self.EnableReplication = channel.unary_unary(
        '/huawei.proto.control.ReplicateControl/EnableReplication',
        request_serializer=control__api_dot_replicate__control__pb2.EnableReplicationReq.SerializeToString,
        response_deserializer=control__api_dot_replicate__control__pb2.ReplicationCommonRes.FromString,
        )
    self.DisableReplication = channel.unary_unary(
        '/huawei.proto.control.ReplicateControl/DisableReplication',
        request_serializer=control__api_dot_replicate__control__pb2.DisableReplicationReq.SerializeToString,
        response_deserializer=control__api_dot_replicate__control__pb2.ReplicationCommonRes.FromString,
        )
    self.FailoverReplication = channel.unary_unary(
        '/huawei.proto.control.ReplicateControl/FailoverReplication',
        request_serializer=control__api_dot_replicate__control__pb2.FailoverReplicationReq.SerializeToString,
        response_deserializer=control__api_dot_replicate__control__pb2.ReplicationCommonRes.FromString,
        )
    self.ReverseReplication = channel.unary_unary(
        '/huawei.proto.control.ReplicateControl/ReverseReplication',
        request_serializer=control__api_dot_replicate__control__pb2.ReverseReplicationReq.SerializeToString,
        response_deserializer=control__api_dot_replicate__control__pb2.ReplicationCommonRes.FromString,
        )
    self.DeleteReplication = channel.unary_unary(
        '/huawei.proto.control.ReplicateControl/DeleteReplication',
        request_serializer=control__api_dot_replicate__control__pb2.DeleteReplicationReq.SerializeToString,
        response_deserializer=control__api_dot_replicate__control__pb2.ReplicationCommonRes.FromString,
        )


class ReplicateControlServicer(object):
  """northern oriented rpc service interface
  """

  def CreateReplication(self, request, context):
    """replicate control operations
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def EnableReplication(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def DisableReplication(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def FailoverReplication(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def ReverseReplication(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def DeleteReplication(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_ReplicateControlServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'CreateReplication': grpc.unary_unary_rpc_method_handler(
          servicer.CreateReplication,
          request_deserializer=control__api_dot_replicate__control__pb2.CreateReplicationReq.FromString,
          response_serializer=control__api_dot_replicate__control__pb2.ReplicationCommonRes.SerializeToString,
      ),
      'EnableReplication': grpc.unary_unary_rpc_method_handler(
          servicer.EnableReplication,
          request_deserializer=control__api_dot_replicate__control__pb2.EnableReplicationReq.FromString,
          response_serializer=control__api_dot_replicate__control__pb2.ReplicationCommonRes.SerializeToString,
      ),
      'DisableReplication': grpc.unary_unary_rpc_method_handler(
          servicer.DisableReplication,
          request_deserializer=control__api_dot_replicate__control__pb2.DisableReplicationReq.FromString,
          response_serializer=control__api_dot_replicate__control__pb2.ReplicationCommonRes.SerializeToString,
      ),
      'FailoverReplication': grpc.unary_unary_rpc_method_handler(
          servicer.FailoverReplication,
          request_deserializer=control__api_dot_replicate__control__pb2.FailoverReplicationReq.FromString,
          response_serializer=control__api_dot_replicate__control__pb2.ReplicationCommonRes.SerializeToString,
      ),
      'ReverseReplication': grpc.unary_unary_rpc_method_handler(
          servicer.ReverseReplication,
          request_deserializer=control__api_dot_replicate__control__pb2.ReverseReplicationReq.FromString,
          response_serializer=control__api_dot_replicate__control__pb2.ReplicationCommonRes.SerializeToString,
      ),
      'DeleteReplication': grpc.unary_unary_rpc_method_handler(
          servicer.DeleteReplication,
          request_deserializer=control__api_dot_replicate__control__pb2.DeleteReplicationReq.FromString,
          response_serializer=control__api_dot_replicate__control__pb2.ReplicationCommonRes.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'huawei.proto.control.ReplicateControl', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
