import grpc
from grpc.framework.common import cardinality
from grpc.framework.interfaces.face import utilities as face_utilities

import control_api.backup_control_pb2 as control__api_dot_backup__control__pb2
import control_api.backup_control_pb2 as control__api_dot_backup__control__pb2
import control_api.backup_control_pb2 as control__api_dot_backup__control__pb2
import control_api.backup_control_pb2 as control__api_dot_backup__control__pb2
import control_api.backup_control_pb2 as control__api_dot_backup__control__pb2
import control_api.backup_control_pb2 as control__api_dot_backup__control__pb2
import control_api.backup_control_pb2 as control__api_dot_backup__control__pb2
import control_api.backup_control_pb2 as control__api_dot_backup__control__pb2
import control_api.backup_control_pb2 as control__api_dot_backup__control__pb2
import control_api.backup_control_pb2 as control__api_dot_backup__control__pb2


class BackupControlStub(object):

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.CreateBackup = channel.unary_unary(
        '/huawei.proto.control.BackupControl/CreateBackup',
        request_serializer=control__api_dot_backup__control__pb2.CreateBackupReq.SerializeToString,
        response_deserializer=control__api_dot_backup__control__pb2.CreateBackupAck.FromString,
        )
    self.DeleteBackup = channel.unary_unary(
        '/huawei.proto.control.BackupControl/DeleteBackup',
        request_serializer=control__api_dot_backup__control__pb2.DeleteBackupReq.SerializeToString,
        response_deserializer=control__api_dot_backup__control__pb2.DeleteBackupAck.FromString,
        )
    self.RestoreBackup = channel.unary_unary(
        '/huawei.proto.control.BackupControl/RestoreBackup',
        request_serializer=control__api_dot_backup__control__pb2.RestoreBackupReq.SerializeToString,
        response_deserializer=control__api_dot_backup__control__pb2.RestoreBackupAck.FromString,
        )
    self.GetBackup = channel.unary_unary(
        '/huawei.proto.control.BackupControl/GetBackup',
        request_serializer=control__api_dot_backup__control__pb2.GetBackupReq.SerializeToString,
        response_deserializer=control__api_dot_backup__control__pb2.GetBackupAck.FromString,
        )
    self.ListBackup = channel.unary_unary(
        '/huawei.proto.control.BackupControl/ListBackup',
        request_serializer=control__api_dot_backup__control__pb2.ListBackupReq.SerializeToString,
        response_deserializer=control__api_dot_backup__control__pb2.ListBackupAck.FromString,
        )


class BackupControlServicer(object):

  def CreateBackup(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def DeleteBackup(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def RestoreBackup(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def GetBackup(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def ListBackup(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_BackupControlServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'CreateBackup': grpc.unary_unary_rpc_method_handler(
          servicer.CreateBackup,
          request_deserializer=control__api_dot_backup__control__pb2.CreateBackupReq.FromString,
          response_serializer=control__api_dot_backup__control__pb2.CreateBackupAck.SerializeToString,
      ),
      'DeleteBackup': grpc.unary_unary_rpc_method_handler(
          servicer.DeleteBackup,
          request_deserializer=control__api_dot_backup__control__pb2.DeleteBackupReq.FromString,
          response_serializer=control__api_dot_backup__control__pb2.DeleteBackupAck.SerializeToString,
      ),
      'RestoreBackup': grpc.unary_unary_rpc_method_handler(
          servicer.RestoreBackup,
          request_deserializer=control__api_dot_backup__control__pb2.RestoreBackupReq.FromString,
          response_serializer=control__api_dot_backup__control__pb2.RestoreBackupAck.SerializeToString,
      ),
      'GetBackup': grpc.unary_unary_rpc_method_handler(
          servicer.GetBackup,
          request_deserializer=control__api_dot_backup__control__pb2.GetBackupReq.FromString,
          response_serializer=control__api_dot_backup__control__pb2.GetBackupAck.SerializeToString,
      ),
      'ListBackup': grpc.unary_unary_rpc_method_handler(
          servicer.ListBackup,
          request_deserializer=control__api_dot_backup__control__pb2.ListBackupReq.FromString,
          response_serializer=control__api_dot_backup__control__pb2.ListBackupAck.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'huawei.proto.control.BackupControl', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
