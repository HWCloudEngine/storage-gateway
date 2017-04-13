# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: snapshot.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import common_pb2 as common__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='snapshot.proto',
  package='huawei.proto',
  syntax='proto3',
  serialized_pb=_b('\n\x0esnapshot.proto\x12\x0chuawei.proto\x1a\x0c\x63ommon.proto\"\xa3\x01\n\x0bSnapReqHead\x12\x0e\n\x06seq_id\x18\x01 \x01(\x03\x12&\n\x05scene\x18\x02 \x01(\x0e\x32\x17.huawei.proto.SnapScene\x12)\n\tsnap_type\x18\x03 \x01(\x0e\x32\x16.huawei.proto.SnapType\x12\x18\n\x10replication_uuid\x18\x04 \x01(\t\x12\x17\n\x0f\x63heckpoint_uuid\x18\x05 \x01(\t\"G\n\x0bSnapAckHead\x12\x0e\n\x06seq_id\x18\x01 \x01(\x03\x12(\n\x06status\x18\x02 \x01(\x0e\x32\x18.huawei.proto.StatusCode\"H\n\nDiffBlocks\x12\x10\n\x08vol_name\x18\x01 \x01(\t\x12\x11\n\tsnap_name\x18\x02 \x01(\t\x12\x15\n\rdiff_block_no\x18\x03 \x03(\x04*^\n\tSnapScene\x12\x0e\n\nFOR_NORMAL\x10\x00\x12\x0e\n\nFOR_BACKUP\x10\x02\x12\x13\n\x0f\x46OR_REPLICATION\x10\x03\x12\x1c\n\x18\x46OR_REPLICATION_FAILOVER\x10\x04*+\n\x08SnapType\x12\x0e\n\nSNAP_LOCAL\x10\x00\x12\x0f\n\x0bSNAP_REMOTE\x10\x01*\x93\x01\n\nSnapStatus\x12\x11\n\rSNAP_CREATING\x10\x00\x12\x10\n\x0cSNAP_CREATED\x10\x01\x12\x11\n\rSNAP_DELETING\x10\x02\x12\x10\n\x0cSNAP_DELETED\x10\x03\x12\x14\n\x10SNAP_ROLLBACKING\x10\x04\x12\x13\n\x0fSNAP_ROLLBACKED\x10\x05\x12\x10\n\x0cSNAP_INVALID\x10\nb\x06proto3')
  ,
  dependencies=[common__pb2.DESCRIPTOR,])
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

_SNAPSCENE = _descriptor.EnumDescriptor(
  name='SnapScene',
  full_name='huawei.proto.SnapScene',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='FOR_NORMAL', index=0, number=0,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='FOR_BACKUP', index=1, number=2,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='FOR_REPLICATION', index=2, number=3,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='FOR_REPLICATION_FAILOVER', index=3, number=4,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=359,
  serialized_end=453,
)
_sym_db.RegisterEnumDescriptor(_SNAPSCENE)

SnapScene = enum_type_wrapper.EnumTypeWrapper(_SNAPSCENE)
_SNAPTYPE = _descriptor.EnumDescriptor(
  name='SnapType',
  full_name='huawei.proto.SnapType',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='SNAP_LOCAL', index=0, number=0,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='SNAP_REMOTE', index=1, number=1,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=455,
  serialized_end=498,
)
_sym_db.RegisterEnumDescriptor(_SNAPTYPE)

SnapType = enum_type_wrapper.EnumTypeWrapper(_SNAPTYPE)
_SNAPSTATUS = _descriptor.EnumDescriptor(
  name='SnapStatus',
  full_name='huawei.proto.SnapStatus',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='SNAP_CREATING', index=0, number=0,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='SNAP_CREATED', index=1, number=1,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='SNAP_DELETING', index=2, number=2,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='SNAP_DELETED', index=3, number=3,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='SNAP_ROLLBACKING', index=4, number=4,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='SNAP_ROLLBACKED', index=5, number=5,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='SNAP_INVALID', index=6, number=10,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=501,
  serialized_end=648,
)
_sym_db.RegisterEnumDescriptor(_SNAPSTATUS)

SnapStatus = enum_type_wrapper.EnumTypeWrapper(_SNAPSTATUS)
FOR_NORMAL = 0
FOR_BACKUP = 2
FOR_REPLICATION = 3
FOR_REPLICATION_FAILOVER = 4
SNAP_LOCAL = 0
SNAP_REMOTE = 1
SNAP_CREATING = 0
SNAP_CREATED = 1
SNAP_DELETING = 2
SNAP_DELETED = 3
SNAP_ROLLBACKING = 4
SNAP_ROLLBACKED = 5
SNAP_INVALID = 10



_SNAPREQHEAD = _descriptor.Descriptor(
  name='SnapReqHead',
  full_name='huawei.proto.SnapReqHead',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='seq_id', full_name='huawei.proto.SnapReqHead.seq_id', index=0,
      number=1, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='scene', full_name='huawei.proto.SnapReqHead.scene', index=1,
      number=2, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='snap_type', full_name='huawei.proto.SnapReqHead.snap_type', index=2,
      number=3, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='replication_uuid', full_name='huawei.proto.SnapReqHead.replication_uuid', index=3,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='checkpoint_uuid', full_name='huawei.proto.SnapReqHead.checkpoint_uuid', index=4,
      number=5, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=47,
  serialized_end=210,
)


_SNAPACKHEAD = _descriptor.Descriptor(
  name='SnapAckHead',
  full_name='huawei.proto.SnapAckHead',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='seq_id', full_name='huawei.proto.SnapAckHead.seq_id', index=0,
      number=1, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='status', full_name='huawei.proto.SnapAckHead.status', index=1,
      number=2, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=212,
  serialized_end=283,
)


_DIFFBLOCKS = _descriptor.Descriptor(
  name='DiffBlocks',
  full_name='huawei.proto.DiffBlocks',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='vol_name', full_name='huawei.proto.DiffBlocks.vol_name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='snap_name', full_name='huawei.proto.DiffBlocks.snap_name', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='diff_block_no', full_name='huawei.proto.DiffBlocks.diff_block_no', index=2,
      number=3, type=4, cpp_type=4, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=285,
  serialized_end=357,
)

_SNAPREQHEAD.fields_by_name['scene'].enum_type = _SNAPSCENE
_SNAPREQHEAD.fields_by_name['snap_type'].enum_type = _SNAPTYPE
_SNAPACKHEAD.fields_by_name['status'].enum_type = common__pb2._STATUSCODE
DESCRIPTOR.message_types_by_name['SnapReqHead'] = _SNAPREQHEAD
DESCRIPTOR.message_types_by_name['SnapAckHead'] = _SNAPACKHEAD
DESCRIPTOR.message_types_by_name['DiffBlocks'] = _DIFFBLOCKS
DESCRIPTOR.enum_types_by_name['SnapScene'] = _SNAPSCENE
DESCRIPTOR.enum_types_by_name['SnapType'] = _SNAPTYPE
DESCRIPTOR.enum_types_by_name['SnapStatus'] = _SNAPSTATUS

SnapReqHead = _reflection.GeneratedProtocolMessageType('SnapReqHead', (_message.Message,), dict(
  DESCRIPTOR = _SNAPREQHEAD,
  __module__ = 'snapshot_pb2'
  # @@protoc_insertion_point(class_scope:huawei.proto.SnapReqHead)
  ))
_sym_db.RegisterMessage(SnapReqHead)

SnapAckHead = _reflection.GeneratedProtocolMessageType('SnapAckHead', (_message.Message,), dict(
  DESCRIPTOR = _SNAPACKHEAD,
  __module__ = 'snapshot_pb2'
  # @@protoc_insertion_point(class_scope:huawei.proto.SnapAckHead)
  ))
_sym_db.RegisterMessage(SnapAckHead)

DiffBlocks = _reflection.GeneratedProtocolMessageType('DiffBlocks', (_message.Message,), dict(
  DESCRIPTOR = _DIFFBLOCKS,
  __module__ = 'snapshot_pb2'
  # @@protoc_insertion_point(class_scope:huawei.proto.DiffBlocks)
  ))
_sym_db.RegisterMessage(DiffBlocks)


try:
  # THESE ELEMENTS WILL BE DEPRECATED.
  # Please use the generated *_pb2_grpc.py files instead.
  import grpc
  from grpc.framework.common import cardinality
  from grpc.framework.interfaces.face import utilities as face_utilities
  from grpc.beta import implementations as beta_implementations
  from grpc.beta import interfaces as beta_interfaces
except ImportError:
  pass
# @@protoc_insertion_point(module_scope)
