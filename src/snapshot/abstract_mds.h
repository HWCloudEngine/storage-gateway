#ifndef _ABSTRACT_MDS_H
#define _ABSTRACT_MDS_H
#include <string>
#include <mutex>
#include <grpc++/grpc++.h>
#include "../rpc/common.pb.h"
#include "../rpc/snapshot_inner_control.pb.h"
#include "../rpc/snapshot_inner_control.grpc.pb.h"

using huawei::proto::StatusCode;

using huawei::proto::inner::CreateReq;
using huawei::proto::inner::CreateAck;
using huawei::proto::inner::ListReq;
using huawei::proto::inner::ListAck;
using huawei::proto::inner::QueryReq;
using huawei::proto::inner::QueryAck;
using huawei::proto::inner::DeleteReq;
using huawei::proto::inner::DeleteAck;
using huawei::proto::inner::RollbackReq;
using huawei::proto::inner::RollbackAck;
using huawei::proto::inner::UpdateReq;
using huawei::proto::inner::UpdateAck;
using huawei::proto::inner::CowReq;
using huawei::proto::inner::CowAck;
using huawei::proto::inner::CowUpdateReq;
using huawei::proto::inner::CowUpdateAck;
using huawei::proto::inner::DiffReq;
using huawei::proto::inner::DiffAck;
using huawei::proto::inner::ReadReq;
using huawei::proto::inner::ReadAck;
using huawei::proto::inner::SyncReq;
using huawei::proto::inner::SyncAck;

class AbstractMds
{
public:
    /*snapshot common operation*/
    virtual StatusCode create_snapshot(const CreateReq* req, CreateAck* ack) = 0;
    virtual StatusCode delete_snapshot(const DeleteReq* req, DeleteAck* ack) = 0;
    virtual StatusCode rollback_snapshot(const RollbackReq* req, RollbackAck* ack) = 0;
    virtual StatusCode list_snapshot(const ListReq* req, ListAck* ack) = 0;
    virtual StatusCode query_snapshot(const QueryReq* req, QueryAck* ack) = 0;
    virtual StatusCode diff_snapshot(const DiffReq* req, DiffAck* ack) = 0;    
    virtual StatusCode read_snapshot(const ReadReq* req, ReadAck* ack) = 0;
    
    /*snapshot status*/
    virtual StatusCode update(const UpdateReq* req, UpdateAck* ack) = 0;
    
    /*cow*/
    virtual StatusCode cow_op(const CowReq* req, CowAck* ack) = 0;
    virtual StatusCode cow_update(const CowUpdateReq* req, CowUpdateAck* ack) = 0;
    
    /*crash recover*/
    virtual int recover() = 0;

};

#endif
