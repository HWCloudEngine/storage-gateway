#ifndef ITRANSACTION_H
#define ITRANSACTION_H
#include <string>
#include "rpc/common.pb.h"
#include "rpc/snapshot.pb.h"
using namespace std;
using huawei::proto::StatusCode;
using huawei::proto::SnapReqHead;

class ITransaction
{
public:
    ITransaction() = default;
    virtual ~ITransaction(){}

    virtual StatusCode create_transaction(const SnapReqHead& shead, const string& snap_name) = 0;
    virtual StatusCode delete_transaction(const SnapReqHead& shead, const string& snap_name) = 0;
    virtual StatusCode rollback_transaction(const SnapReqHead& shead, const string& snap_name) = 0;
};

#endif
