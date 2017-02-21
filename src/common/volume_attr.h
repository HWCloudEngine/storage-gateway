#ifndef VOL_ATTR_H
#define VOL_ATTR_H
#include <string>
#include "../rpc/common.pb.h"
#include "../rpc/snapshot.pb.h"

using namespace std;
using huawei::proto::VolumeInfo;
using huawei::proto::SnapType;

class VolumeAttr
{
public:
    VolumeAttr(const VolumeInfo& vol_info);
    ~VolumeAttr();

    void update(const VolumeInfo& vol_info);

    string vol_name() const;
    size_t vol_size() const;
    string blk_device() const;
    
    /*snapshot relevant*/
    /*whether create snapshot allowable*/
    bool is_snapshot_allowable(const SnapType& snap_type);
    /*whether need append snapshot journal entry*/
    bool is_append_entry_need(const SnapType& snap_type);
    
    /*replay relevant*/
    /*true: use normal_replay, false: use replica_replay*/
    bool decide_replay_mode();
    /*true: fail over, false: no failover*/
    bool is_failover_occur();

private:
    VolumeInfo m_vol_info;
};

#endif
