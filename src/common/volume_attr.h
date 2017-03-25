#ifndef VOL_ATTR_H
#define VOL_ATTR_H
#include <string>
#include "rpc/common.pb.h"
#include "rpc/snapshot.pb.h"
#include "rpc/backup.pb.h"
#include "rpc/volume.pb.h"
#include "locks.h"
using namespace std;
using huawei::proto::VolumeInfo;
using huawei::proto::SnapType;
using huawei::proto::BackupType;
using huawei::proto::RepRole;

class VolumeAttr
{
public:
    VolumeAttr() = default;
    VolumeAttr(const string& vol_name, const size_t& vol_size);
    VolumeAttr(const VolumeInfo& vol_info);
    ~VolumeAttr();

    void update(const VolumeInfo& vol_info);

    string vol_name();
    size_t vol_size();
    string blk_device();
    RepRole replicate_role();

    /*whether create snapshot allowable*/
    bool is_snapshot_allowable(const SnapType& snap_type);

    /* whether need insert snapshot journal entry
     * local snapshot: need insert journal entry on master
     * remote snapshot: noneed insert journal entry on slave
     */
    bool is_append_entry_need(const SnapType& snap_type);
    
    /* decide current adopt which replay mode
     * normal_replay: replay from cache
     * replica_replay: replay from journal file replicate from master
     */
    static constexpr int NORMAL_REPLAY_MODE = 1;
    static constexpr int REPLICA_REPLAY_MODE = 2;
    int current_replay_mode();

    /*true: fail over, false: no failover*/
    bool is_failover_occur();

    /*whether create backup allowable*/
    bool is_backup_allowable(const BackupType& backup_type);

private:
    VolumeInfo m_vol_info;
    SharedMutex mtx;
};

#endif
