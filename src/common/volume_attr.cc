#include "../log/log.h"
#include "volume_attr.h"
using huawei::proto::RepStatus;
using huawei::proto::RepRole;
using huawei::proto::VolumeStatus;

VolumeAttr::VolumeAttr(const string& vol_name, const size_t& vol_size)
{
    m_vol_info.set_vol_id(vol_name);
    m_vol_info.set_size(vol_size);
}

VolumeAttr::VolumeAttr(const VolumeInfo& vol_info)
{
    m_vol_info.CopyFrom(vol_info);
}

VolumeAttr::~VolumeAttr()
{
    m_vol_info.Clear();
}

void VolumeAttr::update(const VolumeInfo& vol_info)
{
    LOG_INFO << m_vol_info.vol_id() << " attr changed:"
        << " vol_status:" << vol_info.vol_status()
        << ",rep_status:" << vol_info.rep_status()
        << ",role:" << vol_info.role();
    WriteLock lck(mtx);
    m_vol_info.CopyFrom(vol_info);
}

string VolumeAttr::vol_name()
{
    ReadLock lck(mtx);
    return m_vol_info.vol_id();
}

size_t VolumeAttr::vol_size()
{
    ReadLock lck(mtx);
    return m_vol_info.size();
}

string VolumeAttr::blk_device()
{
    ReadLock lck(mtx);
    return m_vol_info.path();
}

RepRole VolumeAttr::replicate_role()
{
    ReadLock lck(mtx);
    return m_vol_info.role();
}

RepStatus VolumeAttr::replicate_status(){
    ReadLock lck(mtx);
    return m_vol_info.rep_status();
}

void VolumeAttr::set_replicate_status(const RepStatus& status){
    WriteLock lck(mtx);
    m_vol_info.set_rep_status(status);
}

bool VolumeAttr::is_snapshot_allowable(const SnapType& snap_type)
{
    ReadLock lck(mtx);
    /*todo: maybe sgclient should take volume in-use status*/
    if(m_vol_info.vol_status() != VolumeStatus::VOL_AVAILABLE){
        /*volume not already available, can not create snapshot*/
        return false; 
    }
    
    if(m_vol_info.rep_status() == RepStatus::REP_ENABLED){
        if(m_vol_info.role() == RepRole::REP_SECONDARY && 
           snap_type == SnapType::SNAP_LOCAL){
            /*replication enable, volume is slave, cann't create local snapshot*/
            return false; 
        }
    }

    if(m_vol_info.rep_status() != RepStatus::REP_ENABLED
        && m_vol_info.rep_status() != RepStatus::REP_FAILING_OVER){
        if(snap_type == SnapType::SNAP_REMOTE){
            /*replication disable, remote snapshot no support*/
            /*create a remote snapshot on failover to speed up reprotection */
            return false; 
        } 
    }
    
    /*todo: handle failover and reverse*/
    return true;
}

bool VolumeAttr::is_append_entry_need(const SnapType& snap_type)
{
    ReadLock lck(mtx);
    if(m_vol_info.rep_status() == RepStatus::REP_ENABLED){
        if(m_vol_info.role() == RepRole::REP_SECONDARY && 
           snap_type == SnapType::SNAP_REMOTE){
            /*replication enable, volume is slave, create remote snapshot*/
            return false; 
        }
    }
    /*todo: handle failover and reverse*/
    return true;
}


int VolumeAttr::current_replay_mode()
{
    ReadLock lck(mtx);
    if(!m_vol_info.rep_enable()){
        /*replicate disable use normal_replay*/
        return NORMAL_REPLAY_MODE;
    }
    
    if(m_vol_info.role() == RepRole::REP_PRIMARY && 
       (m_vol_info.rep_status() != RepStatus::REP_FAILED_OVER || 
        m_vol_info.rep_status() != RepStatus::REP_FAILING_OVER)){
        /*cur volume is primary, no failover occur on cur volume*/ 
        return NORMAL_REPLAY_MODE;
    }

    if(m_vol_info.role() == RepRole::REP_SECONDARY && 
       (m_vol_info.rep_status() == RepStatus::REP_FAILED_OVER)){
        /*cur volume is secondary, failover occur on cur volume*/ 
        return NORMAL_REPLAY_MODE;
    }
    
    /*todo failover and reverse*/
    return REPLICA_REPLAY_MODE;
}

bool VolumeAttr::is_failover_occur()
{
    ReadLock lck(mtx);
    /* todo failover contain two case:
     * master crash: master can't get failover command, may be add manual work
     * plan migration: master can get failover command
     */
    if(m_vol_info.role() == RepRole::REP_SECONDARY && 
       m_vol_info.rep_status() == RepStatus::REP_FAILED_OVER){
        return true;
    }
    return false;
}

bool VolumeAttr::is_backup_allowable(const BackupType& backup_type)
{
    ReadLock lck(mtx);
    /*todo: maybe sgclient should take volume in-use status*/
    if(m_vol_info.vol_status() != VolumeStatus::VOL_AVAILABLE){
        /*volume not already available, can not create snapshot*/
        LOG_ERROR << "volume not available ";
        return false; 
    }

    /*todo: handle failover and reverse*/
    return true;
}

bool VolumeAttr::is_writable(){
    ReadLock lck(mtx);
    // primary and not failover
    if((m_vol_info.rep_status() != RepStatus::REP_FAILED_OVER
        || m_vol_info.rep_status() != RepStatus::REP_FAILING_OVER)
        && m_vol_info.role() == RepRole::REP_PRIMARY){
        return true;
    }
    // secondary and failedover
    if(m_vol_info.role() == RepRole::REP_SECONDARY && 
       m_vol_info.rep_status() == RepStatus::REP_FAILED_OVER){
        return true;
    }
    // default: consider volume as writable
    if(m_vol_info.role() == RepRole::REP_UNKNOWN)
        return true;

    return false;
}

