#include "../log/log.h"
#include "volume_attr.h"
using huawei::proto::REP_STATUS;
using huawei::proto::REP_ROLE;
using huawei::proto::VOLUME_STATUS;

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
    m_vol_info.CopyFrom(vol_info);
}

string VolumeAttr::vol_name() const
{
    return m_vol_info.vol_id();
}

size_t VolumeAttr::vol_size() const
{
    return m_vol_info.size();
}

string VolumeAttr::blk_device() const
{
    return m_vol_info.path();
}
 
bool VolumeAttr::is_snapshot_allowable(const SnapType& snap_type)
{
    /*todo: maybe sgclient should take volume in-use status*/
    if(m_vol_info.vol_status() != VOLUME_STATUS::VOL_AVAILABLE){
        /*volume not already available, can not create snapshot*/
        return false; 
    }
    
    if(m_vol_info.rep_status() == REP_STATUS::REP_ENABLED){
        if(m_vol_info.role() == REP_ROLE::REP_SECONDARY && 
           snap_type == SnapType::SNAP_LOCAL){
            /*replication enable, volume is slave, cann't create local snapshot*/
            return false; 
        }
    }

    if(m_vol_info.rep_status() != REP_STATUS::REP_ENABLED){
        if(snap_type == SnapType::SNAP_REMOTE){
            /*replication disable, remote snapshot no support*/
            return false; 
        } 
    }
    
    /*todo: handle failover and reverse*/
    return true;
}

bool VolumeAttr::is_append_entry_need(const SnapType& snap_type)
{
    if(m_vol_info.rep_status() == REP_STATUS::REP_ENABLED){
        if(m_vol_info.role() == REP_ROLE::REP_SECONDARY && 
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
    if(!m_vol_info.rep_enable()){
        /*replicate disable use normal_replay*/
        return NORMAL_REPLAY_MODE;
    }
    
    if(m_vol_info.role() == REP_ROLE::REP_PRIMARY && 
       (m_vol_info.rep_status() != REP_STATUS::REP_FAILED_OVER || 
        m_vol_info.rep_status() != REP_STATUS::REP_FAILING_OVER)){
        /*cur volume is primary, no failover occur on cur volume*/ 
        return NORMAL_REPLAY_MODE;
    }

    if(m_vol_info.role() == REP_ROLE::REP_SECONDARY && 
       (m_vol_info.rep_status() == REP_STATUS::REP_FAILED_OVER || 
        m_vol_info.rep_status() == REP_STATUS::REP_FAILING_OVER)){
        /*cur volume is secondary, failover occur on cur volume*/ 
        return NORMAL_REPLAY_MODE;
    }
    
    /*todo failover and reverse*/
    return REPLICA_REPLAY_MODE;
}

bool VolumeAttr::is_failover_occur()
{
    /* todo failover contain two case:
     * master crash: master can't get failover command, may be add manual work
     * plan migration: master can get failover command
     */
    if(m_vol_info.role() == REP_ROLE::REP_SECONDARY && 
       m_vol_info.rep_status() == REP_STATUS::REP_FAILED_OVER){
        return true;
    }
    return false;
}
