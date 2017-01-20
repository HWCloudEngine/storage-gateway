#include "../log/log.h"
#include "snapshot_util.h"

string DbUtil::spawn_key(const string& prefix, const string& value)
{
    string key = prefix;
    key.append(FS);
    key.append(value);
    return key;
}

void DbUtil::split_key(const string& raw_key, string& prefix, string& key)
{
    size_t pos = raw_key.find(FS, 0);
    prefix = raw_key.substr(0, pos);
    key = raw_key.substr(pos+1, raw_key.length());
}

string DbUtil::spawn_latest_id_key()
{
    return spawn_key(SNAPSHOT_ID_PREFIX, "");
}

string DbUtil::spawn_latest_name_key()
{
    return spawn_key(SNAPSHOT_NAME_PREFIX, "");
}

string DbUtil::spawn_attr_map_key(const string& snap_name)
{
    return spawn_key(SNAPSHOT_MAP_PREFIX, snap_name);
}

void DbUtil::split_attr_map_key(const string& raw_key, string& snap_name)
{
    string prefix;
    split_key(raw_key, prefix, snap_name); 
}

string DbUtil::spawn_attr_map_val(const snap_attr_t& snap_attr)
{
    string key;
    key += snap_attr.replication_uuid;
    key.append(FS);
    key += snap_attr.checkpoint_uuid;
    key.append(FS);
    key += snap_attr.volume_uuid;
    key.append(FS);
    key += to_string(snap_attr.snap_type);
    key.append(FS);
    key += snap_attr.snap_name;
    key.append(FS);
    key += to_string(snap_attr.snap_id);
    key.append(FS);
    key += to_string(snap_attr.snap_status);
    return key;
}

/*todo: use json or protocol buffer*/
void DbUtil::split_attr_map_val(const string& raw_key, snap_attr_t& snap_attr)
{
    LOG_INFO << "attr val:" << raw_key;
    string rep_uuid;
    string remain0;
    split_key(raw_key, rep_uuid, remain0);
    snap_attr.replication_uuid = rep_uuid;

    string ckp_uuid; 
    string remain1;
    split_key(remain0, ckp_uuid, remain1);
    snap_attr.checkpoint_uuid = ckp_uuid;

    string vol_uuid;
    string remain2;
    split_key(remain1, vol_uuid, remain2);
    snap_attr.volume_uuid = vol_uuid;

    string snap_type;
    string remain3;
    split_key(remain2, snap_type, remain3);
    snap_attr.snap_type = (snap_type_t)atoi(snap_type.c_str());

    string snap_name;
    string remain4;
    split_key(remain3, snap_name, remain4);
    snap_attr.snap_name = snap_name;

    string snap_id;
    string remain5;
    split_key(remain4, snap_id, remain5);
    snap_attr.snap_id = (snapid_t)atoi(snap_id.c_str());

    string snap_status;
    string remain6;
    split_key(remain5, snap_status, remain6);
    snap_attr.snap_status = (SnapStatus)atoi(snap_status.c_str());
}
 
string DbUtil::spawn_cow_block_map_key(const snapid_t& snap_id,
                                       const block_t& block_id)
{
   string key = to_string(snap_id);
   key.append(FS);
   key += to_string(block_id);
   return spawn_key(SNAPSHOT_COWBLOCK_PREFIX, key);
}

void DbUtil::split_cow_block_map_key(const string& raw_key, 
                                     snapid_t& snap_id, 
                                     block_t& block_id)
{
    string prefix;
    string key;
    split_key(raw_key, prefix, key);

    string snap;
    string block;
    split_key(key, snap, block);

    snap_id = atol(snap.c_str());
    block_id = atol(block.c_str());
}

string DbUtil::spawn_cow_object_map_key(const string& obj_name)
{
    return spawn_key(SNAPSHOT_COWOBJECT_PREFIX, obj_name);
}

void DbUtil::split_cow_object_map_key(const string& raw_key, 
                                      string& obj_name)
{
   string prefix;
   split_key(raw_key, prefix, obj_name);
}

string DbUtil::spawn_cow_object_map_val(const cow_object_ref_t& obj_ref)
{
    string val;
    for(auto it : obj_ref){
        val.append(to_string(it));
        val.append(FS);
    }
    return val;
}

void DbUtil::split_cow_object_map_val(const string& raw_val, 
                                      cow_object_ref_t& obj_ref)
{
    string raw = raw_val;
    size_t pos = raw.find(FS);
    while(pos != string::npos){
        string   snap_str = raw.substr(0, pos);
        snapid_t snap_id  = atol(snap_str.c_str());
        obj_ref.insert(snap_id);

        raw = raw.substr(pos+1);
        pos = raw.find(FS); 
    } 
}
