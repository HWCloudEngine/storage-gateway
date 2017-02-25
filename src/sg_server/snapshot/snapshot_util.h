#ifndef __SNAPSHOT_UTIL_H
#define __SNAPSHOT_UTIL_H
#include <string>
#include "common/define.h"
#include "snapshot_def.h"
using namespace std;

/*helper function to handle db persist key*/
class DbUtil
{
public:
    /*common snapshot db persist relevant*/
    static string spawn_key(const string& prefix, const string& value);
    static void   split_key(const string& raw, string& prefix, string& key); 
    static string spawn_latest_id_key();
    static string spawn_latest_name_key();
    static string spawn_attr_map_key(const string& snap_name);
    static void   split_attr_map_key(const string& raw_key, string& snap_name);
    static string spawn_attr_map_val(const snap_attr_t& snap_attr);
    static void   split_attr_map_val(const string& raw_key, snap_attr_t& snap_attr);
    static string spawn_cow_block_map_key(const snapid_t& snap_id,
                                          const block_t& block_id);
    static void   split_cow_block_map_key(const string& raw_key, 
                                          snapid_t& snap_id, 
                                          block_t& block_id);
    static string spawn_cow_object_map_key(const string& obj_name);
    static void   split_cow_object_map_key(const string& raw_key, string& obj_name);
    static string spawn_cow_object_map_val(const cow_object_ref_t& obj_ref);
    static void   split_cow_object_map_val(const string& raw_val, 
                                           cow_object_ref_t& obj_ref);
};

#endif
