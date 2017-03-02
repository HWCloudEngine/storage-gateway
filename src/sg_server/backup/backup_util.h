#ifndef BACKUP_UTIL_H
#define BACKUP_UTIL_H
#include "backup_def.h"

/*helper function to handle db persist key*/
string spawn_key(const string& prefix, const string& value);
void   split_key(const string& raw, string& prefix, string& key); 

string spawn_latest_backup_id_key();
string spawn_latest_backup_name_key();

string spawn_backup_attr_map_key(const string& backup_name);
void   split_backup_attr_map_key(const string& raw_key, string& backup_name);
string spawn_backup_attr_map_val(const backup_attr_t& backup_attr);
void   split_backup_attr_map_val(const string& raw_key, backup_attr_t& backup_attr);

string spawn_backup_block_map_key(const backupid_t & backup_id, const block_t& block_id);
void   split_backup_block_map_key(const string& raw_key, backupid_t& backup_id, block_t& block_id);

string spawn_backup_object_name(const string& vol_name, const backupid_t& backup_id, 
                                const block_t& blk_id);

#endif
