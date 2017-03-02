#ifndef BACKUP_CTX_H
#define BACKUP_CTX_H
#include <string>
#include <mutex>
#include <map>
#include "common/block_store.h"
#include "common/index_store.h"
#include "rpc/clients/snapshot_ctrl_client.h"
#include "backup_def.h"

using namespace std;

class BackupCtx
{
public:
	BackupCtx() = default;
	explicit BackupCtx(const string& vol_name, const size_t& vol_size);
	~BackupCtx();

	string vol_name()const;
	size_t vol_size()const;

	backupid_t set_latest_backup_id(const backupid_t& backup_id);
	backupid_t latest_backup_id()const;
	
	void set_cur_backup_name(const string& cur_bakcup);
	string cur_backup_name()const;

	map<string, backup_attr_t>& cur_backups_map();
	map<backupid_t, map<block_t, backup_object_t>>& cur_blocks_map();

	IndexStore* index_store() const;
	BlockStore* block_store()const;

	SnapshotCtrlClient* snap_client()const;
	
    bool is_backup_exist(const string& cur_backup);
    bool is_incr_backup_allowable();
    bool is_backup_deletable(const string& cur_backup);

    backupid_t get_backup_id(const string& cur_backup);
    string get_backup_name(const backupid_t& cur_backup_id);
    BackupMode get_backup_mode(const string& cur_backup);
    BackupStatus get_backup_status(const string& cur_backup);

    /*the latest full backup in system*/
    string get_latest_full_backup();
    /*give a backup, get the base backup of the backup*/
    string get_backup_base(const string& cur_backup);

    /*according current backup name to get prev/next backup name*/
    string get_prev_backup(const string& cur_backup);
    string get_next_backup(const string& cur_backup);

    backupid_t spawn_backup_id();

	/*debug*/
    void trace();
		
private:
	/*volume basic*/
    string m_vol_name;
    size_t m_vol_size;

    /*lock*/
    recursive_mutex m_mutex;

    /*backup basic*/
    backupid_t m_latest_backup_id;
    /*current op backup*/
    string m_cur_backup;

    /*backup and attr map*/
    map<string, backup_attr_t> m_backups;
    /*backup id and backup block map*/
    map<backupid_t, map<block_t, backup_object_t>> m_backup_block_map;

    /*index store for backup meta*/
    IndexStore* m_index_store;
    /*block store for backup data*/
    BlockStore* m_block_store;

    /*snapshot client for reading incremental data and metadata */
    SnapshotCtrlClient* m_snap_client;
};

#endif
