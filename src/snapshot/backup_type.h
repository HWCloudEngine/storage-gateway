#ifndef _BACKUP_TYPE_H
#define _BACKUP_TYPE_H

#include <string>
#include <set>
#include "../rpc/common.pb.h"
using huawei::proto::BackupStatus;
using huawei::proto::BackupMode;
using namespace std;

/*backup id*/
typedef uint64_t backupid_t;
/*backup data object name*/
typedef string backup_object_t;
/*backup data object backup reference list*/
typedef set<backupid_t> backup_object_ref_t;

/*backup attribution*/
struct bakcup_attr {
    string replication_uuid;
    string checkpoint_uuid;
    string volume_uuid;
    
    BakcupMode    backup_mode;
    string        bakcup_name;
    backupid_t    bakcup_id;
    BackupStatus  backup_status;
};
typedef struct backup_attr backup_attr_t;

/*backup chunk size(4MB)*/
#define DEFAULT_BACKUP_CHUNK_SIZE (4*1024*1024UL)

/*backup indexstore key prefix*/
#define BACKUP_ID_PREFIX      "backup_latestid"
#define BACKUP_NAME_PREFIX    "backup_latestname"
#define BACKUP_MAP_PREFIX     "backup_table_prefix"
#define BACKUP_STATUS_PREFIX  "backup_status_prefix"
#define BACKUP_BLOCK_PREFIX   "backup_block_prefix"
#define BACKUP_OBJECT_PREFIX  "backup_object_prefix"

#endif
