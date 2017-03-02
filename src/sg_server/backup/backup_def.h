#ifndef _BACKUP_TYPE_H
#define _BACKUP_TYPE_H
#include <string>
#include <set>
#include "rpc/common.pb.h"
#include "rpc/backup.pb.h"
using huawei::proto::BackupStatus;
using huawei::proto::BackupMode;
using huawei::proto::BackupType;
using namespace std;

/*block*/
typedef uint64_t block_t;
/*backup id*/
typedef uint64_t backupid_t;
/*backup data object name*/
typedef string backup_object_t;

/*backup attribution*/
struct backup_attr 
{
    string volume_uuid;
   
    BackupMode    backup_mode;
    string        backup_name;
    backupid_t    backup_id;
    BackupStatus  backup_status;
    BackupType    backup_type;
};
typedef struct backup_attr backup_attr_t;

#define BACKUP_META "/backup"

#define BACKUP_FS "#"
#define BACKUP_OBJ_SUFFIX ".backupobj"

#define BACKUP_INIT_UUID (2222)

/*backup indexstore key prefix*/
#define BACKUP_ID_PREFIX      "backup_latestid"
#define BACKUP_MAP_PREFIX     "backup_map_prefix"
#define BACKUP_BLOCK_PREFIX   "backup_block_prefix"

#endif
