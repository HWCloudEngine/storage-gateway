#ifndef SNAP_DEF_H
#define SNAP_DEF_H
#include <string>
#include <set>
#include "rpc/snapshot.pb.h"
using huawei::proto::SnapStatus;
using huawei::proto::SnapType;
using namespace std;

/*snapshot id*/
typedef uint64_t snapid_t;

/*cow data object name*/
typedef string cow_object_t;
/*cow data object snapshot reference list*/
typedef set<snapid_t> cow_object_ref_t;

/*snapshot attribution*/
struct snap_attr 
{
    string replication_uuid;
    string checkpoint_uuid;
    string volume_uuid;

    SnapType    snap_type;
    string      snap_name;
    snapid_t    snap_id;
    SnapStatus  snap_status;
};
typedef struct snap_attr snap_attr_t;

/*snapshot meta store path*/
#define SNAPSHOT_META  "/snapshot"

/*use spawn cow object name*/
#define FS  "@"
#define OBJ_SUFFIX ".obj"

/*snapshot indexstore key prefix*/
#define SNAPSHOT_ID_PREFIX           "snapshot_latestid"
#define SNAPSHOT_NAME_PREFIX         "snapshot_latestname"
#define SNAPSHOT_MAP_PREFIX           "snapshot_table_prefix"
#define SNAPSHOT_STATUS_PREFIX        "snapshot_status_prefix"
#define SNAPSHOT_COWBLOCK_PREFIX      "snapshot_cowblock_prefix"
#define SNAPSHOT_COWOBJECT_PREFIX     "snapshot_cowobject_prefix"

#endif
