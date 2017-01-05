# storage-gateway  
# installation  
1. deploy ceph environment  
`to be supplemented`  
2. compile and install grpc and protobuf  
`apt-get update`  
`apt-get install build-essential autoconf libtool git unzip pkg-config`  
`git clone -b $(curl -L http://grpc.io/release) https://github.com/grpc/grpc`  
`cd grpc`  
`git submodule update --init`  
`make -j8`  
`make install`  
`cd third_party/protobuf`  
`make -j8`  
`make install`  
`cd ../../`  
`ldconfig`  
3. compile storage-gateway source code  
`apt-get install libboost-all-dev libcurl3-nss`  
`apt-get install librados-dev`  
`apt-get install libgflags-dev`  
`apt-get install libsnappy-dev`  
`apt-get install zlib1g-dev`  
`apt-get install libbz2-dev`  
`git clone https://github.com/facebook/rocksdb.git`  
`cd rocksdb`  
`make static_lib`  
`make install`  
`git clone https://github.com/Hybrid-Cloud/storage-gateway.git`  
`cd storage-gateway`  
`./build.sh`  
4. compile tgt source code  
`apt-get install tgt xsltproc`  
`apt-get install lttng-tools`  
`apt-get install lttng-modules-dkms`  
`apt-get install liblttng-ust-dev`  
`git clone -b iohook https://github.com/Hybrid-Cloud/tgt.git`  
`cd tgt`  
`make -j8`  
`make install`  
5. prepare cephfs direcotry to store journal files  
prepare mount point  
`mkdir -p /mnt/cephfs`  
create cephfs  
`ceph osd pool create cephfs_data 128`  
`ceph osd pool create cephfs_metadata 128`  
`ceph fs new cephfs cephfs_metadata cephfs_data`  
mount cephfs on mount point, `admin_key` get from `/etc/ceph/ceph.client.admin.keyring`  
`mount -t ceph ceph_fs_ip:ceph_fs_port:/ /mnt/cephfs -o name=admin,secret=admin_key`  
6. configure storage gateway ini file  
create a S3 user and generate `access_key` and `secret_key` for later use  
`radosgw-admin user create --uid={username} --display-name={display-name}`  
create and edit /etc/storage-gateway/config.ini  
`[journal_meta_storage]`  
`type=ceph_s3`  
`[ceph_s3]`  
`secret_key=xxxxx`  
`access_key=xxxxxx`  
`host=ceph-node1:7480`  
`bucket=journals_bucket`  
`[journal_storage]`  
`type=ceph_fs`  
`[ceph_fs]`  
`mount_point=/mnt/cephfs`  
`[meta_server]`  
`port=50051`  
`ip=x.x.x.x`  
`[volumes]`  
`primary=xxx`  
`secondary=xxx`  
`[replicate]`  
`local_ip=x.x.x.x`  
`remote_ip=x.x.x.x`  
`port=x.x.x.x`  
7. start rpc server, which manage all journal files meta data  
`cd storage-gateway/src/sg_server`  
`cp storage-gateway/lib/libs3.so /lib/libs3.so.trunk0`  
`./rpc_server &`  
8. start journal server  
`cd storage-gateway/src/journal_writer`  
`./journal_server &`  
9. configure tgt target  
create and edit /etc/tgt/targets.conf  
`include /etc/tgt/config.d/*conf`  
`<target iqn.2016.xxx.com.test>`  
`bs-type hijacker`  
`bsopts "host=journal_server_ip\;port=journal_server_port\;volume=volume_name\;device=block_device_path"`  
`backing-store block_device_path`  
`allow-in-use yes`  
`</target>`  
10. start iscsi target tgtd  
`service tgt start`  
exceptions on linux kernel version later than 4.0, solution as follow:  
`/etc/init.d/tgt` script no work, replace it with tgt scrpit provided with tgt.1.0.63  
use systemd to control tgt service `systemctl start|stop|status tgt.service`  
11. start iscsi initiator on another host  
install iscsi initiator  
`apt-get install open-iscsi`  
edit /etc/iscsi/iscsid.conf and change following  
`node.startup=automatic`  
iscsi initiator discover iscsi target  
`iscsiadm -m discovery -t st -p target_host_ip`  
iscsi initiator login  
`iscsiadm -m node -p target_host_ip --login`  
iscsi initiator logout  
`iscsiadm -m node -p target_host_op --logout`  

