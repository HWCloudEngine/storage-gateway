# storage-gateway  
# installation  
1. deploy ceph environment  
`to be supplemented`  
2. compile and install grpc and protobuf  
`apt-get install build-essential autoconf libtool`  
`git clone -b $(curl -L http://grpc.io/release) https://github.com/grpc/grpc`  
`cd grpc`  
`git submodule update --init`  
`cd third_party/protobuf/`  
`make -j8`  
`make install`  
`cd ../..`  
`make -j8`  
`make install`  
3. compile storage-gateway source code  
`apt-get update`  
`apt-get install libboost-all-dev`  
`git clone https://github.com/Hybrid-Cloud/storage-gateway.git`  
`cd storage-gateway`  
`./build.sh`  
4. compile tgt source code  
`apt-get install tgt`  
`git clone https://github.com/Hybrid-Cloud/tgt.git`  
`cd tgt`  
`make -j8`  
`make install`  
5. prepare cephfs direcotry to store journal files  
`mkdir -p /mnt/cephfs`  
`mount -t ceph ceph_fs_ip:ceph_fs_port:/ /mnt/cephfs -o name=admin secret=admin_key`  
6. configure storage gateway ini file  
create a S3 user and generate `access_key` and `secret_key` for later use  
`radosgw-admin user create --uid={username}`  
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
`[grpc]`  
`server_port=50051`  
7. start rpc server, which manage all journal files meta data  
`cd storage-gateway/src/dr_server`  
`./rpc_server &`  
8. start journal server  
`cd storage-gateway/src/journal_writer`  
`./journal_server &`  
9. configure tgt target  
create and edit /etc/tgt/targets.conf  
`include /etc/tgt/config.d/*conf`  
`<target iqn.2016.xxx.com.test>`  
`bs_type hijacker`  
`backing-store /dev/sdc`  
`</target>`  
10. start iscsi target tgtd  
`service tgt start`  
11. start iscsi initiator on another host  
install iscsi initiator  
`apt-get install open-iscsi`  
edit /etc/iscsi/iscsid.conf and change following  
`node.startup=automatic`  
iscsi initiator discover iscsi target  
`iscsiadm -m discovery -t st -p target_host_ip`  
iscsi initiator login  
`iscsiadm -m node --login`  
iscsi initiator logout  
`iscsiadm -m node --logout`  

