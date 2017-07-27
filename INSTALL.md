# storage-gateway  
# installation  
1. deploy environment(ceph or nfs)  
1.1.0 prepare mount point  
`mkdir -p /mnt/cephfs`  
1.1.1 create cephfs  
`ceph osd pool create cephfs_data 128`  
`ceph osd pool create cephfs_metadata 128`  
`ceph fs new cephfs cephfs_metadata cephfs_data`  
1.1.2 mount cephfs on mount point, `admin_key` get from `/etc/ceph/ceph.client.admin.keyring`  
`mount -t ceph ceph_fs_ip:ceph_fs_port:/ /mnt/cephfs -o name=admin,secret=admin_key`  
1.1.3 create a S3 user and generate `access_key` and `secret_key` for later use  
`radosgw-admin user create --uid={username} --display-name={display-name}` 
1.2.0  nfs  
` apt-get install nfs-kernel-server`  
`chown nobody:nogroup /mnt/cephfs`  
`sed -i "$ a /mnt/cephfs *(rw,no_root_squash,no_subtree_check)" /etc/exportfs`  
`exportfs -a`  
`service nfs-kernel-server start`  
`apt-get install nfs-common`  
`mount ip:/mnt/cephfs  mount point`  
2. compile and install grpc, protobuf, gtest, gmock  
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
`apt-get install cmake -y`  
`git clone https://github.com/google/googletest.git`  
`cd googletest`  
`git checkout release-1.8.0`  
`cmake googlemock/`  
`make & make install`  
`cd ..`  
`apt-get install libcurl4-nss-dev`  
`git clone https://github.com/ceph/libs3.git`  
`cd libs3`  
`make`  
`cp build/lib/libs3.so.trunk0 /lib64/libs3.so`  
`cd ..`  
`ldconfig`  
`git clone https://github.com/google/googletest.git`  
`cd googletest`  
`git checkout release-1.8.0`  
`cmake googlemock/`  
`make & make install`  
`cd ..`  
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
`PORTABLE=1 make static_lib`  
`make install`  
`git clone https://github.com/Hybrid-Cloud/storage-gateway.git`  
`cd storage-gateway`  
`./build.sh`    
4. build linux agent  
`cd storage-gateway/src/agent`  
`make -j`  
5. compile tgt source code  
`apt-get install tgt xsltproc`  
`git clone -b iohook https://github.com/Hybrid-Cloud/tgt.git`  
`cd tgt`  
`make -j8`  
`make install`  
6. configure storage gateway  
`cp storage_gateway/etc/config.ini /etc/storage_gateway/`  
accord environment to modify configure field  
7. start sg server
`cd storage-gateway/src/sg_server`  
`cp storage-gateway/lib/libs3.so /lib/libs3.so.trunk0`  
`./sg_server &`  
8. install agent driver[agent mode only]  
`cd src/agent`  
`insmod sg_agent.ko`  
9. start sg client 
`cd storage-gateway/src/sg_client`  
`./sg_client &`   
10. start iscsi target tgtd[isci mode only]  
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
