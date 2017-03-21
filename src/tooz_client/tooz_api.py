from tooz import coordination
import six, uuid
import pdb
import time
import socket
import threading
class Coordination():
    def __init__(self):
        self.sock = None
        return
    def start(self, backend_url='kazoo://127.0.0.1:2181',
            member_id=str(uuid.uuid4()).encode('ascii')):
        self.member_id = member_id
        self.coordinator = coordination.get_coordinator(backend_url, member_id)
        self.coordinator.start()
        print ("backend_url=%s" % backend_url)
        print ("member_id=%s" % member_id)
        print "py:start"
        #group = six.binary_type(six.text_type(uuid.uuid4()).encode('ascii'))
    def join_group(self,group):
        print group
        print 'py:join group'
        request = self.coordinator.join_group_create(group)
        if request:
            request.get()
    def leave_group(self,group):
        self.coordinator.leave_group(group)
    def group_changed_callback(self,event):
        if self.sock is None:
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock.connect('./sg_uds')
        self.sock.send('group_changed, callback member=' + self.member_id)
        #reply = sock.recv(4096)
        #print reply
    def get_members(self, group):
        ret = self.coordinator.get_members(group)
        return ret.get()
    def get_member_capabilities(self, group, member_id):
        ret = self.coordinator.get_member_capabilities(group, member_id)
        return ret.get()
    def get_member_info(self, group, member_id):
        ret = self.coordinator.get_member_info(group, member_id)
        return ret.get()
    def watch_group(self,group, callback=None):
        print 'py:watch_group'
        if callback is None:
            callback = self.group_changed_callback
        self.coordinator.watch_join_group(group, callback)
        self.coordinator.watch_leave_group(group, callback)

    def stop(self):
        self.coordinator.stop()
        self.sock.close()
    def join(self):
        self.run_watchers_t.join()
        self.heartbeat_t.join()
    def run_watchers(self):
        self.coordinator.run_watchers()
        print 'py:run_watcher'
    def heartbeat(self):
        self.coordinator.heartbeat()
        print 'py:heartbeat'
