from tooz import coordination
from tooz import hashring
import six, uuid
import time
import socket
import logging

logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='tooz_api.log',
                filemode='w')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

DEFAULT_BUCKET_NUMBER = 2**10
SOCKET_FILE = './sg_uds'
class Coordination():
    def __init__(self):
        self.sock = None
        self.hashring = None
        self.socket_file = SOCKET_FILE
        self.bucket_num = DEFAULT_BUCKET_NUMBER

    def start(self, backend_url, member_id=None):
        self._my_id = member_id or six.binary_type(six.text_type(uuid.uuid4()).encode('ascii'))
        logging.info("backend_url=%s", backend_url)
        logging.info("member_id=%s", self._my_id)
        try:
            self._coordinator = coordination.get_coordinator(backend_url, self._my_id)
            self._coordinator.start()
            logging.info("Coordination backend started successfully.")
        except coordination.ToozError:
            logging.exception("Error connecting to coordination backend.")

        
    def join_group(self, group_id):
        logging.debug('join group = %s', group_id)
        request = self._coordinator.join_group_create(group_id)
        if request:
            request.get()

    def leave_group(self, group_id):
        if self._coordinator:
            self._coordinator.leave_group(group_id)

    def _join_group_callback(self, event):
        if self.sock is None:
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock.connect(self.socket_file)
        self.sock.send("join_group")
        logging.debug('join group callback')
        
    def _leave_group_callback(self, event):
        if self.sock is None:
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock.connect(self.socket_file)
        self.sock.send('leave_group')
        logging.debug('leave group callback')

    def get_members(self, group_id):
        if not self._coordinator:
            return [self._my_id]
        ret = self._coordinator.get_members(group_id)
        members = ret.get()
        logging.debug('members of group %s are: %s, Me: %s',group_id, members, self._my_id)
        return members

    def watch_group(self, group_id):
        logging.debug('watch_group group_id=%s', group_id)
        self._coordinator.watch_join_group(group_id, self._join_group_callback)
        self._coordinator.watch_leave_group(group_id, self._leave_group_callback)

    def stop(self):
        if self._coordinator:
            self._coordinator.stop()
        if self.sock:
            self.sock.close()

    def run_watchers(self):
        if self._coordinator:
            self._coordinator.run_watchers()
            logging.debug('run_watcher')

    def heartbeat(self):
        if self._coordinator:
            if not self._coordinator.is_started:
                # re-connect
                self.start()
            try:
                self._coordinator.heartbeat()
                logging.debug('heartbeat')
            except coordination.ToozError:
                logging.exception('Error sending a heartbeat to coordination backend.')

    def refresh_nodes_view(self, group_id):
        members = self.get_members(group_id)
        self.hashring = hashring.HashRing(members)

    def get_nodes(self, data, ignore_nodes=None, replicas=1):
        if self.hashring is None:
            return []
        return self.hashring.get_nodes(data, ignore_nodes, replicas)

    def rehash_buckets_to_node(self, group_id, member_id=None, bucket_num=DEFAULT_BUCKET_NUMBER):
        """
        We have a fixed number of buckets (DEFAULT_BUCKET_NUMBER=1024),
        each node(also name as member) takes control part of buckets,
        We use hashring to calculate buckets belong to this node(member_id=self._myid)
        """
        node = member_id or self._my_id
        try:
            members = self.get_members(group_id)
            if self._my_id not in members:
                logging.warning("Member not in group, rejoin group")
                self.join_group(group_id)
                members = self.get_members(group_id)
                if self._my_id not in members:
                    logging.error("Can not get buckets because failed to join group")
            hr = hashring.HashRing(members, partitions=100)
            my_buckets=[]
            for bucket in range(0, bucket_num):
                if node in hr.get_nodes(self.encode(bucket)):
                    my_buckets.append(bucket)

            logging.debug('my bucket: %s',[six.text_type(f) for f in my_buckets])
            return my_buckets
        except coordination.ToozError:
            logging.exception("Error getting group membership info from coordination backend")

    @staticmethod
    def encode(value):
        """encode to bytes"""
        return six.text_type(value).encode('utf-8')
