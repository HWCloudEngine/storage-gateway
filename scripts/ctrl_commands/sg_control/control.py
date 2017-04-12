import argparse
import ConfigParser
import sys, uuid
import snap_ctrl, replicate_ctrl, volume_ctrl, backup_ctrl


def snap(args, host, port):
    print('snapshot (%s)' % args)

    params = {'host': host, 'port': port}
    ctrl = snap_ctrl.SnapCtrl(params)
    res = ctrl.do(args)


def replicate(args, host, port):
    print('replicate (%s)' % args)

    params = {'host': host, 'port': port}
    ctrl = replicate_ctrl.RepliacteCtrl(params)
    res = ctrl.do(args)


def volume(args, host, port):
    print('volume %s \n' % args)

    if args.action in ['get', 'disable', 'enable']:
        if not args.vol_id:
            print 'volume name is required!'
            return

    params = {'host': host, 'port': port}
    ctrl = volume_ctrl.VolumeCtrl(params)
    res = ctrl.do(args)


def backup(args, host, port):
    print('volume %s ' % args)

    params = {'host': host, 'port': port}
    ctrl = backup_ctrl.BackupCtrl(params)
    res = ctrl.do(args)


def run():
    # config parser
    config_file = '/etc/storage-gateway/config.ini'
    cp = ConfigParser.ConfigParser()
    cp.read(config_file)
    try:
        host_ = cp.get('ctrl_server', 'ip')
        port_ = cp.getint('ctrl_server', 'port')
    except Exception, ex:
        host_ = '127.0.0.1'
        port_ = 1111

    print ('rpc control server host:port: %s:%s' % (host_, port_))

    # commands parser
    parser = argparse.ArgumentParser(description=
                                     'storage-gateway controler commands converter.')

    sub_parsers = parser.add_subparsers(dest="cmd")
    sub_parsers.required = True

    # snap args
    parser_snap = sub_parsers.add_parser('snap')
    parser_snap.add_argument('-v', '--volume', help='volume id', dest='vol_id')
    parser_snap.add_argument('-a', '--action', \
                             required=True,
                             choices=(
                             'create', 'delete', 'diff', 'rollback', 'read',
                             'list'))
    parser_snap.add_argument('-s', '--snap', help='snapshot id',
                             dest='snap_id')
    parser_snap.add_argument('--second_snap', help='snapshot id',
                             dest='snap_id2')
    parser_snap.add_argument('-o', '--off', dest='offset')
    parser_snap.add_argument('-l', '--len', dest='length')
    parser_snap.add_argument('--seq', help='sequence id', dest='seq_id')
    parser_snap.add_argument('--snap_type', dest='type')
    parser_snap.add_argument('--rep_uuid', help='uuid for replication', \
                             dest='rep_uuid')
    parser_snap.add_argument('--checkpoint', help='uuid for checkpoint', \
                             dest='cp_uuid')
    parser_snap.set_defaults(func=snap)

    # replicate args
    parser_replicate = sub_parsers.add_parser('replicate')
    parser_replicate.add_argument('-a', '--action', \
                                  choices=(
                                  'create', 'enable', 'disable', 'failover', \
                                  'reverse'),
                                  required=True)
    parser_replicate.add_argument('-i', '--replication_uuid', \
                                  dest='uuid')
    parser_replicate.add_argument('-o', '--operation_uuid', dest='op_id', \
                                  default=uuid.uuid1().hex)
    parser_replicate.add_argument('-v', '--volume', help='volume id', \
                                  required=True, dest='vol_id')
    parser_replicate.add_argument('--second_volume', help='volume id',
                                  dest='vol_id2')
    parser_replicate.add_argument('-r', '--role', help='replication\
                         role:primary|secondary',
                                  choices=('primary', 'secondary'),
                                  default='primary')
    parser_replicate.set_defaults(func=replicate)

    # volume control args
    parser_volume = sub_parsers.add_parser('volume')
    parser_volume.add_argument('-a', '--action', \
                               choices=('list', 'enable', 'disable', 'get'),
                               required=True)
    parser_volume.add_argument('-v', '--volume', help='volume id',
                               dest='vol_id')
    parser_volume.add_argument('-t', '--type', help='list type:device|volume',
                               dest='list_type', choices=('device', 'volume'),
                               default='volume')
    parser_volume.add_argument('-s', '--size', help='volume size',
                               dest='vol_size', type=long)
    parser_volume.add_argument('-p', '--device_path', help='block device path',
                               dest='device_path')
    parser_volume.set_defaults(func=volume)

    # backup control args
    parser_backup = sub_parsers.add_parser('backup')
    parser_backup.add_argument('-a', '--action', \
                               choices=(
                               'list', 'create', 'delete', 'get', 'restore'),
                               required=True)
    parser_backup.add_argument('-v', '--volume', help='volume id',
                               dest='vol_id',
                               required=True)
    parser_backup.add_argument('-b', '--backup_name', help='backup name',
                               dest='backup_id')
    parser_backup.add_argument('-s', '--size', help='volume size',
                               dest='vol_size', type=long)
    parser_backup.add_argument('--volume2', help='new volume name',
                               dest='vol_id2')
    parser_backup.add_argument('--device2', help='new block device name',
                               dest='device2')
    parser_backup.add_argument('--size2', help='size of new volume',
                               dest='vol_size2', type=long)
    # backup optional params
    parser_backup.add_argument('--mode', help='backup mode:full|incr|diff',
                               dest='mode', choices=('full', 'incr', 'diff'))
    parser_backup.add_argument('--store_mode',
                               help='backup storage mode:file|object',
                               dest='store_mode', choices=('file', 'object'))
    parser_backup.add_argument('--chunk_size', help='chuck size',
                               dest='chunk_size', default=1048576, type=long)
    parser_backup.add_argument('--backup_path', help='backup path',
                               dest='backup_path')
    parser_backup.set_defaults(func=backup)

    # parse command line args and run
    args = parser.parse_args(sys.argv[1:])
    args.func(args, host_, port_)


def main():
    run()


if __name__ == '__main__':
    main()
