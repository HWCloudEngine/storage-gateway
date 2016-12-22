import argparse
import ConfigParser
import sys,uuid
import snap_ctrl,replicate_ctrl

def snap(args,cp):
    print ('action: %s' % args.action)
    print ('volume: %s'%args.vol_id)
    ip = cp.get('replicate','local_ip')
    print ('local ip: %s' % ip)
    params = {'ip':ip,'port':'1111'}
    ctrl = snap_ctrl.SnapCtrl(params)
    ctrl.do(args)

def replicate(args,cp):
    print('operation uuid: %s' %args.op_id)
    print('replicate (%s)' % args)
    ip = cp.get('replicate','local_ip')                                          
    print ('local ip: %s' % ip)                                                  
    params = {'ip':ip,'port':'1111'} 
    ctrl = replicate_ctrl.RepliacteCtrl(params)
    ctrl.do(args)

def run():
    #config parser
    config_file = '/etc/storage-gateway/config.ini'
    cp = ConfigParser.ConfigParser()
    cp.read(config_file)
    #commands parser
    parser = argparse.ArgumentParser(description=
            'storage-gateway controler commands converter.')
    #snap args
    sub_parsers = parser.add_subparsers()
    parser_snap = sub_parsers.add_parser('snap')
    parser_snap.add_argument('-v','--volume',help='volume id',dest='vol_id')
    parser_snap.add_argument('-a','--action',\
                         required=True,
                         choices=('create','delete','diff','rollback','read','list'))
    parser_snap.add_argument('-s','--snap',help='snapshot id',dest='snap_id')
    parser_snap.add_argument('--second_snap',help='snapshot id',dest='snap_id2')
    parser_snap.add_argument('-o','--off',dest='offset')
    parser_snap.add_argument('-l','--len',dest='length')
    parser_snap.set_defaults(func=snap)
    #replicate args
    parser_replicate = sub_parsers.add_parser('replicate')
    parser_replicate.add_argument('-a','--action',\
                              choices=('create','enable','disable','failover',\
                              'reverse','query','list'),
                              required=True)
    parser_replicate.add_argument('-i','--replication_uuid',required=True,\
                              dest='uuid')
    parser_replicate.add_argument('-o','--operation_uuid',dest='op_id',\
                              default=uuid.uuid1().hex)
    parser_replicate.add_argument('-v','--volume',help='volume id',dest='vol_id')
    parser_replicate.add_argument('--second_volume',help='volume id',
                              dest='vol_id2')
    parser_replicate.add_argument('-r','--role',help='replication\
                         role:primary|secondary',choices=('primary','secondary'),
                         default='primary')
    parser_replicate.set_defaults(func=replicate)
    #parse command line args and run
    args = parser.parse_args(sys.argv[1:])
    args.func(args,cp)

if __name__ == '__main__':
    run()
