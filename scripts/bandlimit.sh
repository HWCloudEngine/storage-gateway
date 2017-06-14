#!/bin/bash

#can use tc iptables wondershaper tricker tools to limit network bandwith

tc qdisc del dev eth0 root
tc qdisc add dev eth0 handle 1: root htb default 11

#1GBps
#tc class add dev eth0 parent 1: classid 1:1 htb rate 10000kbps
#tc class add dev eth0 parent 1:1 classid 1:11 htb rate 10000kbps
#1GBps
#tc class add dev eth0 parent 1: classid 1:1 htb rate 1000kbps
#tc class add dev eth0 parent 1:1 classid 1:11 htb rate 1000kbps

#80MBps
tc class add dev eth0 parent 1: classid 1:1 htb rate 100kbps
tc class add dev eth0 parent 1:1 classid 1:11 htb rate 100kbps

#1MBps
#tc class add dev eth0 parent 1: classid 1:1 htb rate 10kbps
#tc class add dev eth0 parent 1:1 classid 1:11 htb rate 10kbps
