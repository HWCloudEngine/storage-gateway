#ifndef TRANSPORT_H
#define TRANSPORT_H

#include <linux/net.h>

struct transport
{
    union 
    {
        /*tcp udp(host:port)*/
        char* host;
        /*unix domain socket(path)*/
        char* path; 
    } addr;
    
    struct socket* sock;
};

int tp_create(struct transport* net, const char* host);
int tp_connect(struct transport* net);
int tp_send(struct transport* net, const char* buf, const int len);
int tp_recv(struct transport* net, char* buf, const int len);
int tp_close(struct transport* net);

#endif
