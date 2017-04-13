#include <linux/slab.h>
#include <linux/in.h>
#include <linux/un.h>
#include <linux/tcp.h>
#include <linux/inet.h>
#include <linux/delay.h>
#include "log.h"
#include "transport.h"

int tp_create(struct transport* net, const char* host, int port)
{
    int ret;
    /*todo add tcp protocol support*/
    net->addr.host = kstrdup(host, GFP_KERNEL);
	net->port = port;
    if(IS_ERR(net->addr.host)){
        ret = -EFAULT; 
        LOG_ERR("kstrdump host failed");
        return ret;
    }
    //ret = sock_create_kern(AF_UNIX, SOCK_STREAM, 0, &(net->sock));
    ret = sock_create_kern(AF_INET, SOCK_STREAM, 0, &(net->sock));
    if(ret || NULL == net->sock){
        LOG_ERR("socket create failed:%d", ret);
        return ret;
    }
    LOG_INFO("socket create ok");
    return 0;
}

int tp_connect(struct transport* net)
{
    int ret;
	int val = 1;
#if 0
    struct sockaddr_un net_addr;
    memset(&net_addr, 0, sizeof(net_addr));
    net_addr.sun_family = AF_UNIX;
    strncpy(net_addr.sun_path, net->addr.path, strlen(net->addr.path));
    LOG_INFO("socket connect sun_path:%s path:%s", net_addr.sun_path, net->addr.path);
#else  
    struct sockaddr_in net_addr;
    memset(&net_addr, 0, sizeof(net_addr));
    net_addr.sin_family = AF_INET;
    net_addr.sin_port = htons(net->port);
    net_addr.sin_addr.s_addr = in_aton(net->addr.host);
#endif
    
    ret = kernel_setsockopt(net->sock, IPPROTO_TCP, TCP_NODELAY, (char*)&val, sizeof(val));
    //ret = kernel_setsockopt(net->sock, SOL_TCP, TCP_NODELAY, (char*)&val, sizeof(val));
    if(ret){
        LOG_ERR("socket setsocket:%s failed:%d", net->addr.host, ret);
        return ret;
    } 

    /*kernel unix domain socket has bug, 
     * should sizeof(addr)-1 instead of sizeof(addr)
     */
    //ret = kernel_connect(net->sock, (struct sockaddr*)&net_addr, (sizeof(net_addr)-1), 0);
    ret = kernel_connect(net->sock, (struct sockaddr*)&net_addr, (sizeof(net_addr)), 0);
    if(ret){
        //LOG_ERR("socket connect:%s failed:%d", net_addr.sun_path, ret);
        LOG_ERR("socket connect:%s failed:%d", net->addr.host, ret);
        return ret;
    }
    
    LOG_INFO("socket connect ok");
    return ret;
}

int tp_send(struct transport* net, const char* buf, const int len)
{
    int ret;
    char* send_buf = (char*)buf;
    int   send_len = (int)len;

    while(send_len > 0)
    {
        /*todo check socket status*/
        if(net->sock->state != SS_CONNECTED){
            ret = -EPIPE;
            LOG_ERR("socket not connected");
            break;
        }
        
        {
            struct kvec iov = {
                .iov_base = (void*)send_buf,
                .iov_len  = send_len,
            };
            struct msghdr msg;
            memset(&msg, 0, sizeof(msg));
            ret = kernel_sendmsg(net->sock, &msg, &iov, 1, send_len);
        }

        if(ret == -EAGAIN || ret == -EINTR){
            LOG_INFO("send busy eagain");
            msleep(10);
            continue;
        }

        if(ret < 0){
            LOG_INFO("send failed ret:%d, len:%d", ret, send_len);
            break;
        }

        send_len -= ret;
        send_buf += ret;
    }
    return (send_len == 0) ? 0 : -1;
}

int tp_recv(struct transport* net, char* buf, const int len)
{
    int ret;
    char* recv_buf = (char*)buf;
    int   recv_len = 0;

    while(recv_len < len)
    {
        /*todo check socket status*/
        if(net->sock->state != SS_CONNECTED){
            ret = -EPIPE;
            LOG_ERR("socket not connected");
            break;
        }
        
        {
            struct kvec iov = {
                .iov_base = (void*)recv_buf,
                .iov_len  = (len-recv_len),
            };
            struct msghdr msg;
            memset(&msg, 0, sizeof(msg));
            LOG_INFO("recv_msg start");
            ret = kernel_recvmsg(net->sock, &msg, &iov, 1, (len-recv_len), 0);
            LOG_INFO("recv_msg over ret:%d read:%d", ret, (len-recv_len));
        }

        if(ret == -EAGAIN || ret == -EINTR){
            LOG_INFO("recv busy eagain");
            msleep(10);
            continue;
        }

        if(ret <= 0){
            LOG_INFO("recv failed ret:%d, len:%d", ret, (len-recv_len));
            break;
        }

        recv_len += ret;
        recv_buf += ret;
    }
    return (recv_len == len) ? 0 : -1;
}

int tp_close(struct transport* net)
{
    kernel_sock_shutdown(net->sock, SHUT_RDWR);
    return 0;
}
