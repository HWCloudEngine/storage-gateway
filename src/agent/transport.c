#include <linux/slab.h>
#include <linux/in.h>
#include <linux/un.h>
#include <linux/tcp.h>
#include <linux/inet.h>
#include <linux/delay.h>
#include <linux/version.h>
#include "log.h"
#include "transport.h"

int tp_set_timeout(struct transport* net, int timeout)
{
    int ret;
    long jiffies_left = timeout * msecs_to_jiffies(MSEC_PER_SEC);
    struct timeval tv;
    jiffies_to_timeval(jiffies_left, &tv);
    ret = kernel_setsockopt(net->sock,SOL_SOCKET,SO_RCVTIMEO,(char *)&tv,sizeof(tv));
    if(ret){
        LOG_ERR("Can't set socket recv timeout %ld.%06d: %d\n",(long)tv.tv_sec,(int)tv.tv_usec,ret);
    }

    return ret;
}

int tp_create(struct transport* net, const char* host, int port)
{
    int ret;
    net->addr.host = kstrdup(host, GFP_KERNEL);
    net->port = port;
    atomic_set(&net->isok, 0);
    if(IS_ERR(net->addr.host)){
        ret = -EFAULT; 
        LOG_ERR("kstrdump host failed");
        return ret;
    }
#ifdef USE_UDS
    ret = sock_create_kern(AF_UNIX, SOCK_STREAM, 0, &(net->sock));
#else
#if (LINUX_VERSION_CODE < KERNEL_VERSION(4,2,0))
    ret = sock_create_kern(AF_INET, SOCK_STREAM, 0, &(net->sock));
#else
    ret = sock_create_kern(&init_net, AF_INET, SOCK_STREAM, 0, &(net->sock));
#endif
#endif
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
#ifdef USE_UDS
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
    if(ret){
        LOG_ERR("socket setsocket:%s failed:%d", net->addr.host, ret);
        return ret;
    } 

    /*kernel unix domain socket has bug, should sizeof(addr)-1 instead of sizeof(addr)*/
#ifdef USE_UDS
    ret = kernel_connect(net->sock, (struct sockaddr*)&net_addr, (sizeof(net_addr)-1), 0);
#else
    ret = kernel_connect(net->sock, (struct sockaddr*)&net_addr, (sizeof(net_addr)), 0);
#endif
    if(ret){
        LOG_ERR("socket connect:%s failed:%d", net->addr.host, ret);
        return ret;
    }
    atomic_set(&net->isok, 1);
    LOG_INFO("socket connect ok");
    return ret;
}

int tp_send(struct transport* net, const char* buf, const int len)
{
    int ret = 0;
    char* send_buf = (char*)buf;
    int send_len = (int)len;
    struct msghdr msg;
    struct kvec iov;
    sigset_t blocked, oldset;
    unsigned long pflags= current->flags;
    siginitsetinv(&blocked, sigmask(SIGKILL));
    sigprocmask(SIG_SETMASK, &blocked, &oldset);
    current->flags |= PF_MEMALLOC;
    while(send_len > 0)
    {
        /*check socket status*/
        if(net->sock->state != SS_CONNECTED){
            ret = -EPIPE;
            LOG_ERR("socket not connected");
            atomic_set(&net->isok, 0);
            break;
        }
        net->sock->sk->sk_allocation = GFP_NOIO | __GFP_MEMALLOC;
        iov.iov_base = (void*)send_buf,
        iov.iov_len = send_len,
        memset(&msg, 0, sizeof(msg));
        ret = kernel_sendmsg(net->sock, &msg, &iov, 1, send_len);
        if(ret == -EAGAIN || ret == -EINTR){
            LOG_INFO("send busy eagain");
            msleep(10);
            continue;
        }
        if(ret <= 0){
            LOG_INFO("send failed ret:%d, len:%d", ret, send_len);
            atomic_set(&net->isok, 0);
            break;
        }
        send_len -= ret;
        send_buf += ret;
    }
    if(send_len == 0){
        atomic_set(&net->isok, 1);
    }
    sigprocmask(SIG_SETMASK, &oldset, NULL);
    tsk_restore_flags(current, pflags, PF_MEMALLOC);
    return (send_len == 0) ? 0 : -1;
}

int tp_recv(struct transport* net, char* buf, const int len)
{
    int ret = 0;
    char* recv_buf = (char*)buf;
    int recv_len = 0;
    struct msghdr msg;
    struct kvec iov;
    sigset_t blocked, oldset;
    unsigned long pflags= current->flags;
    siginitsetinv(&blocked, sigmask(SIGKILL));
    sigprocmask(SIG_SETMASK, &blocked, &oldset);
    current->flags |= PF_MEMALLOC;

    while(recv_len < len)
    {
        /*check socket status*/
        if(net->sock->state != SS_CONNECTED){
            ret = -EPIPE;
            atomic_set(&net->isok, 0);
            LOG_ERR("socket not connected");
            break;
        }
        net->sock->sk->sk_allocation = GFP_NOIO | __GFP_MEMALLOC;
        iov.iov_base = (void*)recv_buf,
        iov.iov_len  = (len-recv_len),
        memset(&msg, 0, sizeof(msg));
        msg.msg_flags = MSG_WAITALL | MSG_NOSIGNAL;
        ret = kernel_recvmsg(net->sock, &msg, &iov, 1, (len-recv_len), msg.msg_flags);
        if(ret == -EAGAIN || ret == -EINTR){
            msleep(10);
            continue;
        }
        if(ret <= 0){
            LOG_INFO("recv failed ret:%d, len:%d", ret, (len-recv_len));
            atomic_set(&net->isok, 0);
            break;
        }
        recv_len += ret;
        recv_buf += ret;
    }
    if(recv_len == len){
        atomic_set(&net->isok, 1);
    }
    sigprocmask(SIG_SETMASK, &oldset, NULL);
    tsk_restore_flags(current, pflags, PF_MEMALLOC);
    return (recv_len == len) ? 0 : -1;
}

int tp_close(struct transport* net)
{
    kernel_sock_shutdown(net->sock, SHUT_RDWR);
    return 0;
}
