#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<arpa/inet.h>
#include<stdint.h>
#include<sys/socket.h>
#include<sys/types.h>
#include<rdma/rdma_cma.h>
#include <netdb.h>
#include<pthread.h>

#define TIMEOUT_MS 500
#define BUFF_SIZE 100

struct connect
{
//    struct ibv_context *context;   //id->verbs 
    struct ibv_pd * pd;
    struct ibv_comp_channel *comp_channel;
    struct ibv_cq *cq;
//    pthread_t cq_poller_thread;
};

struct memory_qp
{
    char * write_region;
    struct ibv_mr * mr;
};

struct pdata
{
    char * recv_region;
    uint32_t rkey;
};

int on_event(struct rdma_cm_event * event, struct memory_qp *memory_qp);
int addr_resolved(struct rdma_cm_id *id);
int route_resolved(struct rdma_cm_id *id, struct memory_qp *memory_qp);
int conn_established(struct rdma_cm_event *event, struct memory_qp *memory_qp);
void error_handing(char * message);
void bulid_qp(struct ibv_qp_init_attr *qp_init_attr, struct rdma_cm_id *id);
// void * poll_cq(void * args);
// void on_completion(struct ibv_wc *wc, struct rdma_cm_id *id);
int conn_disconnected(struct rdma_cm_id *id);
void free_memory(struct memory_qp *memory_qp);


struct connect *s_connect = NULL;

int main(int argc, char *argv[])
{
    if(argc != 4)
    {
        printf("usage: %s <server-addr> <port> <message>\n",argv[0]);
        exit(1);
    }

    struct rdma_event_channel * ec = NULL; 
    struct rdma_cm_id * client_id = NULL;
    struct addrinfo *result;
    struct addrinfo hints = {
        .ai_family    = AF_INET,
   		.ai_socktype  = SOCK_STREAM
    };

    struct rdma_cm_event *event;
    struct memory_qp *memory_qp = (struct memory_qp *)malloc(sizeof(memory_qp));
    s_connect = (struct connect *)malloc(sizeof(struct connect));
    memory_qp->write_region = (char *)malloc(BUFF_SIZE);

    ec = rdma_create_event_channel();  //创建RDMA事件通道
    if(ec == NULL)
        error_handing("rdma_create_event_channel() error");
    
    if(rdma_create_id(ec, &client_id, NULL, RDMA_PS_TCP) == -1)  //绑定监听套接字
        error_handing("rdma_create_id() error");

    if(getaddrinfo(argv[1], argv[2], &hints, &result))   //解析地址信息将结果存到result
        error_handing("getaddrinfo() error");

    if(rdma_resolve_addr(client_id, NULL, result->ai_addr, TIMEOUT_MS) == -1)
        error_handing("rdma_create_id() error");

    while(rdma_get_cm_event(ec, &event) == 0)
    {
        struct rdma_cm_event event_copy;
        memcpy(&event_copy, event, sizeof(*event));
        rdma_ack_cm_event(event);
        if (on_event(&event_copy, memory_qp))
            break;
    }

    rdma_destroy_event_channel(ec);
    free_memory(memory_qp);
    return 0;
}

int on_event(struct rdma_cm_event * event, struct memory_qp *memory_qp)
{

    int ret = 0;
    if(event -> event == RDMA_CM_EVENT_ADDR_RESOLVED)
        ret = addr_resolved(event->id);
    else if(event -> event == RDMA_CM_EVENT_ROUTE_RESOLVED)
        ret = route_resolved(event->id, memory_qp);
    else if(event -> event == RDMA_CM_EVENT_ESTABLISHED)
        ret = conn_established(event, memory_qp);
    else if(event-> event == RDMA_CM_EVENT_DISCONNECTED)
        ret = conn_disconnected(event->id);
    else
        error_handing("on_event: no event");
    return ret;
}

int addr_resolved(struct rdma_cm_id *id)
{
    if(rdma_resolve_route(id, TIMEOUT_MS) == -1)
        error_handing("rdma_resolve_route() error");
        return 0;
}


int route_resolved(struct rdma_cm_id *id, struct memory_qp *memory_qp)
{
    struct ibv_qp_init_attr qp_init_attr = {};
    struct rdma_conn_param conn_param;
    memset(&conn_param, 0, sizeof(conn_param));

    s_connect->pd = ibv_alloc_pd(id->verbs);
    if(s_connect->pd == NULL)
        error_handing("ibv_alloc_pd() error");

    s_connect->comp_channel = ibv_create_comp_channel(id->verbs);
    if(s_connect->comp_channel == NULL)
        error_handing("ibv_create_comp_channel() error");
    
    s_connect->cq = ibv_create_cq(id->verbs, 2, NULL, s_connect->comp_channel, 0);

//    s_connect->context = id->verbs;   //为轮询线程做准备,轮询线程需要该参数

    if(s_connect->cq == NULL)
        error_handing("ibv_create_cq() error");
    
    if(ibv_req_notify_cq(s_connect->cq, 0) == -1)
        error_handing("ibv_req_notify_cq() error");

    memory_qp->mr = ibv_reg_mr(s_connect->pd, memory_qp->write_region, BUFF_SIZE, IBV_ACCESS_LOCAL_WRITE);
    if(memory_qp->mr == NULL)
        error_handing("ibv_reg_mr() error");

    bulid_qp(&qp_init_attr, id);

    conn_param.initiator_depth = 1;
	conn_param.retry_count     = 7;

//    pthread_create(&s_connect->cq_poller_thread, NULL, poll_cq, id);  //创建一个线程轮询CQ队列

    if(rdma_connect(id, &conn_param) == -1)
        error_handing("rdma_connect() error");
    
    return 0;
}

void bulid_qp(struct ibv_qp_init_attr *qp_init_attr, struct rdma_cm_id *id)
{
    qp_init_attr->send_cq = s_connect->cq;
    qp_init_attr->recv_cq = s_connect->cq;
    qp_init_attr->qp_type = IBV_QPT_RC;

    qp_init_attr->cap.max_send_wr = 2;
    qp_init_attr->cap.max_recv_wr = 2;
    qp_init_attr->cap.max_send_sge = 2;
    qp_init_attr->cap.max_recv_sge = 2;

    if(rdma_create_qp(id, s_connect->pd, qp_init_attr) == -1)
        error_handing("rdma_create_qp() error");
}

int conn_established(struct rdma_cm_event *event, struct memory_qp *memory_qp)
{
    struct pdata server_data = {};
    struct ibv_send_wr send_wr = {};
    struct ibv_sge sge = {};
    struct ibv_send_wr *bad_wr = NULL;
    memcpy(&server_data, event->param.conn.private_data,sizeof(server_data));

    //memory_qp->write_region = "hello server\n";
    snprintf(memory_qp->write_region, BUFF_SIZE, "hello server\n");

    sge.addr = (uint64_t)memory_qp->write_region;
    sge.length = BUFF_SIZE;
    sge.lkey = memory_qp->mr->lkey;

    send_wr.wr_id = 1;
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_RDMA_WRITE;
    send_wr.wr.rdma.remote_addr = (uint64_t)server_data.recv_region;
    send_wr.wr.rdma.rkey = server_data.rkey;

    ibv_post_send(event->id->qp, &send_wr, &bad_wr);

    return 0;
}


// void * poll_cq(void * args)
// {
//     struct rdma_cm_id *id= (struct rdma_cm_id *)args;
//     struct ibv_cq *output_cq = NULL;
//     struct ibv_wc wc = {};
//     printf("2222\n");
//     while(1)
//     {
//         if(ibv_get_cq_event(s_connect->comp_channel, &output_cq, (void **)&s_connect->context) == -1)
//             error_handing("ibv_get_cq_event() error");
//         printf("11111\n");
//         ibv_ack_cq_events(output_cq, 1);
//         if(ibv_req_notify_cq(output_cq, 0) == -1)
//             error_handing("ibv_req_notify_cq() error");
//         while (ibv_poll_cq(output_cq, 1, &wc))   //从cq中产生一个cqe  存入wc中
//             on_completion(&wc, id);
//     }

// }

// void on_completion(struct ibv_wc *wc, struct rdma_cm_id *id)
// {
//     if (wc->status != IBV_WC_SUCCESS)
//         error_handing("on_completion: status is not IBV_WC_SUCCESS.");
//         printf("ssssss\n");
//     rdma_disconnect(id);
// }

int conn_disconnected(struct rdma_cm_id *id)
{
    printf("disconnected.\n");
    rdma_destroy_qp(id);
    rdma_destroy_id(id);
    return 1;
}


void free_memory(struct memory_qp *memory_qp)
{
    ibv_dereg_mr(memory_qp->mr);
    free(memory_qp->write_region);
    free(memory_qp);
    free(s_connect);
}

void error_handing(char * message)
{
    fputs(message, stderr);
    fputs("\n" , stderr);
    exit(1);
}