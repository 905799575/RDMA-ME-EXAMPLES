#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<arpa/inet.h>
#include<stdint.h>
#include<sys/socket.h>
#include<sys/types.h>
#include<rdma/rdma_cma.h>
#include<netdb.h>
#include<pthread.h>

#define BUFF_SIZE 100
#define TIME_OUT 600

struct connection
{
    struct ibv_context *context;   //id->verbs 
    struct ibv_pd * pd;
    struct ibv_cq * cq;
    struct ibv_comp_channel *comp_channel;
    pthread_t cq_poller_thread;
};


struct memory
{
    char * send_region;
    struct ibv_mr * mr;

};


void error_handing(char * message);
int on_event(struct rdma_cm_event *event, struct memory * memory_qp);
int connect_request(struct rdma_cm_id * id, struct memory * memory_qp);
void build_pd_cq(struct rdma_cm_id * id);
void memory_register(struct memory * memory_qp);
void build_qp(struct rdma_cm_id * id, struct ibv_qp_init_attr *qp_init_attr);
void post_receive(struct rdma_cm_id * id, struct memory * memory_qp);
int conn_established(struct rdma_cm_id * id, struct memory * memory_qp);
void * poll_cq(void * args);
void on_completion(struct ibv_wc *wc, struct rdma_cm_id *id);
int conn_disconnected(struct rdma_cm_id *id);
void free_memory(struct memory *memory_qp);


struct connection * s_connection = NULL;

int main(int argc, char *argv[])
{
    struct rdma_event_channel *ec = NULL;
    struct rdma_cm_id *client_id = NULL;
    struct sockaddr_in client_addr;
    memset(&client_addr, 0, sizeof(client_addr));
    struct rdma_cm_event *event = NULL;

    struct addrinfo hints = {
        .ai_family    = AF_INET,
   		.ai_socktype  = SOCK_STREAM
    };

    struct addrinfo *result;

    struct memory * memory_qp = (struct memory *)malloc(sizeof(*memory_qp));
    memory_qp->send_region = (char *)malloc(BUFF_SIZE);
    
    s_connection = (struct connection *)malloc(sizeof(*s_connection));

    if(argc != 3)
    {
        printf("usage: %s <sever-ip> <port>\n", argv[0]);
        exit(1);
    }

    client_addr.sin_family = AF_INET;
    client_addr.sin_port = htons(atoi(argv[2]));
    client_addr.sin_addr.s_addr = inet_addr(argv[1]);

    ec = rdma_create_event_channel();   
    if(ec == NULL)
        error_handing("rdma_create_event_channel() error");

    if(rdma_create_id(ec, &client_id, NULL, RDMA_PS_TCP) == -1)      //这里会分配id
        error_handing("rdma_create_id() error");

    if(getaddrinfo(argv[1], argv[2], &hints, &result))
        error_handing("getaddrinfo() error");

    if(rdma_resolve_addr(client_id, NULL, result->ai_addr, TIME_OUT) == -1)
        error_handing("rdma_resolve_addr() error");

    while(rdma_get_cm_event(ec, &event) == 0)
    {
        struct rdma_cm_event event_copy;
        memset(&event_copy, 0, sizeof(event_copy));
        memcpy(&event_copy, event, sizeof(*event));
        rdma_ack_cm_event(event);
        if(on_event(&event_copy, memory_qp))
            break;
     }

    rdma_destroy_event_channel(ec);
    free(s_connection);
    free_memory(memory_qp);
    return 0;
}

int on_event(struct rdma_cm_event *event, struct memory * memory_qp)
{
    int ret = 0;
    if(event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
        ret = addr_resolved(event->id);
    if(event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)
        ret = route_resolved(event->id, memory_qp);
    if(event->event == RDMA_CM_EVENT_ESTABLISHED)
        ret = conn_established(event->id, memory_qp);
    if(event->event == RDMA_CM_EVENT_DISCONNECTED)
        ret = conn_disconnected(event->id);
    return ret;
}


int addr_resolved(struct rdma_cm_id * id)
{
    printf("addr_resolved\n");
    if(rdma_resolve_route(id, TIME_OUT) == -1)
        error_handing("rdma_resolve_route() error");
    return 0;
}

int route_resolved(struct rdma_cm_id *id, struct memory * memory_qp)
{
    printf("route resolved\n");
    struct ibv_qp_init_attr qp_init_attr = {};
    struct rdma_conn_param conn_param;
    memset(&conn_param, 0, sizeof(conn_param));

    conn_param.initiator_depth = 1;
	conn_param.retry_count     = 7;

    build_pd_cq(id);
    memory_register(memory_qp);
    build_qp(id, &qp_init_attr);
    s_connection->context = id->verbs;   //为轮询线程做准备,轮询线程需要该参数
    pthread_create(&s_connection->cq_poller_thread, NULL, poll_cq, id);  //创建一个线程轮询CQ队列
    rdma_connect(id, &conn_param);
    return 0;
}

void build_pd_cq(struct rdma_cm_id * id)
{
    s_connection->pd = ibv_alloc_pd(id->verbs);
    if(s_connection->pd == NULL)
        error_handing("ibv_alloc_pd() error");

    s_connection->comp_channel = ibv_create_comp_channel(id->verbs);
    if(s_connection->comp_channel == NULL)
        error_handing("ibv_create_com_channel() error");

    s_connection->cq = ibv_create_cq(id->verbs, 2, NULL, s_connection->comp_channel, 0);
    if(s_connection->cq == NULL)
        error_handing("ibv_create_cq() error");
    
    if(ibv_req_notify_cq(s_connection->cq, 0) == -1)
        error_handing("ibv_req_notify_cq() error");
}

void memory_register(struct memory * memory_qp)
{
    memory_qp->mr = ibv_reg_mr(s_connection->pd, memory_qp->send_region, BUFF_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if(memory_qp->mr == NULL)
        error_handing("ibv_reg_mr() error");
}

void build_qp(struct rdma_cm_id * id, struct ibv_qp_init_attr *qp_init_attr)
{
    qp_init_attr->send_cq = s_connection->cq;
    qp_init_attr->recv_cq = s_connection->cq;

    qp_init_attr->cap.max_send_wr = 2;
    qp_init_attr->cap.max_recv_wr = 2;
    qp_init_attr->cap.max_send_sge = 2;
    qp_init_attr->cap.max_recv_sge = 2;

    qp_init_attr->qp_type = IBV_QPT_RC;

    rdma_create_qp(id, s_connection->pd, qp_init_attr);
}

int conn_established(struct rdma_cm_id * id, struct memory * memory_qp)
{
    struct ibv_send_wr send_wr = {};
    struct ibv_sge sge = {};
    struct ibv_send_wr *bad_wr = NULL;

    snprintf(memory_qp->send_region, BUFF_SIZE, "hello server i am client\n");

    sge.addr = (uint64_t)memory_qp->send_region;
    sge.length = BUFF_SIZE;
    sge.lkey = memory_qp->mr->lkey;

    send_wr.wr_id = (uint64_t)memory_qp;
    send_wr.sg_list = &sge;
    send_wr.num_sge = 1;
    send_wr.opcode = IBV_WR_SEND;
	send_wr.send_flags = IBV_SEND_SIGNALED;

    if(ibv_post_send(id->qp, &send_wr, &bad_wr))
        error_handing("ibv_post_send() error");
    return 0;
}

void * poll_cq(void * args)
{
    struct rdma_cm_id *id= (struct rdma_cm_id *)args;
    struct ibv_cq *output_cq = NULL;
    struct ibv_wc wc = {};
    void * cq_contex = NULL;
    while(1)
    {
        if(ibv_get_cq_event(s_connection->comp_channel, &output_cq, &cq_contex) == -1)
            error_handing("ibv_get_cq_event() error");
        ibv_ack_cq_events(output_cq, 1);
        if(ibv_req_notify_cq(output_cq, 0) == -1)
            error_handing("ibv_req_notify_cq() error");
        while (ibv_poll_cq(output_cq, 1, &wc))   //从cq中产生一个cqe  存入wc中
            on_completion(&wc, id);
   }
    return NULL;

}

void on_completion(struct ibv_wc *wc, struct rdma_cm_id *id)
{
    struct memory * memory_qp = (struct memory *)wc->wr_id; 
    if (wc->status != IBV_WC_SUCCESS)
        error_handing("on_completion: status is not IBV_WC_SUCCESS.");
    else if(wc->opcode == IBV_WC_SEND)
    {
        printf("send success\n");
        rdma_disconnect(id);
    }
}

int conn_disconnected(struct rdma_cm_id *id)
{
    printf("disconnected.\n");
    rdma_destroy_qp(id);
    rdma_destroy_id(id);
    return 1;
}

void free_memory(struct memory *memory_qp)
{
    ibv_dereg_mr(memory_qp->mr);
    free(memory_qp->send_region);
    free(memory_qp);
}

void error_handing(char * message)
{
    fputs(message, stderr);
    fputs("\n", stderr);
    exit(1);
}