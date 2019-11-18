#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<arpa/inet.h>
#include<stdint.h>
#include<sys/socket.h>
#include<sys/types.h>
#include<rdma/rdma_cma.h>
#include<pthread.h>

#define BUFF_SIZE 100

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
    char * recv_region;
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
int conn_disconnected(struct rdma_cm_id * id);
void free_memory(struct memory *memory_qp);

struct connection * s_connection = NULL;

int main(int argc, char *argv[])
{
    struct rdma_event_channel *ec = NULL;
    struct rdma_cm_id *listen_id = NULL;
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    struct rdma_cm_event *event = NULL;

    struct memory * memory_qp = (struct memory *)malloc(sizeof(*memory_qp));
    memory_qp->recv_region = (char *)malloc(BUFF_SIZE);
    
    s_connection = (struct connection *)malloc(sizeof(*s_connection));

    if(argc != 2)
    {
        printf("usage: %s <port>\n", argv[0]);
        exit(1);
    }

    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(atoi(argv[1]));
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    ec = rdma_create_event_channel();   
    if(ec == NULL)
        error_handing("rdma_create_event_channel() error");

    if(rdma_create_id(ec, &listen_id, NULL, RDMA_PS_TCP) == -1)      //这里会分配id
        error_handing("rdma_create_id() error");

    if(rdma_bind_addr(listen_id, (struct sockaddr *)&server_addr) == -1)
        error_handing("rdma_bind_addr() error");

    if(rdma_listen(listen_id, 5) == -1)
        error_handing("rdma_listen() error");

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
    if(event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
        ret = connect_request(event->id, memory_qp);
    if(event->event == RDMA_CM_EVENT_ESTABLISHED)
        ret = conn_established(event->id, memory_qp);
    if(event->event == RDMA_CM_EVENT_DISCONNECTED)
        ret = conn_disconnected(event->id);
    return ret;
}


int connect_request(struct rdma_cm_id * id, struct memory * memory_qp)
{
    struct ibv_qp_init_attr qp_init_attr = {};
    struct rdma_conn_param conn_param = {};
    conn_param.responder_resources = 1;
    build_pd_cq(id);
    memory_register(memory_qp);
    build_qp(id, &qp_init_attr);
    s_connection->context = id->verbs;   //为轮询线程做准备,轮询线程需要该参数
    pthread_create(&s_connection->cq_poller_thread, NULL, poll_cq, id);  //创建一个线程轮询CQ队列

    post_receive(id, memory_qp);

    rdma_accept(id, &conn_param);

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
    memory_qp->mr = ibv_reg_mr(s_connection->pd, memory_qp->recv_region, BUFF_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
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


void post_receive(struct rdma_cm_id * id, struct memory * memory_qp)
{
    struct ibv_recv_wr recv_wr = {};
    struct ibv_sge sge= {};
    struct ibv_recv_wr *bad_wr = NULL;

    sge.addr = (uint64_t)memory_qp->recv_region;
    sge.length = BUFF_SIZE;
    sge.lkey = memory_qp->mr->lkey;

    recv_wr.wr_id = (uint64_t)memory_qp;
    recv_wr.next = NULL;
    recv_wr.sg_list = &sge;
    recv_wr.num_sge = 1;

    ibv_post_recv(id->qp, &recv_wr, &bad_wr);

}

void * poll_cq(void * args)
{
    struct rdma_cm_id *id= (struct rdma_cm_id *)args;
    struct ibv_cq *output_cq = NULL;
    struct ibv_wc wc = {};
    void * cq_contex = NULL;
//    printf("2222\n");
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
    else if(wc->opcode == IBV_WC_RECV)
    {
        printf("receive success\n");
        printf("receive message from client:%s\n", memory_qp->recv_region);
    }

//    rdma_disconnect(id);
}

int conn_established(struct rdma_cm_id * id, struct memory * memory_qp)
{
    return 0;
}

int conn_disconnected(struct rdma_cm_id * id)
{
    printf("disconnected\n");
    return 1;
}

void free_memory(struct memory *memory_qp)
{
    ibv_dereg_mr(memory_qp->mr);
    free(memory_qp->recv_region);
    free(memory_qp);
}

void error_handing(char * message)
{
    fputs(message, stderr);
    fputs("\n", stderr);
    exit(1);
}