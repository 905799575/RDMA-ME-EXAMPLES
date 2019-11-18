#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<arpa/inet.h>
#include<stdint.h>
#include<sys/socket.h>
#include<sys/types.h>
#include<rdma/rdma_cma.h>

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
    char * recv_region;
    struct ibv_mr * mr;
};

struct pdata
{
    char * recv_region;
    uint32_t rkey;
};

void error_handing(char * message);
void build_pd_cq(struct rdma_cm_id *id);
int on_event(struct rdma_cm_event * event, struct memory_qp * memory_qp);
int client_connect_request(struct rdma_cm_id *id, struct memory_qp * memory_qp);
void register_memory(struct memory_qp * memory_qp);
void bulid_qp(struct ibv_qp_init_attr *qp_init_attr, struct rdma_cm_id *id);
void bulid_conn_param(struct rdma_conn_param *conn_param, struct pdata *p_data, struct memory_qp * memory_qp);
int conn_established(struct rdma_cm_id *id, struct memory_qp * memory_qp);
// void * poll_cq(void * args);
// void on_completion(struct ibv_wc *wc, struct rdma_cm_id *id);
 int conn_disconnected(struct rdma_cm_id *id);
void free_memory(struct memory_qp *memory_qp);

struct connect *s_connect = NULL;

int main(int argc, char * argv[])
{
    if(argc != 2)
    {
        printf("usage: %s <port>\n", argv[0]);
        exit(1);
    } 
    struct rdma_event_channel * ec = NULL; 
    struct rdma_cm_id * listen_id = NULL;
    struct sockaddr_in server_addr = {};
    struct rdma_cm_event *event = NULL;
    struct memory_qp * memory_qp = (struct memory_qp *)malloc(sizeof(memory_qp));
    s_connect = (struct connect *)malloc(sizeof(struct connect));
    memory_qp->recv_region = (char *)malloc(BUFF_SIZE);
    if(memory_qp->recv_region == NULL)
        error_handing("malloc recv_region error");

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(atoi(argv[1]));


    ec = rdma_create_event_channel();  //创建RDMA事件通道

    if(ec == NULL)
        error_handing("rdma_create_event_channel() error");
    
    if(rdma_create_id(ec, &listen_id, NULL, RDMA_PS_TCP) == -1)  //绑定监听套接字
        error_handing("rdma_create_id() error");
    
    if(rdma_bind_addr(listen_id, (struct sockaddr *)&server_addr) == -1)    //绑定套接字的地址
        error_handing("rdma_bind_addr() error");
    
    if(rdma_listen(listen_id, 5))                //在服务器端开启监听
        error_handing("rdma_listen() error");
    
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

int on_event(struct rdma_cm_event * event, struct memory_qp * memory_qp)
{
    int ret = 0;
    if(event -> event == RDMA_CM_EVENT_CONNECT_REQUEST)
        ret = client_connect_request(event->id, memory_qp);
    else if(event -> event == RDMA_CM_EVENT_ESTABLISHED)
        ret = conn_established(event->id, memory_qp);
    else if(event-> event == RDMA_CM_EVENT_DISCONNECTED)
        ret = conn_disconnected(event->id);
    else 
        error_handing("on_event: no event");
    return ret;
}

int client_connect_request(struct rdma_cm_id *id, struct memory_qp * memory_qp)
{
    struct ibv_qp_init_attr qp_init_attr = {};
    struct pdata p_data = {};
    struct rdma_conn_param conn_param = {}; 

    build_pd_cq(id);
    register_memory(memory_qp);
    bulid_qp(&qp_init_attr, id);

    bulid_conn_param(&conn_param, &p_data, memory_qp);

//    pthread_create(&s_connect->cq_poller_thread, NULL, poll_cq, id);  //创建一个线程轮询CQ队列

    if(rdma_accept(id ,&conn_param) == -1)
        error_handing("ibv_alloc_pd() error");
    return 0;
}


void build_pd_cq(struct rdma_cm_id *id)
{
    s_connect->pd = ibv_alloc_pd(id->verbs);
    if(s_connect->pd == NULL)
        error_handing("ibv_alloc_pd() error");

    s_connect->comp_channel = ibv_create_comp_channel(id->verbs);
    if(s_connect->comp_channel == NULL)
        error_handing("ibv_create_comp_channel() error");
    
    s_connect->cq = ibv_create_cq(id->verbs, 2, NULL, s_connect->comp_channel, 0);

    if(s_connect->cq == NULL)
        error_handing("ibv_create_cq() error");
    
    if(ibv_req_notify_cq(s_connect->cq, 0) == -1)
        error_handing("ibv_req_notify_cq() error");

}

void register_memory(struct memory_qp * memory_qp)
{   
    memory_qp->mr = ibv_reg_mr(s_connect->pd, memory_qp->recv_region, BUFF_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    if(memory_qp->mr == NULL)
        error_handing("ibv_reg_mr() error");

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

void bulid_conn_param(struct rdma_conn_param *conn_param, struct pdata *p_data, struct memory_qp * memory_qp)
{
    p_data->recv_region = memory_qp->recv_region;
    p_data->rkey = memory_qp->mr->rkey;

    conn_param->responder_resources = 2;
    conn_param->private_data = p_data;
    conn_param->private_data_len = sizeof(* p_data);
}


int conn_established(struct rdma_cm_id *id, struct memory_qp * memory_qp)
{
    printf("server establishend\n");
    // while(1)
    // {
    //     printf("receive client data: %s", memory_qp->recv_region);
    //     sleep(1);
    // }
    sleep(1);
    printf("receive client data: %s", memory_qp->recv_region);
    sleep(1);
    rdma_disconnect(id);
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

    free(memory_qp->recv_region);
    free(memory_qp);
    free(s_connect);
}

void error_handing(char * message)
{
    fputs(message, stderr);
    fputs("\n" , stderr);
    exit(1);
}


