#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<stdint.h>
#include<sys/socket.h>
#include<rdma/rdma_cma.h>
#include <sys/types.h>
#include <arpa/inet.h>

struct pdata{
    uint64_t buf_data;
    uint32_t buf_rkey;
};

void error_handing(char * message);
//void build_pd_cq(struct rdma_cm_id * listen_id, struct ibv_pd * pd, struct ibv_comp_channel * comp_channel, struct ibv_cq * cq);
//void build_qp(struct rdma_cm_id *listen_id, struct ibv_pd *pd, struct ibv_mr *data_mr, struct ibv_cq *cq, uint64_t * data_mr_buf, struct ibv_qp_init_attr * qp_init_attr);

int main(int argc, char * argv[])
{
    struct rdma_event_channel * ec;
    struct rdma_cm_id * listen_id;
    struct sockaddr_in server_addr;
    struct ibv_pd * pd = NULL;
    struct ibv_comp_channel * comp_channel;
    struct ibv_cq * cq;
    struct ibv_mr * data_mr;
    struct rdma_conn_param conn_param = {};
    struct pdata  p_data = {};
    struct rdma_cm_event *event;
    struct ibv_qp_init_attr qp_init_attr = {};

    if(argc != 2)
    {
        printf("usage: %s <port>\n", argv[0]);
        exit(1);
    }
    ec = rdma_create_event_channel();  //创建rdma事件通道

    if(ec == NULL)
        error_handing("rdma_create_event_channel() error");
    
    if(rdma_create_id(ec, &listen_id, NULL, RDMA_PS_TCP) == -1)  //创建rdma_cm_id
        error_handing("rdma_create_id() error");
    
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(atoi(argv[1]));
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if(rdma_bind_addr(listen_id, (struct sockaddr *)& server_addr) == -1)  //绑定listen_id
        error_handing("rdma_bind_addr() error");

    if(rdma_listen(listen_id, 5) == -1)   //server开启监听，监听到客户端的rdma_connect()时会产生事件RDMA_CM_EVENT_REQUEST
        error_handing("rdma_listen() error");
	
    printf("listenaaaaa\n");
    if(rdma_get_cm_event(ec, &event) == -1)           //调用检测事件函数
        error_handing("1.rdma_get_cm_event() error");
    
    if(event -> event != RDMA_CM_EVENT_CONNECT_REQUEST)  
        error_handing("RDMA_CM_EVENT_REQUEST error");
    printf("listenbbbbb\n");
//    build_pd_cq(listen_id, pd, comp_channel, cq);   //创建verbs的pd和cq
    listen_id = event->id;
    rdma_ack_cm_event(event);

    pd = ibv_alloc_pd(listen_id->verbs);
    if(pd == NULL)
        error_handing("ibv_alloc_pd() error");
    
    comp_channel = ibv_create_comp_channel(listen_id->verbs);
    if(comp_channel == NULL)
        error_handing("ibv_create_comp_channel() error");

    cq = ibv_create_cq(listen_id->verbs, 2, NULL, comp_channel, 0);
    if(cq == NULL)
        error_handing("ibv_create_cq() error");

    uint32_t * data_mr_buf = calloc(2, sizeof (uint32_t)); 

//    build_qp(listen_id, pd, data_mr, cq, data_mr_buf, &qp_init_attr);   //创建qp
    qp_init_attr.send_cq = cq;
    qp_init_attr.recv_cq = cq;
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.cap.max_send_wr = 1;
    qp_init_attr.cap.max_recv_wr = 1;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;

    data_mr = ibv_reg_mr(pd, data_mr_buf, sizeof(data_mr_buf), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    if(data_mr == NULL)
        error_handing("ibv_reg_mr() error");

    if(rdma_create_qp(listen_id, pd, &qp_init_attr) == -1)
        error_handing("rdma_create_qp() error");

    p_data.buf_data = be64toh((unsigned long long) data_mr_buf);  //封装数据到client
    p_data.buf_rkey = be32toh(data_mr->rkey);  //将server注册内存的lkey发送到client

    conn_param.responder_resources = 1;  
    conn_param.private_data = &p_data; //将它们封装到连接参数conn_param中
    conn_param.private_data_len = sizeof(p_data);

    data_mr_buf[0]=0x123;

    rdma_accept(listen_id, &conn_param);  //接受客户端的连接,连接成功后会产生事件RDMA_CM_EVENT_ESTABLISHED

    if(rdma_get_cm_event(ec, &event) == -1)           //调用检测事件函数
        error_handing("2.rdma_get_cm_event() error");
    
    if(event -> event != RDMA_CM_EVENT_ESTABLISHED)  
        error_handing("RDMA_CM_EVENT_ESTABLISHED error");
    rdma_ack_cm_event(event);


    if(rdma_get_cm_event(ec, &event) == -1)           //调用检测事件函数
        error_handing("3.rdma_get_cm_event() error");
    
    if(event -> event != RDMA_CM_EVENT_DISCONNECTED)  
        error_handing("RDMA_CM_EVENT_DISCONNECTED error");

    if(event -> event == RDMA_CM_EVENT_DISCONNECTED)  
        printf("RDMA_CM_EVENT_DISCONNECTED\n");

    rdma_ack_cm_event(event);

    rdma_destroy_qp(listen_id);
    ibv_dereg_mr(data_mr);
    free(data_mr_buf);
    rdma_destroy_id(listen_id);
    rdma_destroy_event_channel(ec);
    return 0;
}

// void build_pd_cq(struct rdma_cm_id * listen_id, struct ibv_pd * pd, struct ibv_comp_channel * comp_channel, struct ibv_cq * cq)
// {
//     pd = ibv_alloc_pd(listen_id->verbs);
//     if(pd == NULL)
//         error_handing("ibv_alloc_pd() error");
    
//     comp_channel = ibv_create_comp_channel(listen_id->verbs);
//     if(comp_channel == NULL)
//         error_handing("ibv_create_comp_channel() error");

//     cq = ibv_create_cq(listen_id->verbs, 2, NULL, comp_channel, 0);
//     if(cq == NULL)
//         error_handing("ibv_create_cq() error");

// }

// void build_qp(struct rdma_cm_id *listen_id, struct ibv_pd *pd, struct ibv_mr *data_mr, struct ibv_cq *cq, uint64_t * data_mr_buf, struct ibv_qp_init_attr * qp_init_attr)
// {
//     qp_init_attr->send_cq = cq;
//     qp_init_attr->recv_cq = cq;
//     qp_init_attr->qp_type = IBV_QPT_RC;
//     qp_init_attr->cap.max_send_wr = 1;
//     qp_init_attr->cap.max_recv_wr = 1;
//     qp_init_attr->cap.max_send_sge = 1;
//     qp_init_attr->cap.max_recv_sge = 1;

//     data_mr = ibv_reg_mr(pd, data_mr_buf, sizeof(data_mr_buf), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
//     if(data_mr == NULL)
//         error_handing("ibv_reg_mr() error");

//     if(rdma_create_qp(listen_id, pd, qp_init_attr) == -1)
//         error_handing("rdma_create_qp() error");
// }

void error_handing(char * message)
{
    fputs(message, stderr);
    fputs("\n",stderr);
    exit(1);
}
