#include<stdio.h>
#include<stdlib.h>
#include<sys/socket.h>
#include<rdma/rdma_cma.h>
#include<stdint.h>
#include<string.h>
#include<netdb.h>

#define TIMEOUT_MS 5000
#define BUFF_SIZE 100
struct pdata{
    uint64_t buf_addr;
    uint32_t buf_rkey;
};

void error_handing(char * message);
//void build_pd_cq(struct rdma_cm_id *client_id, struct ibv_pd *pd, struct ibv_comp_channel * comp_channel, struct ibv_cq * cq);
//void build_qp(struct rdma_cm_id * client_id, struct ibv_pd *pd, struct ibv_mr * read_receive_mr, struct ibv_cq * cq, uint32_t * read_receive_mr_buff, struct ibv_qp_init_attr *qp_init_attr);
//void build_wr(struct ibv_send_wr *wr, struct pdata *server_data, struct ibv_mr * read_receive_mr, uint32_t * read_receive_mr_buff, struct ibv_sge * sge);
// void * poll_cq(struct ibv_comp_channel * comp_channel, struct ibv_cq * cq, struct ibv_context * context);
// void on_completion(struct ibv_send_wr *wr, struct ibv_wc *wc, uint64_t * read_receive_mr_buff);
// void on_completion(struct ibv_wc *wc, uint32_t * read_receive_mr_buff);
// void * poll_cq(struct ibv_comp_channel * comp_channel, struct ibv_context * context, uint32_t * read_receive_mr_buff);

int main(int argc, char * argv[])
{
    struct rdma_cm_id * client_id;
    struct rdma_event_channel * ec;
    struct rdma_cm_event * event;
    struct ibv_context *context;
    struct ibv_pd * pd = NULL;
    struct addrinfo	hints = { 
   		.ai_family    = AF_INET,
   		.ai_socktype  = SOCK_STREAM
   	};

    struct addrinfo *result;

    struct ibv_comp_channel * comp_channel;

    struct ibv_cq * cq;

    struct ibv_mr * read_receive_mr = NULL;

    struct ibv_qp_init_attr qp_init_attr = {};

    struct rdma_conn_param conn_param = {};

    struct ibv_send_wr wr = {};

    struct pdata server_data = {};

    struct ibv_send_wr *bad_wr;

    pthread_t cq_poller_thread;

    struct ibv_sge sge = {};
    uint32_t * read_receive_mr_buff = NULL;

    if(argc != 3)
    {
        printf("usage: %s <server-ip> <port>", argv[0]);
        exit(1);
    }
    
    ec = rdma_create_event_channel();  //创建rdma事件通道
    if(ec == NULL)
        error_handing("rdma_create_event_channel() error");
    
    if(rdma_create_id(ec, &client_id, NULL, RDMA_PS_TCP) == -1)  //创建id
        error_handing("rdma_create_id() error");

    if(getaddrinfo(argv[1], argv[2], &hints, &result))   //解析地址信息将结果存到result
        error_handing("getaddrinfo() error");

    if(rdma_resolve_addr(client_id, NULL, result->ai_addr, TIMEOUT_MS) == -1)  //将地址信息解析为rdma地址信息，此时会产生事件RDMA_CM_EVENT_ADDR_RESOLVED
        error_handing("rdma_resolve_addr() error");

    if(rdma_get_cm_event(ec, &event) == -1)           //调用检测事件函数
        error_handing("1.rdma_get_cm_event() error");

    if(event -> event != RDMA_CM_EVENT_ADDR_RESOLVED)  
        error_handing("RDMA_CM_EVENT_ADDR_RESOLVED error");

    rdma_ack_cm_event(event);   //释放该事件
  //  freeaddrinfo(result);

    if(rdma_resolve_route(client_id, TIMEOUT_MS) == -1)   //解析RDMA路由到目的地址，解析完后会产生事件RDMA_CM_EVENT_ROUTE_RESOLVED
        error_handing("rdma_resolve_route() error");

    if(rdma_get_cm_event(ec, &event) == -1)           //调用检测事件函数
        error_handing("2.rdma_get_cm_event() error");
    
    if(event -> event != RDMA_CM_EVENT_ROUTE_RESOLVED)  
        error_handing("RDMA_CM_EVENT_ROUTE_RESOLVED error");

    rdma_ack_cm_event(event);   //释放该事件

    pd = ibv_alloc_pd(client_id->verbs);  //为verbs创建保护域
    if(pd == NULL)
        error_handing("ibv_alloc_pd() error");

    comp_channel = ibv_create_comp_channel(client_id->verbs);  //为verbs创建完成通道
    if(comp_channel == NULL)
        error_handing("ibv_create_comp_channel() error");

    cq = ibv_create_cq(client_id->verbs, 2, NULL, comp_channel, 0);  //为verbs指定完成队列

    if(cq == NULL)
        error_handing("ibv_create_cq() error");

    if(ibv_req_notify_cq(cq, 0) == -1)
        error_handing("1.ibv_req_notify_cq() error");

//    pthread_create(&cq_poller_thread, NULL, poll_cq, NULL);

//    build_pd_cq(client_id, pd, comp_channel, cq);
    
    read_receive_mr_buff = calloc(2, sizeof (uint32_t));  //开辟一段内存buff

    if(read_receive_mr == NULL)
        printf("read_receive_mr == NULL \n");
    read_receive_mr = ibv_reg_mr(pd, read_receive_mr_buff, 2 * sizeof(uint32_t), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);  //注册内存MR
  //  read_receive_mr = ibv_reg_mr(pd, read_receive_mr_buff, BUFF_SIZE, 0);
    if(read_receive_mr == NULL)
        error_handing("ibv_reg_mr() error");
//    build_qp(client_id,pd,read_receive_mr, cq, read_receive_mr_buff, &qp_init_attr);

    qp_init_attr.send_cq = cq;
    qp_init_attr.recv_cq = cq;
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.cap.max_send_wr = 2;
    qp_init_attr.cap.max_recv_wr = 1;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;
    
    if(rdma_create_qp(client_id, pd, &qp_init_attr) == -1)
        error_handing("rdma_create_qp() error");

    conn_param.initiator_depth = 1;
	conn_param.retry_count = 7;

    if(rdma_connect(client_id, &conn_param) == -1)   //请求与服务器连接，连接成功后会产生事件RDMA_CM_EVENT_ESTABLISHED
        error_handing("rdma_connect() error");

    if(rdma_get_cm_event(ec, &event) == -1)           //调用检测事件函数
        error_handing("3.rdma_get_cm_event() error");
    
    if(event -> event != RDMA_CM_EVENT_ESTABLISHED)  
        error_handing("RDMA_CM_EVENT_ESTABLISHED error");
    
  //  rdma_ack_cm_event(event);   //释放该事件
    
    memcpy(&server_data, event->param.conn.private_data, sizeof(server_data));

    rdma_ack_cm_event(event);   //释放该事件

//    build_wr(wr, &server_data, read_receive_mr, read_receive_mr_buff, sge);
    sge.addr = (uint64_t)read_receive_mr_buff;
    sge.length = sizeof(uint32_t);
    sge.lkey = read_receive_mr->lkey;

        
    wr.wr_id = 1;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.wr.rdma.remote_addr = be64toh(server_data.buf_addr);
    wr.wr.rdma.rkey = be32toh(server_data.buf_rkey);

    printf("rkey:%llx\n", (unsigned long long)wr.wr.rdma.rkey);
	printf("remote_addr:%llx\n", (unsigned long long)wr.wr.rdma.remote_addr);

    if(ibv_post_send(client_id->qp, &wr, &bad_wr) == -1)
        error_handing("ibv_post_send() error");

    sleep(1);
    printf("receive from server data:%d\n", read_receive_mr_buff[0]);

    if(rdma_disconnect(client_id) == -1)
         error_handing("rdma_disconnect() error");

    if(rdma_get_cm_event(ec, &event) == -1)           //调用检测事件函数
        error_handing("4.rdma_get_cm_event() error");
    
    if(event -> event != RDMA_CM_EVENT_DISCONNECTED)  
        error_handing("RDMA_CM_EVENT_DISCONNECTED error");
    rdma_ack_cm_event(event);
    printf("disconnect\n");

    rdma_destroy_qp(client_id);
    ibv_dereg_mr(read_receive_mr);
    free(read_receive_mr_buff);
    rdma_destroy_id(client_id);
    rdma_destroy_event_channel(ec);

    // if(rdma_get_cm_event(ec, &event) == -1)           //调用检测事件函数
    //     error_handing("4.rdma_get_cm_event() error");
    
    // if(event -> event != RDMA_CM_EVENT_DISCONNECTED)  
    //     error_handing("RDMA_CM_EVENT_DISCONNECTED error");
    
    // rdma_ack_cm_event(event);   //释放该事件

    // printf("disconnected.\n");
    // rdma_destroy_qp(client_id);
    // ibv_dereg_mr(read_receive_mr);
    // free(read_receive_mr_buff);
    // rdma_destroy_id(client_id);

    return 0;

}

// void build_pd_cq(struct rdma_cm_id *client_id, struct ibv_pd *pd, struct ibv_comp_channel * comp_channel, struct ibv_cq * cq)
// {
//     pd = ibv_alloc_pd(client_id->verbs);  //为verbs创建保护域
//     if(pd == NULL)
//         error_handing("ibv_alloc_pd() error");

//     comp_channel = ibv_create_comp_channel(client_id->verbs);  //为verbs创建完成通道
//     if(comp_channel == NULL)
//         error_handing("ibv_create_comp_channel() error");

//     cq = ibv_create_cq(client_id->verbs, 2, NULL, comp_channel, 0);  //为verbs指定完成队列

//     if(cq == NULL)
//         error_handing("ibv_create_cq() error");

//     if(ibv_req_notify_cq(cq, 0) == -1)
//         error_handing("1.ibv_req_notify_cq() error");

// //   pthread_create(cq_poller_thread, NULL, poll_cq, NULL);
    
// }


// void build_qp(struct rdma_cm_id * client_id, struct ibv_pd *pd, struct ibv_mr * read_receive_mr, struct ibv_cq * cq, uint32_t * read_receive_mr_buff, struct ibv_qp_init_attr *qp_init_attr)
// {
//     qp_init_attr->send_cq = cq;
//     qp_init_attr->recv_cq = cq;
//     qp_init_attr->qp_type = IBV_QPT_RC;
//     qp_init_attr->cap.max_send_wr = 1;
//     qp_init_attr->cap.max_recv_wr = 1;
//     qp_init_attr->cap.max_send_sge = 1;
//     qp_init_attr->cap.max_recv_sge = 1;
    
//     if(rdma_create_qp(client_id, pd, qp_init_attr) == -1)
//         error_handing("rdma_create_qp() error");
// }

// void build_wr(struct ibv_send_wr *wr, struct pdata *server_data, struct ibv_mr * read_receive_mr, uint32_t * read_receive_mr_buff, struct ibv_sge * sge)
// {
//     sge->addr = (uint64_t)read_receive_mr_buff;
//     sge->length = sizeof(uint32_t);
//     sge->lkey = read_receive_mr->lkey;

        
//     wr->wr_id = 1;
//     wr->sg_list = sge;
//     wr->num_sge = 1;
//     wr->opcode = IBV_WR_RDMA_READ;
//     wr->wr.rdma.remote_addr = server_data->buf_addr;
//     wr->wr.rdma.rkey = server_data->buf_rkey;

// }

// void * poll_cq(struct ibv_comp_channel * comp_channel, struct ibv_context * context, uint32_t * read_receive_mr_buff)
// {
//     struct ibv_cq *cq;
//     struct ibv_wc wc;
//     while(1)
//     {
//         if(ibv_get_cq_event(comp_channel, &cq, (void **)&context) == -1)
//             error_handing("ibv_get_cq_event() error");

//         ibv_ack_cq_events(cq, 1);

//         if(ibv_req_notify_cq(cq, 0) == -1);
//             error_handing("2.ibv_req_notify_cq() error");

//         while (ibv_poll_cq(cq, 1, &wc))
//             on_completion(&wc, read_receive_mr_buff);
//     }
//     return NULL;

// }

// void on_completion(struct ibv_wc *wc, uint32_t * read_receive_mr_buff)
// {
//   if (wc->status != IBV_WC_SUCCESS)
//     error_handing("on_completion: status is not IBV_WC_SUCCESS.");

//   if (wc->opcode & IBV_WC_RECV)
//     printf("aa:received message: %d\n", read_receive_mr_buff[0]);
//   else if (wc->opcode == IBV_WC_SEND)
//     printf("send completed successfully.\n");
//   else
//     error_handing("on_completion: completion isn't a send or a receive.");

//     rdma_disconnect((struct rdma_cm_id *)wc->wr_id);
// }

void error_handing(char * message)
{
    fputs(message, stderr);
    fputs("\n",stderr);
    exit(1);
}
