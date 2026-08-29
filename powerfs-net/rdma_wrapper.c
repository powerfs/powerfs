/*
 * C wrapper for libibverbs inline functions and macros.
 *
 * ibv_poll_cq, ibv_post_send, ibv_post_recv, ibv_req_notify_cq are defined as
 * `static inline` in <infiniband/verbs.h> and are NOT exported from
 * libibverbs.so. Rust FFI cannot call inline functions directly, so we provide
 * thin C wrappers that the Rust side calls via FFI.
 *
 * ibv_query_port is a macro that expands to ___ibv_query_port (not exported).
 * The exported ibv_query_port symbol is the OLD compat version that uses a
 * smaller struct layout. Calling it from Rust with the new struct layout
 * would read fields at wrong offsets. This wrapper uses the macro (which
 * correctly calls ___ibv_query_port and handles struct size detection).
 */

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

int powerfs_ibv_poll_cq(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc)
{
    return ibv_poll_cq(cq, num_entries, wc);
}

int powerfs_ibv_post_send(struct ibv_qp *qp, struct ibv_send_wr *wr,
                          struct ibv_send_wr **bad_wr)
{
    int rc = ibv_post_send(qp, wr, bad_wr);
    if (rc != 0) {
        fprintf(stderr, "[rdma] post_send failed: rc=%d qp_num=%u "
                "opcode=%d errno=%d\n",
                rc, qp->qp_num, wr->opcode, errno);
    }
    return rc;
}

int powerfs_ibv_post_recv(struct ibv_qp *qp, struct ibv_recv_wr *wr,
                          struct ibv_recv_wr **bad_recv_wr)
{
    int rc = ibv_post_recv(qp, wr, bad_recv_wr);
    if (rc != 0) {
        fprintf(stderr, "[rdma] post_recv failed: rc=%d qp_num=%u errno=%d\n",
                rc, qp->qp_num, errno);
    }
    return rc;
}

int powerfs_ibv_req_notify_cq(struct ibv_cq *cq, int solicited_only)
{
    return ibv_req_notify_cq(cq, solicited_only);
}

int powerfs_ibv_query_port(struct ibv_context *context, uint8_t port_num,
                           struct ibv_port_attr *port_attr)
{
    /* Uses the macro, which expands to ___ibv_query_port and correctly
     * handles struct size detection via verbs_context. */
    return ibv_query_port(context, port_num, port_attr);
}

/* Wrapper for rdma_create_qp: constructs ibv_qp_init_attr in C to avoid
 * any Rust struct layout mismatch. */
int powerfs_rdma_create_qp(struct rdma_cm_id *id, struct ibv_pd *pd,
                           void *qp_context,
                           struct ibv_cq *send_cq, struct ibv_cq *recv_cq,
                           struct ibv_srq *srq,
                           uint32_t max_send_wr, uint32_t max_recv_wr,
                           uint32_t max_send_sge, uint32_t max_recv_sge,
                           uint32_t max_inline_data,
                           int qp_type, int sq_sig_all)
{
    struct ibv_qp_init_attr init;
    memset(&init, 0, sizeof(init));
    init.qp_context = qp_context;
    init.send_cq = send_cq;
    init.recv_cq = recv_cq;
    init.srq = srq;
    init.cap.max_send_wr = max_send_wr;
    init.cap.max_recv_wr = max_recv_wr;
    init.cap.max_send_sge = max_send_sge;
    init.cap.max_recv_sge = max_recv_sge;
    init.cap.max_inline_data = max_inline_data;
    init.qp_type = qp_type;
    init.sq_sig_all = sq_sig_all;

    int rc = rdma_create_qp(id, pd, &init);
    if (rc != 0) {
        fprintf(stderr, "[rdma] rdma_create_qp failed: rc=%d errno=%d (%s)\n",
                rc, errno, strerror(errno));
    }
    return rc;
}

/* Wrapper for rdma_connect: constructs conn_param in C. */
int powerfs_rdma_connect(struct rdma_cm_id *id,
                         uint8_t responder_resources,
                         uint8_t initiator_depth,
                         uint8_t flow_control,
                         uint8_t retry_count,
                         uint8_t rnr_retry_count,
                         uint8_t srq,
                         uint32_t qp_num)
{
    struct rdma_conn_param param;
    memset(&param, 0, sizeof(param));
    param.private_data = NULL;
    param.private_data_len = 0;
    param.responder_resources = responder_resources;
    param.initiator_depth = initiator_depth;
    param.flow_control = flow_control;
    param.retry_count = retry_count;
    param.rnr_retry_count = rnr_retry_count;
    param.srq = srq;
    param.qp_num = qp_num;
    return rdma_connect(id, &param);
}

/* Wrapper for rdma_accept: constructs conn_param in C. */
int powerfs_rdma_accept(struct rdma_cm_id *id,
                         uint8_t responder_resources,
                         uint8_t initiator_depth,
                         uint8_t flow_control,
                         uint8_t retry_count,
                         uint8_t rnr_retry_count,
                         uint8_t srq,
                         uint32_t qp_num)
{
    struct rdma_conn_param param;
    memset(&param, 0, sizeof(param));
    param.private_data = NULL;
    param.private_data_len = 0;
    param.responder_resources = responder_resources;
    param.initiator_depth = initiator_depth;
    param.flow_control = flow_control;
    param.retry_count = retry_count;
    param.rnr_retry_count = rnr_retry_count;
    param.srq = srq;
    param.qp_num = qp_num;
    return rdma_accept(id, &param);
}
