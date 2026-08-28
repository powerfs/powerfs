/*
 * C wrapper for libibverbs inline functions.
 *
 * ibv_poll_cq, ibv_post_send, ibv_post_recv are defined as `static inline`
 * in <infiniband/verbs.h> and are NOT exported from libibverbs.so. Rust FFI
 * cannot call inline functions directly, so we provide thin C wrappers that
 * the Rust side calls via FFI.
 */

#include <infiniband/verbs.h>

int powerfs_ibv_poll_cq(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc)
{
    return ibv_poll_cq(cq, num_entries, wc);
}

int powerfs_ibv_post_send(struct ibv_qp *qp, struct ibv_send_wr *wr,
                          struct ibv_send_wr **bad_wr)
{
    return ibv_post_send(qp, wr, bad_wr);
}

int powerfs_ibv_post_recv(struct ibv_qp *qp, struct ibv_recv_wr *wr,
                          struct ibv_recv_wr **bad_recv_wr)
{
    return ibv_post_recv(qp, wr, bad_recv_wr);
}

int powerfs_ibv_req_notify_cq(struct ibv_cq *cq, int solicited_only)
{
    return ibv_req_notify_cq(cq, solicited_only);
}
