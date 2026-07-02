#include <linux/netlink.h>
#include <linux/socket.h>
#include <linux/net.h>
#include <linux/inet.h>
#include <linux/udp.h>
#include <linux/slab.h>
#include <linux/uaccess.h>

#include "powerfs.h"

static struct sock *powerfs_nl_sock;

static void powerfs_nl_rcv_msg(struct sk_buff *skb)
{
	struct nlmsghdr *nlh = nlmsg_hdr(skb);

	pr_info("PowerFS: Received netlink message type=%u len=%u\n",
		nlh->nlmsg_type, nlh->nlmsg_len);
}

static int powerfs_nl_send_msg(u32 pid, u32 type, void *data, size_t len)
{
	struct sk_buff *skb;
	struct nlmsghdr *nlh;
	int ret;

	skb = nlmsg_new(len, GFP_KERNEL);
	if (!skb)
		return -ENOMEM;

	nlh = nlmsg_put(skb, 0, 0, type, len, 0);
	if (!nlh) {
		kfree_skb(skb);
		return -ENOMEM;
	}

	memcpy(nlmsg_data(nlh), data, len);

	ret = netlink_unicast(powerfs_nl_sock, skb, pid, MSG_DONTWAIT);
	if (ret < 0)
		return ret;

	return 0;
}

static struct netlink_kernel_cfg powerfs_nl_cfg = {
	.groups		= 1,
	.input		= powerfs_nl_rcv_msg,
};

int powerfs_transport_init(void)
{
	powerfs_nl_sock = netlink_kernel_create(&init_net, POWERFS_NETLINK_FAMILY, &powerfs_nl_cfg);
	if (!powerfs_nl_sock) {
		pr_err("PowerFS: Failed to create netlink socket\n");
		return -ENOMEM;
	}

	pr_info("PowerFS: Netlink socket created successfully\n");
	return 0;
}

void powerfs_transport_cleanup(void)
{
	if (powerfs_nl_sock) {
		netlink_kernel_release(powerfs_nl_sock);
		powerfs_nl_sock = NULL;
		pr_info("PowerFS: Netlink socket released\n");
	}
}

int powerfs_send_request(u32 pid, u32 opcode, u64 inode, void *data, size_t len)
{
	return powerfs_nl_send_msg(pid, opcode, data, len);
}
