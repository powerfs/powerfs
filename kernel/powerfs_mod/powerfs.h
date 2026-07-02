#ifndef POWERFS_H
#define POWERFS_H

#include <linux/fs.h>
#include <linux/netlink.h>
#include <linux/list.h>
#include <linux/hash.h>

#define POWERFS_MAGIC 0x5046534b
#define POWERFS_NETLINK_FAMILY 31

#define POWERFS_MAX_FILENAME 256
#define POWERFS_MAX_DATA_SIZE (4 * 1024 * 1024)

#define powerfs_i(inode) container_of(inode, struct powerfs_inode_info, vfs_inode)

struct powerfs_inode_info {
	struct inode vfs_inode;
	char *data;
	unsigned long size;
	unsigned long capacity;
	char symlink_target[POWERFS_MAX_FILENAME];
	struct list_head children;
	struct list_head sibling;
	struct powerfs_inode_info *parent;
	char name[POWERFS_MAX_FILENAME];
	unsigned int nlink;
};

struct powerfs_sb_info {
	struct net *net;
	struct sock *nl_sock;
	struct list_head root_inodes;
	spinlock_t inode_lock;
	unsigned long next_inode_id;
};

extern const struct inode_operations powerfs_dir_inode_operations;
extern const struct inode_operations powerfs_file_inode_operations;
extern const struct file_operations powerfs_dir_fops;
extern const struct file_operations powerfs_file_fops;

struct inode *powerfs_get_inode(struct super_block *sb, umode_t mode);

#endif
