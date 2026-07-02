#include <linux/module.h>
#include <linux/fs.h>
#include <linux/namei.h>
#include <linux/nsproxy.h>
#include <linux/net.h>
#include <linux/socket.h>
#include <linux/netlink.h>
#include <linux/sched.h>
#include <linux/uaccess.h>
#include <linux/slab.h>
#include <linux/dcache.h>
#include <linux/inet.h>
#include <linux/list.h>
#include <linux/string.h>

#include "powerfs.h"

#define MODULE_NAME "powerfs"
#define POWERFS_VERSION "0.1.0"

static struct file_system_type powerfs_fs_type;
static struct super_block *powerfs_sb;

static int powerfs_fill_super(struct super_block *sb, void *data, int silent);
static struct dentry *powerfs_get_root(struct super_block *sb);

static struct dentry *powerfs_mount(struct file_system_type *fs_type,
				    int flags, const char *dev_name,
				    void *data)
{
	return mount_nodev(fs_type, flags, data, powerfs_fill_super);
}

static struct file_system_type powerfs_fs_type = {
	.owner		= THIS_MODULE,
	.name		= MODULE_NAME,
	.mount		= powerfs_mount,
	.kill_sb	= kill_litter_super,
	.fs_flags	= FS_REQUIRES_DEV,
};

extern int powerfs_statfs(struct dentry *dentry, struct kstatfs *buf);

static struct inode *powerfs_alloc_inode(struct super_block *sb)
{
	struct powerfs_inode_info *pi;

	pi = kzalloc(sizeof(struct powerfs_inode_info), GFP_KERNEL);
	if (!pi)
		return NULL;

	INIT_LIST_HEAD(&pi->children);
	pi->data = NULL;
	pi->size = 0;
	pi->capacity = 0;
	pi->parent = NULL;
	pi->nlink = 0;

	return &pi->vfs_inode;
}

static void powerfs_evict_inode(struct inode *inode)
{
	truncate_inode_pages_final(&inode->i_data);
	clear_inode(inode);
}

static void powerfs_destroy_inode(struct inode *inode)
{
	struct powerfs_inode_info *pi = powerfs_i(inode);

	if (pi->data)
		kfree(pi->data);
	kfree(pi);
}

static const struct super_operations powerfs_super_operations = {
	.statfs		= powerfs_statfs,
	.alloc_inode	= powerfs_alloc_inode,
	.destroy_inode	= powerfs_destroy_inode,
	.evict_inode	= powerfs_evict_inode,
};

struct inode *powerfs_get_inode(struct super_block *sb, umode_t mode)
{
	struct powerfs_inode_info *pi;
	struct inode *inode;

	inode = new_inode(sb);
	if (!inode)
		return NULL;

	pi = powerfs_i(inode);

	inode->i_mode = mode;
	inode->i_uid = current_fsuid();
	inode->i_gid = current_fsgid();
	inode->i_atime_sec = inode->i_mtime_sec = inode->i_ctime_sec =
		current_time(inode).tv_sec;
	inode->i_atime_nsec = inode->i_mtime_nsec = inode->i_ctime_nsec =
		current_time(inode).tv_nsec;
	inode->i_blkbits = PAGE_SHIFT;
	inode->i_blocks = 0;

	if (S_ISDIR(mode)) {
		inode->i_op = &powerfs_dir_inode_operations;
		inode->i_fop = &powerfs_dir_fops;
		set_nlink(inode, 2);
	} else if (S_ISREG(mode)) {
		inode->i_op = &powerfs_file_inode_operations;
		inode->i_fop = &powerfs_file_fops;
		set_nlink(inode, 1);
	} else if (S_ISLNK(mode)) {
		inode->i_op = &powerfs_file_inode_operations;
		set_nlink(inode, 1);
	}

	return inode;
}

static struct dentry *powerfs_get_root(struct super_block *sb)
{
	struct inode *root_inode = powerfs_get_inode(sb, S_IFDIR | S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
	struct dentry *root_dentry;
	struct powerfs_inode_info *pi;

	if (!root_inode)
		return ERR_PTR(-ENOMEM);

	pi = powerfs_i(root_inode);
	pi->nlink = 2;
	pi->name[0] = '/';
	pi->name[1] = '\0';

	root_inode->i_op = &powerfs_dir_inode_operations;
	root_inode->i_fop = &powerfs_dir_fops;

	root_dentry = d_make_root(root_inode);
	if (!root_dentry) {
		kfree(pi);
		return ERR_PTR(-ENOMEM);
	}

	return root_dentry;
}

static int powerfs_fill_super(struct super_block *sb, void *data, int silent)
{
	struct dentry *root;

	sb->s_magic = POWERFS_MAGIC;
	sb->s_maxbytes = MAX_LFS_FILESIZE;
	sb->s_blocksize_bits = PAGE_SHIFT;
	sb->s_blocksize = PAGE_SIZE;
	sb->s_op = &powerfs_super_operations;

	root = powerfs_get_root(sb);
	if (IS_ERR(root))
		return PTR_ERR(root);

	sb->s_root = root;
	powerfs_sb = sb;

	pr_info("PowerFS: mounted superblock at %p\n", sb);

	return 0;
}

extern int powerfs_transport_init(void);
extern void powerfs_transport_cleanup(void);

static int __init powerfs_init(void)
{
	int ret;

	pr_info("PowerFS: Initializing kernel module v%s\n", POWERFS_VERSION);

	ret = powerfs_transport_init();
	if (ret) {
		pr_err("PowerFS: Failed to initialize transport: %d\n", ret);
		return ret;
	}

	ret = register_filesystem(&powerfs_fs_type);
	if (ret) {
		pr_err("PowerFS: Failed to register filesystem: %d\n", ret);
		powerfs_transport_cleanup();
		return ret;
	}

	pr_info("PowerFS: Registered filesystem successfully\n");

	return 0;
}

static void __exit powerfs_exit(void)
{
	unregister_filesystem(&powerfs_fs_type);
	powerfs_transport_cleanup();
	pr_info("PowerFS: Unregistered filesystem\n");
	pr_info("PowerFS: Kernel module unloaded\n");
}

module_init(powerfs_init);
module_exit(powerfs_exit);

MODULE_LICENSE("GPL");
MODULE_AUTHOR("PowerFS Team <jiangjinhu@fudan.edu.cn>");
MODULE_DESCRIPTION("PowerFS Linux Kernel Module - Zero-jitter unified parallel file system");
MODULE_VERSION(POWERFS_VERSION);
MODULE_ALIAS_FS(MODULE_NAME);
