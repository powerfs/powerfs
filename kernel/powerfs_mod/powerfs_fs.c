#include <linux/fs.h>
#include <linux/slab.h>
#include <linux/uaccess.h>
#include <linux/dcache.h>
#include <linux/namei.h>
#include <linux/list.h>
#include <linux/spinlock.h>
#include <linux/rwsem.h>
#include <linux/string.h>
#include <linux/statfs.h>

#include "powerfs.h"

static struct dentry *powerfs_lookup(struct inode *dir, struct dentry *dentry, unsigned int flags);
static int powerfs_getattr(struct mnt_idmap *idmap, const struct path *path,
			   struct kstat *stat, u32 request_mask, unsigned int query_flags);
static int powerfs_readlink(struct dentry *dentry, char __user *buffer, int buflen);
static int powerfs_symlink(struct mnt_idmap *idmap, struct inode *dir, struct dentry *dentry, const char *symname);
static int powerfs_link(struct dentry *old_dentry, struct inode *new_dir, struct dentry *new_dentry);
static int powerfs_unlink(struct inode *dir, struct dentry *dentry);
static int powerfs_rmdir(struct inode *dir, struct dentry *dentry);
static struct dentry *powerfs_mkdir(struct mnt_idmap *idmap, struct inode *dir, struct dentry *dentry, umode_t mode);
static int powerfs_rename(struct mnt_idmap *idmap, struct inode *old_dir, struct dentry *old_dentry,
			  struct inode *new_dir, struct dentry *new_dentry,
			  unsigned int flags);
static int powerfs_create(struct mnt_idmap *idmap, struct inode *dir, struct dentry *dentry, umode_t mode, bool excl);
static ssize_t powerfs_read(struct file *file, char __user *buf, size_t size, loff_t *pos);
static ssize_t powerfs_write(struct file *file, const char __user *buf, size_t size, loff_t *pos);
static int powerfs_open(struct inode *inode, struct file *file);
static int powerfs_release(struct inode *inode, struct file *file);
static int powerfs_fsync(struct file *file, loff_t start, loff_t end, int datasync);
static int powerfs_readdir(struct file *file, struct dir_context *ctx);
int powerfs_statfs(struct dentry *dentry, struct kstatfs *buf);
static int powerfs_setattr(struct mnt_idmap *idmap, struct dentry *dentry, struct iattr *attr);

static struct powerfs_inode_info *powerfs_lookup_child(struct powerfs_inode_info *parent, const char *name);
static void powerfs_add_child(struct powerfs_inode_info *parent, struct powerfs_inode_info *child);
static void powerfs_remove_child(struct powerfs_inode_info *parent, struct powerfs_inode_info *child);

const struct inode_operations powerfs_dir_inode_operations = {
	.lookup		= powerfs_lookup,
	.getattr	= powerfs_getattr,
	.symlink	= powerfs_symlink,
	.link		= powerfs_link,
	.unlink		= powerfs_unlink,
	.rmdir		= powerfs_rmdir,
	.mkdir		= powerfs_mkdir,
	.rename		= powerfs_rename,
	.create		= powerfs_create,
	.setattr	= powerfs_setattr,
};

const struct inode_operations powerfs_file_inode_operations = {
	.getattr	= powerfs_getattr,
	.readlink	= powerfs_readlink,
	.setattr	= powerfs_setattr,
};

const struct file_operations powerfs_dir_fops = {
	.read		= generic_read_dir,
	.iterate_shared	= powerfs_readdir,
	.fsync		= powerfs_fsync,
};

const struct file_operations powerfs_file_fops = {
	.open		= powerfs_open,
	.release	= powerfs_release,
	.read		= powerfs_read,
	.write		= powerfs_write,
	.fsync		= powerfs_fsync,
	.llseek		= generic_file_llseek,
};

struct powerfs_inode_info *powerfs_lookup_child(struct powerfs_inode_info *parent, const char *name)
{
	struct powerfs_inode_info *child;

	list_for_each_entry(child, &parent->children, sibling) {
		if (strcmp(child->name, name) == 0) {
			return child;
		}
	}
	return NULL;
}

void powerfs_add_child(struct powerfs_inode_info *parent, struct powerfs_inode_info *child)
{
	list_add(&child->sibling, &parent->children);
}

void powerfs_remove_child(struct powerfs_inode_info *parent, struct powerfs_inode_info *child)
{
	list_del(&child->sibling);
}

static struct dentry *powerfs_lookup(struct inode *dir, struct dentry *dentry, unsigned int flags)
{
	struct powerfs_inode_info *parent = powerfs_i(dir);
	struct powerfs_inode_info *child;
	struct inode *inode;

	if (dentry->d_name.len > POWERFS_MAX_FILENAME - 1)
		return ERR_PTR(-ENAMETOOLONG);

	child = powerfs_lookup_child(parent, dentry->d_name.name);
	if (child) {
		inode = iget_locked(dir->i_sb, child->vfs_inode.i_ino);
		if (inode) {
			unlock_new_inode(inode);
			d_add(dentry, inode);
			return NULL;
		}
		return ERR_PTR(-ENOMEM);
	}

	return NULL;
}

static int powerfs_getattr(struct mnt_idmap *idmap, const struct path *path,
			   struct kstat *stat, u32 request_mask, unsigned int query_flags)
{
	struct inode *inode = d_inode(path->dentry);
	struct powerfs_inode_info *pi = powerfs_i(inode);

	generic_fillattr(idmap, 0, inode, stat);
	stat->size = pi->size;
	stat->nlink = pi->nlink;

	return 0;
}

static int powerfs_readlink(struct dentry *dentry, char __user *buffer, int buflen)
{
	struct inode *inode = d_inode(dentry);
	struct powerfs_inode_info *pi = powerfs_i(inode);
	int len;

	if (!S_ISLNK(inode->i_mode))
		return -EINVAL;

	len = strlen(pi->symlink_target);
	if (len > buflen)
		len = buflen;

	if (copy_to_user(buffer, pi->symlink_target, len))
		return -EFAULT;

	return len;
}

static int powerfs_symlink(struct mnt_idmap *idmap, struct inode *dir, struct dentry *dentry, const char *symname)
{
	struct powerfs_inode_info *parent = powerfs_i(dir);
	struct powerfs_inode_info *child;
	struct inode *inode;

	inode = powerfs_get_inode(dir->i_sb, S_IFLNK | 0777);
	if (!inode)
		return -ENOMEM;

	child = powerfs_i(inode);
	strncpy(child->symlink_target, symname, POWERFS_MAX_FILENAME - 1);
	child->symlink_target[POWERFS_MAX_FILENAME - 1] = '\0';
	child->size = strlen(symname);
	strncpy(child->name, dentry->d_name.name, POWERFS_MAX_FILENAME - 1);
	child->name[POWERFS_MAX_FILENAME - 1] = '\0';
	child->parent = parent;
	child->nlink = 1;

	powerfs_add_child(parent, child);

	d_add(dentry, inode);
	return 0;
}

static int powerfs_link(struct dentry *old_dentry, struct inode *new_dir, struct dentry *new_dentry)
{
	struct powerfs_inode_info *old_pi = powerfs_i(d_inode(old_dentry));
	struct powerfs_inode_info *new_parent = powerfs_i(new_dir);
	struct powerfs_inode_info *new_pi;
	struct inode *inode = d_inode(old_dentry);

	new_pi = kzalloc(sizeof(struct powerfs_inode_info), GFP_KERNEL);
	if (!new_pi)
		return -ENOMEM;

	new_pi->data = old_pi->data;
	new_pi->size = old_pi->size;
	new_pi->capacity = old_pi->capacity;
	strncpy(new_pi->name, new_dentry->d_name.name, POWERFS_MAX_FILENAME - 1);
	new_pi->name[POWERFS_MAX_FILENAME - 1] = '\0';
	new_pi->parent = new_parent;
	INIT_LIST_HEAD(&new_pi->children);

	powerfs_add_child(new_parent, new_pi);

	d_add(new_dentry, inode);
	inode_inc_link_count(inode);

	return 0;
}

static int powerfs_unlink(struct inode *dir, struct dentry *dentry)
{
	struct powerfs_inode_info *parent = powerfs_i(dir);
	struct powerfs_inode_info *child = powerfs_i(d_inode(dentry));

	powerfs_remove_child(parent, child);
	drop_nlink(d_inode(dentry));

	return 0;
}

static int powerfs_rmdir(struct inode *dir, struct dentry *dentry)
{
	struct powerfs_inode_info *parent = powerfs_i(dir);
	struct powerfs_inode_info *child = powerfs_i(d_inode(dentry));

	if (!list_empty(&child->children))
		return -ENOTEMPTY;

	powerfs_remove_child(parent, child);
	drop_nlink(d_inode(dentry));
	drop_nlink(dir);

	return 0;
}

static struct dentry *powerfs_mkdir(struct mnt_idmap *idmap, struct inode *dir, struct dentry *dentry, umode_t mode)
{
	struct powerfs_inode_info *parent = powerfs_i(dir);
	struct powerfs_inode_info *child;
	struct inode *inode;

	inode = powerfs_get_inode(dir->i_sb, S_IFDIR | mode);
	if (!inode)
		return ERR_PTR(-ENOMEM);

	child = powerfs_i(inode);
	INIT_LIST_HEAD(&child->children);
	strncpy(child->name, dentry->d_name.name, POWERFS_MAX_FILENAME - 1);
	child->name[POWERFS_MAX_FILENAME - 1] = '\0';
	child->parent = parent;
	child->nlink = 2;

	powerfs_add_child(parent, child);

	d_add(dentry, inode);
	return NULL;
}

static int powerfs_rename(struct mnt_idmap *idmap, struct inode *old_dir, struct dentry *old_dentry,
			  struct inode *new_dir, struct dentry *new_dentry,
			  unsigned int flags)
{
	struct powerfs_inode_info *old_parent = powerfs_i(old_dir);
	struct powerfs_inode_info *new_parent = powerfs_i(new_dir);
	struct powerfs_inode_info *child = powerfs_i(d_inode(old_dentry));

	powerfs_remove_child(old_parent, child);
	strncpy(child->name, new_dentry->d_name.name, POWERFS_MAX_FILENAME - 1);
	child->name[POWERFS_MAX_FILENAME - 1] = '\0';
	child->parent = new_parent;
	powerfs_add_child(new_parent, child);

	return 0;
}

static int powerfs_create(struct mnt_idmap *idmap, struct inode *dir, struct dentry *dentry, umode_t mode, bool excl)
{
	struct powerfs_inode_info *parent = powerfs_i(dir);
	struct powerfs_inode_info *child;
	struct inode *inode;

	inode = powerfs_get_inode(dir->i_sb, S_IFREG | mode);
	if (!inode)
		return -ENOMEM;

	child = powerfs_i(inode);
	child->data = kzalloc(POWERFS_MAX_DATA_SIZE, GFP_KERNEL);
	if (!child->data) {
		iput(inode);
		return -ENOMEM;
	}
	child->size = 0;
	child->capacity = POWERFS_MAX_DATA_SIZE;
	strncpy(child->name, dentry->d_name.name, POWERFS_MAX_FILENAME - 1);
	child->name[POWERFS_MAX_FILENAME - 1] = '\0';
	child->parent = parent;

	powerfs_add_child(parent, child);

	d_add(dentry, inode);
	return 0;
}

static ssize_t powerfs_read(struct file *file, char __user *buf, size_t size, loff_t *pos)
{
	struct inode *inode = file_inode(file);
	struct powerfs_inode_info *pi = powerfs_i(inode);
	ssize_t ret;

	if (*pos >= pi->size)
		return 0;

	if (*pos + size > pi->size)
		size = pi->size - *pos;

	ret = copy_to_user(buf, pi->data + *pos, size);
	if (ret != 0)
		return -EFAULT;

	*pos += size;
	return size;
}

static ssize_t powerfs_write(struct file *file, const char __user *buf, size_t size, loff_t *pos)
{
	struct inode *inode = file_inode(file);
	struct powerfs_inode_info *pi = powerfs_i(inode);
	ssize_t ret;

	if (*pos + size > pi->capacity)
		size = pi->capacity - *pos;

	ret = copy_from_user(pi->data + *pos, buf, size);
	if (ret != 0)
		return -EFAULT;

	*pos += size;
	if (*pos > pi->size)
		pi->size = *pos;

	mark_inode_dirty(inode);
	return size;
}

static int powerfs_open(struct inode *inode, struct file *file)
{
	return 0;
}

static int powerfs_release(struct inode *inode, struct file *file)
{
	return 0;
}

static int powerfs_fsync(struct file *file, loff_t start, loff_t end, int datasync)
{
	return 0;
}

static int powerfs_readdir(struct file *file, struct dir_context *ctx)
{
	struct inode *inode = file_inode(file);
	struct powerfs_inode_info *parent = powerfs_i(inode);
	struct powerfs_inode_info *child;
	unsigned long pos = ctx->pos;

	if (!S_ISDIR(inode->i_mode))
		return -ENOTDIR;

	if (pos == 0) {
		if (!dir_emit(ctx, ".", 1, inode->i_ino, DT_DIR))
			return 0;
		ctx->pos++;
		pos++;
	}

	if (pos == 1) {
		if (!dir_emit(ctx, "..", 2, parent->parent ? parent->parent->vfs_inode.i_ino : inode->i_ino, DT_DIR))
			return 0;
		ctx->pos++;
		pos++;
	}

	list_for_each_entry(child, &parent->children, sibling) {
		if (pos > 2) {
			if (!dir_emit(ctx, child->name, strlen(child->name), child->vfs_inode.i_ino,
				      S_ISDIR(child->vfs_inode.i_mode) ? DT_DIR : DT_REG))
				return 0;
			ctx->pos++;
		}
		pos++;
	}

	return 0;
}

int powerfs_statfs(struct dentry *dentry, struct kstatfs *buf)
{
	buf->f_type = POWERFS_MAGIC;
	buf->f_bsize = PAGE_SIZE;
	buf->f_blocks = 1024 * 1024;
	buf->f_bfree = 1024 * 1024;
	buf->f_bavail = 1024 * 1024;
	buf->f_files = 1000000;
	buf->f_ffree = 1000000;
	buf->f_namelen = POWERFS_MAX_FILENAME;

	return 0;
}

static int powerfs_setattr(struct mnt_idmap *idmap, struct dentry *dentry, struct iattr *attr)
{
	struct inode *inode = d_inode(dentry);
	struct powerfs_inode_info *pi = powerfs_i(inode);

	if (attr->ia_valid & ATTR_SIZE) {
		if (attr->ia_size > pi->capacity)
			return -EFBIG;

		if (attr->ia_size < pi->size) {
			pi->size = attr->ia_size;
			memset(pi->data + pi->size, 0, pi->capacity - pi->size);
		}
	}

	if (attr->ia_valid & ATTR_MODE)
		inode->i_mode = attr->ia_mode;

	if (attr->ia_valid & ATTR_UID)
		inode->i_uid = attr->ia_uid;

	if (attr->ia_valid & ATTR_GID)
		inode->i_gid = attr->ia_gid;

	if (attr->ia_valid & ATTR_ATIME) {
		inode->i_atime_sec = attr->ia_atime.tv_sec;
		inode->i_atime_nsec = attr->ia_atime.tv_nsec;
	}

	if (attr->ia_valid & ATTR_MTIME) {
		inode->i_mtime_sec = attr->ia_mtime.tv_sec;
		inode->i_mtime_nsec = attr->ia_mtime.tv_nsec;
	}

	if (attr->ia_valid & ATTR_CTIME) {
		inode->i_ctime_sec = attr->ia_ctime.tv_sec;
		inode->i_ctime_nsec = attr->ia_ctime.tv_nsec;
	}

	mark_inode_dirty(inode);
	return 0;
}
