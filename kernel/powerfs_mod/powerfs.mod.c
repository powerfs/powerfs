#include <linux/module.h>
#include <linux/export-internal.h>
#include <linux/compiler.h>

MODULE_INFO(name, KBUILD_MODNAME);

__visible struct module __this_module
__section(".gnu.linkonce.this_module") = {
	.name = KBUILD_MODNAME,
	.init = init_module,
#ifdef CONFIG_MODULE_UNLOAD
	.exit = cleanup_module,
#endif
	.arch = MODULE_ARCH_INIT,
};



static const struct modversion_info ____versions[]
__used __section("__versions") = {
	{ 0x6e8d34d0, "generic_fillattr" },
	{ 0x74895968, "iget_locked" },
	{ 0xa61fd7aa, "__check_object_size" },
	{ 0x4c5d2323, "clear_inode" },
	{ 0x471d23f5, "__nlmsg_put" },
	{ 0x092a35a2, "_copy_from_user" },
	{ 0xd186bac3, "new_inode" },
	{ 0xe53d4a79, "unregister_filesystem" },
	{ 0x1d52ec49, "d_make_root" },
	{ 0xfcf02643, "current_time" },
	{ 0xa53f4e29, "memcpy" },
	{ 0xcb8b6ec6, "kfree" },
	{ 0x4c5d2323, "iput" },
	{ 0xb22889f3, "__netlink_kernel_create" },
	{ 0xe53d4a79, "register_filesystem" },
	{ 0xd272d446, "__fentry__" },
	{ 0x5a844b26, "__x86_indirect_thunk_rax" },
	{ 0x4c5d2323, "unlock_new_inode" },
	{ 0xe8213e80, "_printk" },
	{ 0x21bfdf43, "truncate_inode_pages_final" },
	{ 0xd710adbf, "__kmalloc_large_noprof" },
	{ 0x9479a1e8, "strnlen" },
	{ 0x80cced5e, "__alloc_skb" },
	{ 0xb1172073, "init_net" },
	{ 0xbb653e95, "sk_skb_reason_drop" },
	{ 0x1b261533, "netlink_unicast" },
	{ 0xbd03ed67, "random_kmalloc_seed" },
	{ 0xc4cc9b40, "set_nlink" },
	{ 0xd94efd11, "const_current_task" },
	{ 0xc609ff70, "strncpy" },
	{ 0x09289ea7, "netlink_kernel_release" },
	{ 0xe54e0a6b, "__fortify_panic" },
	{ 0x811995cf, "kill_litter_super" },
	{ 0x27683a56, "memset" },
	{ 0x5a844b26, "__x86_indirect_thunk_r10" },
	{ 0xd272d446, "__x86_return_thunk" },
	{ 0x092a35a2, "_copy_to_user" },
	{ 0x888b8f57, "strcmp" },
	{ 0xe95df651, "mount_nodev" },
	{ 0xb38562b7, "generic_read_dir" },
	{ 0xa853c451, "d_add" },
	{ 0x4c5d2323, "inc_nlink" },
	{ 0xecd17989, "__kmalloc_cache_noprof" },
	{ 0x546c19d9, "validate_usercopy_range" },
	{ 0x43a349ca, "strlen" },
	{ 0x7ddf7992, "__mark_inode_dirty" },
	{ 0xc0112974, "generic_file_llseek" },
	{ 0x4c5d2323, "drop_nlink" },
	{ 0x08bfc903, "kmalloc_caches" },
	{ 0x814e12e5, "module_layout" },
};

static const u32 ____version_ext_crcs[]
__used __section("__version_ext_crcs") = {
	0x6e8d34d0,
	0x74895968,
	0xa61fd7aa,
	0x4c5d2323,
	0x471d23f5,
	0x092a35a2,
	0xd186bac3,
	0xe53d4a79,
	0x1d52ec49,
	0xfcf02643,
	0xa53f4e29,
	0xcb8b6ec6,
	0x4c5d2323,
	0xb22889f3,
	0xe53d4a79,
	0xd272d446,
	0x5a844b26,
	0x4c5d2323,
	0xe8213e80,
	0x21bfdf43,
	0xd710adbf,
	0x9479a1e8,
	0x80cced5e,
	0xb1172073,
	0xbb653e95,
	0x1b261533,
	0xbd03ed67,
	0xc4cc9b40,
	0xd94efd11,
	0xc609ff70,
	0x09289ea7,
	0xe54e0a6b,
	0x811995cf,
	0x27683a56,
	0x5a844b26,
	0xd272d446,
	0x092a35a2,
	0x888b8f57,
	0xe95df651,
	0xb38562b7,
	0xa853c451,
	0x4c5d2323,
	0xecd17989,
	0x546c19d9,
	0x43a349ca,
	0x7ddf7992,
	0xc0112974,
	0x4c5d2323,
	0x08bfc903,
	0x814e12e5,
};
static const char ____version_ext_names[]
__used __section("__version_ext_names") =
	"generic_fillattr\0"
	"iget_locked\0"
	"__check_object_size\0"
	"clear_inode\0"
	"__nlmsg_put\0"
	"_copy_from_user\0"
	"new_inode\0"
	"unregister_filesystem\0"
	"d_make_root\0"
	"current_time\0"
	"memcpy\0"
	"kfree\0"
	"iput\0"
	"__netlink_kernel_create\0"
	"register_filesystem\0"
	"__fentry__\0"
	"__x86_indirect_thunk_rax\0"
	"unlock_new_inode\0"
	"_printk\0"
	"truncate_inode_pages_final\0"
	"__kmalloc_large_noprof\0"
	"strnlen\0"
	"__alloc_skb\0"
	"init_net\0"
	"sk_skb_reason_drop\0"
	"netlink_unicast\0"
	"random_kmalloc_seed\0"
	"set_nlink\0"
	"const_current_task\0"
	"strncpy\0"
	"netlink_kernel_release\0"
	"__fortify_panic\0"
	"kill_litter_super\0"
	"memset\0"
	"__x86_indirect_thunk_r10\0"
	"__x86_return_thunk\0"
	"_copy_to_user\0"
	"strcmp\0"
	"mount_nodev\0"
	"generic_read_dir\0"
	"d_add\0"
	"inc_nlink\0"
	"__kmalloc_cache_noprof\0"
	"validate_usercopy_range\0"
	"strlen\0"
	"__mark_inode_dirty\0"
	"generic_file_llseek\0"
	"drop_nlink\0"
	"kmalloc_caches\0"
	"module_layout\0"
;

MODULE_INFO(depends, "");


MODULE_INFO(srcversion, "43BC971D635B899FC30B42C");
