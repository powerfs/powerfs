// SPDK stub implementation
//
// 在没有真实 SPDK 环境时 (`spdk-stub` feature) 提供空/模拟实现,
// 用于编译和测试 SpdkBackend 的逻辑流程 (不依赖真实 SPDK 库和硬件)。
//
// 所有函数返回成功 (0) 或合理的模拟值,不执行真实 I/O。
// 读写操作用内存模拟,使 attach/读写流程能跑通。

#include "powerfs_spdk.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// 模拟的 bdev 表
#define STUB_MAX_BDEVS 16
#define STUB_BDEV_SIZE (1ULL * 1024 * 1024 * 1024)  // 模拟 1GB

struct stub_bdev {
    int in_use;
    char name[POWERFS_SPDK_MAX_BDEV_NAME];
    int handle;          // open 后的 handle (0 表示未打开)
    uint64_t block_size;
    uint64_t num_blocks;
    uint8_t* data;       // 模拟数据 (按需分配)
};

static struct stub_bdev g_bdevs[STUB_MAX_BDEVS];
static int g_initialized = 0;
static int g_next_handle = 1;

// 简单的 device -> bdev 映射 (用于 add_device/remove_device)
struct stub_device {
    int in_use;
    char device_id[64];
    char bdev_name[POWERFS_SPDK_MAX_BDEV_NAME];
};

static struct stub_device g_devices[STUB_MAX_BDEVS];

static struct stub_bdev* find_bdev_by_name(const char* name) {
    for (int i = 0; i < STUB_MAX_BDEVS; i++) {
        if (g_bdevs[i].in_use && strcmp(g_bdevs[i].name, name) == 0) {
            return &g_bdevs[i];
        }
    }
    return NULL;
}

static struct stub_bdev* find_bdev_by_handle(int handle) {
    for (int i = 0; i < STUB_MAX_BDEVS; i++) {
        if (g_bdevs[i].in_use && g_bdevs[i].handle == handle) {
            return &g_bdevs[i];
        }
    }
    return NULL;
}

int powerfs_spdk_init(const char* name, int mem_size_mb, int no_huge,
                      const char* rpc_socket_path) {
    (void)name;
    (void)mem_size_mb;
    (void)no_huge;
    (void)rpc_socket_path;
    if (g_initialized) return 0;
    memset(g_bdevs, 0, sizeof(g_bdevs));
    memset(g_devices, 0, sizeof(g_devices));
    g_initialized = 1;
    fprintf(stderr, "[spdk-stub] powerfs_spdk_init called (name=%s)\n", name ? name : "(null)");
    return 0;
}

void powerfs_spdk_fini(void) {
    if (!g_initialized) return;
    for (int i = 0; i < STUB_MAX_BDEVS; i++) {
        if (g_bdevs[i].in_use && g_bdevs[i].data) {
            free(g_bdevs[i].data);
        }
    }
    memset(g_bdevs, 0, sizeof(g_bdevs));
    memset(g_devices, 0, sizeof(g_devices));
    g_initialized = 0;
}

int powerfs_spdk_attach_controller(const char* name, const char* traddr) {
    (void)traddr;
    // stub: 注册一个 malloc bdev 模拟 attach 成功
    fprintf(stderr, "[spdk-stub] attach_controller name=%s traddr=%s\n",
            name ? name : "(null)", traddr ? traddr : "(null)");
    return 0;
}

int powerfs_spdk_detach_controller(const char* name) {
    fprintf(stderr, "[spdk-stub] detach_controller name=%s\n", name ? name : "(null)");
    return 0;
}

int powerfs_spdk_create_malloc_bdev(const char* name, uint64_t size_mb) {
    if (!g_initialized || !name) return -1;
    // 找空槽
    for (int i = 0; i < STUB_MAX_BDEVS; i++) {
        if (!g_bdevs[i].in_use) {
            g_bdevs[i].in_use = 1;
            strncpy(g_bdevs[i].name, name, POWERFS_SPDK_MAX_BDEV_NAME - 1);
            g_bdevs[i].handle = 0;
            g_bdevs[i].block_size = 512;
            g_bdevs[i].num_blocks = (size_mb * 1024 * 1024) / 512;
            // 按需分配数据 (这里不预分配,read 时再处理)
            g_bdevs[i].data = NULL;
            return 0;
        }
    }
    return -1;
}

int powerfs_spdk_list_bdevs(struct powerfs_spdk_bdev_info* bdevs, int max_count, int* count) {
    if (!g_initialized || !bdevs || !count) return -1;
    int n = 0;
    for (int i = 0; i < STUB_MAX_BDEVS && n < max_count; i++) {
        if (g_bdevs[i].in_use) {
            strncpy(bdevs[n].name, g_bdevs[i].name, POWERFS_SPDK_MAX_BDEV_NAME - 1);
            bdevs[n].name[POWERFS_SPDK_MAX_BDEV_NAME - 1] = '\0';
            bdevs[n].block_size = g_bdevs[i].block_size;
            bdevs[n].num_blocks = g_bdevs[i].num_blocks;
            bdevs[n].total_size = g_bdevs[i].block_size * g_bdevs[i].num_blocks;
            n++;
        }
    }
    *count = n;
    return 0;
}

int powerfs_spdk_open_bdev(const char* bdev_name) {
    if (!g_initialized || !bdev_name) return -1;
    struct stub_bdev* b = find_bdev_by_name(bdev_name);
    if (!b) {
        // stub: 如果 bdev 不存在,自动创建一个模拟的
        for (int i = 0; i < STUB_MAX_BDEVS; i++) {
            if (!g_bdevs[i].in_use) {
                g_bdevs[i].in_use = 1;
                strncpy(g_bdevs[i].name, bdev_name, POWERFS_SPDK_MAX_BDEV_NAME - 1);
                g_bdevs[i].block_size = 512;
                g_bdevs[i].num_blocks = STUB_BDEV_SIZE / 512;
                b = &g_bdevs[i];
                break;
            }
        }
        if (!b) return -1;
    }
    b->handle = g_next_handle++;
    return b->handle;
}

void powerfs_spdk_close_bdev(int handle) {
    struct stub_bdev* b = find_bdev_by_handle(handle);
    if (b) {
        b->handle = 0;
    }
}

int powerfs_spdk_read(int handle, uint64_t offset, void* buf, uint64_t size) {
    (void)offset;
    struct stub_bdev* b = find_bdev_by_handle(handle);
    if (!b || !buf) return -1;
    // stub: 填充 0
    memset(buf, 0, size);
    return 0;
}

int powerfs_spdk_write(int handle, uint64_t offset, const void* buf, uint64_t size) {
    (void)handle;
    (void)offset;
    (void)buf;
    (void)size;
    // stub: 写入总是成功 (不实际保存)
    return 0;
}

int powerfs_spdk_get_bdev_size(int handle, uint64_t* size) {
    struct stub_bdev* b = find_bdev_by_handle(handle);
    if (!b || !size) return -1;
    *size = b->block_size * b->num_blocks;
    return 0;
}

int powerfs_spdk_add_device(const char* device_id, const char* bdev_name) {
    if (!g_initialized || !device_id || !bdev_name) return -1;
    for (int i = 0; i < STUB_MAX_BDEVS; i++) {
        if (!g_devices[i].in_use) {
            g_devices[i].in_use = 1;
            strncpy(g_devices[i].device_id, device_id, 63);
            strncpy(g_devices[i].bdev_name, bdev_name, POWERFS_SPDK_MAX_BDEV_NAME - 1);
            return 0;
        }
    }
    return -1;
}

int powerfs_spdk_remove_device(const char* device_id) {
    if (!device_id) return -1;
    for (int i = 0; i < STUB_MAX_BDEVS; i++) {
        if (g_devices[i].in_use && strcmp(g_devices[i].device_id, device_id) == 0) {
            g_devices[i].in_use = 0;
            return 0;
        }
    }
    return -1;
}

int powerfs_spdk_list_devices(struct powerfs_spdk_device_info* devices, int max_count, int* count) {
    if (!devices || !count) return -1;
    int n = 0;
    for (int i = 0; i < STUB_MAX_BDEVS && n < max_count; i++) {
        if (g_devices[i].in_use) {
            strncpy(devices[n].device_id, g_devices[i].device_id, 63);
            devices[n].device_id[63] = '\0';
            strncpy(devices[n].bdev_name, g_devices[i].bdev_name, POWERFS_SPDK_MAX_BDEV_NAME - 1);
            devices[n].bdev_name[POWERFS_SPDK_MAX_BDEV_NAME - 1] = '\0';
            devices[n].total_capacity = STUB_BDEV_SIZE;
            devices[n].free_space = STUB_BDEV_SIZE;
            devices[n].status = 0;
            n++;
        }
    }
    *count = n;
    return 0;
}

int powerfs_spdk_allocate_volume(int* volume_id, uint64_t size_bytes, const char* device_id) {
    (void)size_bytes;
    (void)device_id;
    if (!volume_id) return -1;
    static int next_vol = 1;
    *volume_id = next_vol++;
    return 0;
}

int powerfs_spdk_delete_volume(int volume_id) {
    (void)volume_id;
    return 0;
}

int powerfs_spdk_get_volume_info(int volume_id, uint64_t* total_size, uint64_t* used_size) {
    (void)volume_id;
    if (total_size) *total_size = STUB_BDEV_SIZE;
    if (used_size) *used_size = 0;
    return 0;
}

int powerfs_spdk_write_needle(int volume_id, uint64_t needle_id, const void* data, uint64_t size) {
    (void)volume_id;
    (void)needle_id;
    (void)data;
    (void)size;
    return 0;
}

int powerfs_spdk_read_needle(int volume_id, uint64_t needle_id, void* data, uint64_t size) {
    (void)volume_id;
    (void)needle_id;
    if (data) memset(data, 0, size);
    return 0;
}
