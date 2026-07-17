#include "spdk_ffi.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct spdk_seg_handle {
    uint64_t size;
    uint8_t* data;
    uint32_t block_size;
} spdk_seg_handle_t;

bool spdk_initialize_env(void) {
    return true;
}

void spdk_cleanup(void) {
}

spdk_seg_handle_t* spdk_open_segment(const char* tr_str) {
    (void)tr_str;
    
    spdk_seg_handle_t* handle = malloc(sizeof(spdk_seg_handle_t));
    if (!handle) return NULL;
    
    handle->size = 64 * 1024;
    handle->block_size = 4096;
    handle->data = calloc(handle->size, 1);
    
    if (!handle->data) {
        free(handle);
        return NULL;
    }
    
    return handle;
}

void spdk_close_segment(spdk_seg_handle_t* seg) {
    if (seg) {
        if (seg->data) {
            free(seg->data);
        }
        free(seg);
    }
}

uint32_t spdk_get_block_size(spdk_seg_handle_t* seg) {
    if (!seg) return 0;
    return seg->block_size;
}

int spdk_read(spdk_seg_handle_t* seg, void* buf, uint64_t lba, uint32_t lba_count) {
    if (!seg || !buf || lba_count == 0) return -1;
    
    uint64_t offset = lba * seg->block_size;
    uint64_t size = lba_count * seg->block_size;
    
    if (offset + size > seg->size) return -1;
    
    memcpy(buf, seg->data + offset, size);
    return 0;
}

int spdk_write(spdk_seg_handle_t* seg, const void* buf, uint64_t lba, uint32_t lba_count) {
    if (!seg || !buf || lba_count == 0) return -1;
    
    uint64_t offset = lba * seg->block_size;
    uint64_t size = lba_count * seg->block_size;
    
    if (offset + size > seg->size) return -1;
    
    memcpy(seg->data + offset, buf, size);
    return 0;
}

bool spdk_probe_segment(const char* tr_str, uint32_t timeout_ms, char* error_reason, size_t error_reason_buf_size) {
    (void)tr_str;
    (void)timeout_ms;
    (void)error_reason;
    (void)error_reason_buf_size;
    return true;
}

uint64_t spdk_get_ns_size(spdk_seg_handle_t* seg) {
    if (!seg) return 0;
    return seg->size;
}
