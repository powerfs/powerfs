#pragma once

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct spdk_seg_handle spdk_seg_handle_t;

bool spdk_initialize_env(void);

void spdk_cleanup(void);

spdk_seg_handle_t* spdk_open_segment(const char* tr_str);

void spdk_close_segment(spdk_seg_handle_t* seg);

uint32_t spdk_get_block_size(spdk_seg_handle_t* seg);

int spdk_read(spdk_seg_handle_t* seg, void* buf, uint64_t lba, uint32_t lba_count);

int spdk_write(spdk_seg_handle_t* seg, const void* buf, uint64_t lba, uint32_t lba_count);

bool spdk_probe_segment(const char* tr_str, uint32_t timeout_ms, char* error_reason, size_t error_reason_buf_size);

uint64_t spdk_get_ns_size(spdk_seg_handle_t* seg);

#ifdef __cplusplus
}
#endif
