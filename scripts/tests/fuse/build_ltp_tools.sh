#!/bin/bash
# Build xfstests-dev/ltp tools inside the fuse-1-test container
# Usage: bash build_ltp_tools.sh
#
# xfstests-dev is cloned from upstream since kernel/ is maintained in a
# separate repository.
set -e

LTP_SRC="${LTP_SRC:-/tmp/xfstests-dev}"
LTP_INSTALL="/opt/ltp-tools"

# Clone xfstests-dev if not present locally
if [ ! -d "$LTP_SRC/ltp" ]; then
    echo "Cloning xfstests-dev..."
    git clone --depth 1 https://git.kernel.org/pub/scm/fs/xfs/xfsprogs-dev.git "$LTP_SRC" 2>/dev/null || \
    git clone --depth 1 https://github.com/kdave/xfstests.git "$LTP_SRC" 2>/dev/null || \
    { echo "ERROR: Failed to clone xfstests-dev"; exit 1; }
fi

# Install build dependencies
docker exec fuse-1-test bash -c '
apt-get update -qq && apt-get install -y -qq gcc make libaio-dev xfslibs-dev linux-libc-dev 2>&1 | tail -3
'

# Copy source files into container
docker exec fuse-1-test mkdir -p /tmp/ltp-build
docker cp "$LTP_SRC/ltp/." fuse-1-test:/tmp/ltp-build/
docker cp "$LTP_SRC/src/global.h" fuse-1-test:/tmp/ltp-build/
docker cp "$LTP_SRC/src/statx.h" fuse-1-test:/tmp/ltp-build/
docker cp "$LTP_SRC/include/." fuse-1-test:/tmp/ltp-build/
docker cp "$LTP_SRC/lib/." fuse-1-test:/tmp/ltp-build/

# Create config.h
docker exec fuse-1-test bash -c 'cat > /tmp/ltp-build/config.h << "INNEREOF"
#ifndef CONFIG_H
#define CONFIG_H
#define _GNU_SOURCE 1
#define HAVE_SYS_TYPES_H 1
#define HAVE_SYS_STAT_H 1
#define HAVE_SYS_STATVFS_H 1
#define HAVE_SYS_TIME_H 1
#define HAVE_SYS_IOCTL_H 1
#define HAVE_SYS_WAIT_H 1
#define HAVE_MALLOC_H 1
#define HAVE_DIRENT_H 1
#define HAVE_STDLIB_H 1
#define HAVE_UNISTD_H 1
#define HAVE_ERRNO_H 1
#define HAVE_STRING_H 1
#define HAVE_STRINGS_H 1
#define HAVE_TIME_H 1
#define HAVE_SIGNAL_H 1
#define HAVE_STDINT_H 1
#define HAVE_INTTYPES_H 1
#define HAVE_GETMNTENT 1
#define HAVE_FALLOCATE 1
#define HAVE_COPY_FILE_RANGE 1
#define HAVE_AIO 1
#define HAVE_SYS_MMAN_H 1
#define HAVE_SYS_PARAM_H 1
#define HAVE_SYS_SYSCALL_H 1
#define HAVE_SYS_UIO_H 1
#define HAVE_LINUX_FALLOC_H 1
#define HAVE_LINUX_TYPES_H 1
#define HAVE_XFS_XFS_H 1
#define HAVE_SYS_FCNTL_H 1
#define HAVE_FCNTL_H 1
#define HAVE_ASSERT_H 1
#define HAVE_LIBGEN_H 1
#define HAVE_PTHREAD_H 1
#define HAVE_LIMITS_H 1
#define HAVE_SYS_VFS_H 1
#define HAVE_SYS_MOUNT_H 1
#define HAVE_SYS_RESOURCE_H 1
#define HAVE_SYS_SOCKET_H 1
#endif
INNEREOF
'

# Patch fsx.c to skip check_trunc_hack (PowerFS doesn't support large inline truncation)
docker exec fuse-1-test bash -c 'sed -i "s/check_trunc_hack();//" /tmp/ltp-build/fsx.c'

# Compile tools
docker exec fuse-1-test bash -c '
cd /tmp/ltp-build
CFLAGS="-g -O2 -D_GNU_SOURCE -DXFS -DFALLOCATE -DHAVE_COPY_FILE_RANGE -DAIO -DHAVE_RENAMEAT2 -DDEBUG -I. -Wno-unused-result -Wno-implicit-function-declaration"
LDFLAGS="-laio -lpthread"

echo "Building fsstress..."
gcc $CFLAGS -o fsstress fsstress.c $LDFLAGS

echo "Building fsx..."
gcc $CFLAGS -o fsx fsx.c $LDFLAGS

echo "Building doio..."
gcc $CFLAGS -o doio doio.c pattern.c random_range.c write_log.c string_to_tokens.c str_to_bytes.c open_flags.c $LDFLAGS

echo "Building iogen..."
gcc $CFLAGS -o iogen iogen.c str_to_bytes.c string_to_tokens.c open_flags.c random_range.c $LDFLAGS

echo "Building aio-stress..."
gcc $CFLAGS -o aio-stress aio-stress.c $LDFLAGS
'

# Install tools
docker exec fuse-1-test bash -c "
mkdir -p $LTP_INSTALL/testcases/bin
cp /tmp/ltp-build/{fsstress,fsx,doio,iogen,aio-stress,rwtest.sh} $LTP_INSTALL/
chmod +x $LTP_INSTALL/*
ln -sf $LTP_INSTALL/rwtest.sh $LTP_INSTALL/rwtest
ln -sf $LTP_INSTALL/iogen $LTP_INSTALL/testcases/bin/iogen
ln -sf $LTP_INSTALL/doio $LTP_INSTALL/testcases/bin/doio
echo 'LTP tools installed to $LTP_INSTALL'
ls -la $LTP_INSTALL/
"
