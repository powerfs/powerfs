#!/bin/bash
set -e

echo "=== PowerFS Raft 多分组分片均衡 - 性能测试 ==="

mkdir -p /tmp/powerfs/test

echo "1. 启动测试集群..."
cd /home/portion/powerfs/docker
docker-compose -f docker-compose.test.yml up -d

echo "2. 等待集群启动..."
sleep 60

echo "3. 检查集群状态..."
docker-compose -f docker-compose.test.yml ps

echo "4. 运行元数据创建测试..."
docker exec benchmark fio --name=metadata-create --directory=/mnt/powerfs --ioengine=sync \
    --rw=write --create_on_open=1 --numjobs=4 --iodepth=32 --runtime=30 --size=4k \
    --group_reporting --name_format=fio-test-file-%04d

echo "5. 运行元数据删除测试..."
docker exec benchmark fio --name=metadata-delete --directory=/mnt/powerfs --ioengine=sync \
    --rw=read --unlink=1 --numjobs=4 --iodepth=32 --runtime=30 --size=4k \
    --group_reporting --name_format=fio-test-file-%04d

echo "6. 运行元数据Stat测试..."
docker exec benchmark fio --name=metadata-stat --directory=/mnt/powerfs --ioengine=sync \
    --rw=read --openflags=O_RDONLY --numjobs=8 --iodepth=128 --runtime=30 \
    --group_reporting --filename=fio-test-file-0000

echo "7. 运行目录列表测试..."
docker exec benchmark bash -c "time ls -la /mnt/powerfs | wc -l"

echo "8. 停止测试集群..."
docker-compose -f docker-compose.test.yml down

echo "=== 测试完成 ==="
