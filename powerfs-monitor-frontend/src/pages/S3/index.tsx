import { useState, useEffect } from 'react'
import { Card, Row, Col, Statistic, Table, Tag, Button, Modal, Form, Input, Space, Popconfirm, message, Tabs, Alert } from 'antd'
import {
  FolderOpenOutlined,
  FileOutlined,
  UploadOutlined,
  DeleteOutlined,
  PlusOutlined,
  BarChartOutlined,
  InboxOutlined,
  DownloadOutlined,
  KeyOutlined,
  LinkOutlined,
  CloudServerOutlined,
  DatabaseOutlined,
} from '@ant-design/icons'
import type { BucketInfo, ObjectInfo, MultipartUploadInfo, S3Metrics, S3AccessKey } from '@/types'
import { getS3Metrics, getBuckets, createBucket, deleteBucket, getObjects, deleteObject, uploadObject, downloadObject, getMultipartUploads, abortMultipartUpload, getS3AccessKeys, createS3AccessKey, deleteS3AccessKey } from '@/services/api'
import { formatBytes, formatNumber } from '@/utils/format'

function S3() {
  const [metrics, setMetrics] = useState<S3Metrics | null>(null)
  const [buckets, setBuckets] = useState<BucketInfo[]>([])
  const [objects, setObjects] = useState<ObjectInfo[]>([])
  const [uploads, setUploads] = useState<MultipartUploadInfo[]>([])
  const [accessKeys, setAccessKeys] = useState<S3AccessKey[]>([])
  const [selectedBucket, setSelectedBucket] = useState<string | null>(null)
  const [createModalVisible, setCreateModalVisible] = useState(false)
  const [uploadModalVisible, setUploadModalVisible] = useState(false)
  const [keyModalVisible, setKeyModalVisible] = useState(false)
  const [uploadFile, setUploadFile] = useState<File | null>(null)
  const [uploadKey, setUploadKey] = useState('')
  const [form] = Form.useForm()
  const [keyForm] = Form.useForm()

  useEffect(() => {
    loadData()
  }, [])

  const loadData = async () => {
    try {
      const [s3Metrics, bucketList] = await Promise.all([
        getS3Metrics(),
        getBuckets(),
      ])
      setMetrics(s3Metrics)
      setBuckets(bucketList)
    } catch (error) {
      console.error('Failed to load S3 data:', error)
      message.error('加载S3数据失败')
    }
  }

  const loadBucketObjects = async (bucketName: string) => {
    try {
      const objList = await getObjects(bucketName)
      setObjects(objList)
      setSelectedBucket(bucketName)
    } catch (error) {
      console.error('Failed to load bucket objects:', error)
      message.error('加载Bucket对象失败')
    }
  }

  const loadMultipartUploads = async () => {
    try {
      const uploadList = await getMultipartUploads()
      if (selectedBucket) {
        setUploads(uploadList.filter(u => u.bucket === selectedBucket))
      } else {
        setUploads(uploadList)
      }
    } catch (error) {
      console.error('Failed to load multipart uploads:', error)
      message.error('加载分片上传列表失败')
    }
  }

  const loadAccessKeys = async () => {
    try {
      const keys = await getS3AccessKeys()
      setAccessKeys(keys)
    } catch (error) {
      console.error('Failed to load access keys:', error)
      message.error('加载访问密钥失败')
    }
  }

  const handleCreateAccessKey = async () => {
    try {
      const values = await keyForm.validateFields()
      await createS3AccessKey(values.access_key, values.secret_key)
      setKeyModalVisible(false)
      keyForm.resetFields()
      loadAccessKeys()
      message.success('访问密钥创建成功')
    } catch (error) {
      console.error('Failed to create access key:', error)
      message.error('创建访问密钥失败')
    }
  }

  const handleDeleteAccessKey = async (accessKey: string) => {
    try {
      await deleteS3AccessKey(accessKey)
      loadAccessKeys()
      message.success('访问密钥删除成功')
    } catch (error) {
      console.error('Failed to delete access key:', error)
      message.error('删除访问密钥失败')
    }
  }

  const handleCreateBucket = async () => {
    try {
      const values = await form.validateFields()
      await createBucket(values.name)
      setCreateModalVisible(false)
      form.resetFields()
      loadData()
      message.success('Bucket创建成功')
    } catch (error) {
      console.error('Failed to create bucket:', error)
      message.error('创建Bucket失败')
    }
  }

  const handleDeleteBucket = async (name: string) => {
    try {
      await deleteBucket(name)
      loadData()
      if (selectedBucket === name) {
        setSelectedBucket(null)
        setObjects([])
      }
      message.success('Bucket删除成功')
    } catch (error) {
      console.error('Failed to delete bucket:', error)
      message.error('删除Bucket失败')
    }
  }

  const handleDeleteObject = async (bucket: string, key: string) => {
    try {
      await deleteObject(bucket, key)
      loadBucketObjects(bucket)
      message.success('对象删除成功')
    } catch (error) {
      console.error('Failed to delete object:', error)
      message.error('删除对象失败')
    }
  }

  const handleUploadObject = async () => {
    if (!uploadFile || !uploadKey || !selectedBucket) {
      message.warning('请选择文件并输入对象名称')
      return
    }
    try {
      await uploadObject(selectedBucket, uploadKey, uploadFile)
      setUploadModalVisible(false)
      setUploadFile(null)
      setUploadKey('')
      loadBucketObjects(selectedBucket)
      message.success('对象上传成功')
    } catch (error) {
      console.error('Failed to upload object:', error)
      message.error('上传对象失败')
    }
  }

  const handleDownloadObject = async (bucket: string, key: string) => {
    try {
      await downloadObject(bucket, key)
    } catch (error) {
      console.error('Failed to download object:', error)
      message.error('下载对象失败')
    }
  }

  const handleAbortUpload = async (bucket: string, key: string, uploadId: string) => {
    try {
      await abortMultipartUpload(bucket, key, uploadId)
      loadMultipartUploads()
      message.success('分片上传已中止')
    } catch (error) {
      console.error('Failed to abort upload:', error)
      message.error('中止分片上传失败')
    }
  }

  const bucketColumns = [
    {
      title: 'Bucket名称',
      dataIndex: 'name',
      key: 'name',
      render: (name: string) => (
        <a onClick={() => loadBucketObjects(name)} style={{ cursor: 'pointer' }}>
          <FolderOpenOutlined style={{ marginRight: 8 }} />
          {name}
        </a>
      ),
    },
    {
      title: '创建时间',
      dataIndex: 'creation_date',
      key: 'creation_date',
      render: (date: string) => new Date(date).toLocaleString(),
    },
    {
      title: '对象数量',
      dataIndex: 'object_count',
      key: 'object_count',
      render: (count: number) => formatNumber(count),
    },
    {
      title: '总大小',
      dataIndex: 'total_size',
      key: 'total_size',
      render: (size: number) => formatBytes(size),
    },
    {
      title: '操作',
      key: 'actions',
      render: (_: unknown, record: BucketInfo) => (
        <Space>
          <Button size="small" onClick={() => loadBucketObjects(record.name)}>
            <FileOutlined /> 浏览
          </Button>
          <Button size="small" type="primary" onClick={() => { loadBucketObjects(record.name); setUploadModalVisible(true); }}>
            <UploadOutlined /> 上传
          </Button>
          <Popconfirm
            title={`确定删除Bucket "${record.name}" 吗？`}
            onConfirm={() => handleDeleteBucket(record.name)}
            okText="确定"
            cancelText="取消"
          >
            <Button size="small" danger>
              <DeleteOutlined /> 删除
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ]

  const objectColumns = [
    {
      title: '对象名称',
      dataIndex: 'key',
      key: 'key',
      render: (key: string) => (
        <span>
          <FileOutlined style={{ marginRight: 8, color: '#1890ff' }} />
          {key}
        </span>
      ),
    },
    {
      title: 'ETag',
      dataIndex: 'etag',
      key: 'etag',
      render: (etag: string) => <code>{etag}</code>,
    },
    {
      title: '大小',
      dataIndex: 'size',
      key: 'size',
      render: (size: number) => formatBytes(size),
    },
    {
      title: '最后修改',
      dataIndex: 'last_modified',
      key: 'last_modified',
      render: (date: string) => new Date(date).toLocaleString(),
    },
    {
      title: '存储类型',
      dataIndex: 'storage_class',
      key: 'storage_class',
      render: (class_: string) => (
        <Tag color="blue">{class_}</Tag>
      ),
    },
    {
      title: '操作',
      key: 'actions',
      render: (_: unknown, record: ObjectInfo) => (
        <Space>
          <Button size="small" onClick={() => handleDownloadObject(selectedBucket!, record.key)}>
            <DownloadOutlined /> 下载
          </Button>
          <Popconfirm
            title={`确定删除对象 "${record.key}" 吗？`}
            onConfirm={() => handleDeleteObject(selectedBucket!, record.key)}
            okText="确定"
            cancelText="取消"
          >
            <Button size="small" danger>
              <DeleteOutlined /> 删除
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ]

  const accessKeyColumns = [
    {
      title: 'Access Key',
      dataIndex: 'access_key',
      key: 'access_key',
      render: (key: string) => <code style={{ color: '#1890ff' }}>{key}</code>,
    },
    {
      title: 'Secret Key',
      dataIndex: 'secret_key',
      key: 'secret_key',
      render: (key: string) => <code style={{ fontSize: 12 }}>{'•'.repeat(8)}{key.slice(-4)}</code>,
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      render: (date: string) => new Date(date).toLocaleString(),
    },
    {
      title: '操作',
      key: 'actions',
      render: (_: unknown, record: S3AccessKey) => (
        <Popconfirm
          title={`确定删除访问密钥 "${record.access_key}" 吗？`}
          onConfirm={() => handleDeleteAccessKey(record.access_key)}
          okText="确定"
          cancelText="取消"
        >
          <Button size="small" danger>
            <DeleteOutlined /> 删除
          </Button>
        </Popconfirm>
      ),
    },
  ]

  const uploadColumns = [
    {
      title: 'Bucket',
      dataIndex: 'bucket',
      key: 'bucket',
    },
    {
      title: '对象Key',
      dataIndex: 'key',
      key: 'key',
    },
    {
      title: '上传ID',
      dataIndex: 'upload_id',
      key: 'upload_id',
      render: (id: string) => <code style={{ fontSize: 12 }}>{id}</code>,
    },
    {
      title: '发起者',
      dataIndex: 'initiator',
      key: 'initiator',
    },
    {
      title: '创建时间',
      dataIndex: 'creation_date',
      key: 'creation_date',
      render: (date: string) => new Date(date).toLocaleString(),
    },
    {
      title: '分片数',
      dataIndex: 'part_count',
      key: 'part_count',
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => (
        <Tag color={status === 'in_progress' ? 'orange' : status === 'completed' ? 'green' : 'red'}>
          {status === 'in_progress' ? '进行中' : status === 'completed' ? '已完成' : '已中止'}
        </Tag>
      ),
    },
    {
      title: '操作',
      key: 'actions',
      render: (_: unknown, record: MultipartUploadInfo) => (
        record.status === 'in_progress' ? (
          <Popconfirm
            title={`确定中止上传 "${record.key}" 吗？`}
            onConfirm={() => handleAbortUpload(record.bucket, record.key, record.upload_id)}
            okText="确定"
            cancelText="取消"
          >
            <Button size="small" danger>
              <DeleteOutlined /> 中止
            </Button>
          </Popconfirm>
        ) : null
      ),
    },
  ]

  return (
    <div>
      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        <Col span={6}>
          <Card
            hoverable
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <Space direction="vertical" style={{ width: '100%' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{ background: '#e6f7ff', padding: 8, borderRadius: 8 }}>
                  <FolderOpenOutlined style={{ fontSize: 24, color: '#1890ff' }} />
                </div>
                <span style={{ color: '#8c8c8c' }}>Bucket数量</span>
              </div>
              <Statistic
                value={metrics?.bucket_count || 0}
                suffix="个"
                valueStyle={{ fontSize: 32, fontWeight: 'bold', color: '#1890ff' }}
              />
            </Space>
          </Card>
        </Col>
        <Col span={6}>
          <Card
            hoverable
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <Space direction="vertical" style={{ width: '100%' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{ background: '#f6ffed', padding: 8, borderRadius: 8 }}>
                  <FileOutlined style={{ fontSize: 24, color: '#52c41a' }} />
                </div>
                <span style={{ color: '#8c8c8c' }}>对象总数</span>
              </div>
              <Statistic
                value={formatNumber(metrics?.object_count || 0)}
                valueStyle={{ fontSize: 32, fontWeight: 'bold', color: '#52c41a' }}
              />
            </Space>
          </Card>
        </Col>
        <Col span={6}>
          <Card
            hoverable
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <Space direction="vertical" style={{ width: '100%' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{ background: '#fff7e6', padding: 8, borderRadius: 8 }}>
                  <InboxOutlined style={{ fontSize: 24, color: '#fa8c16' }} />
                </div>
                <span style={{ color: '#8c8c8c' }}>存储总量</span>
              </div>
              <Statistic
                value={formatBytes(metrics?.total_size || 0)}
                valueStyle={{ fontSize: 32, fontWeight: 'bold', color: '#fa8c16' }}
              />
            </Space>
          </Card>
        </Col>
        <Col span={6}>
          <Card
            hoverable
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <Space direction="vertical" style={{ width: '100%' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{ background: '#fff0f6', padding: 8, borderRadius: 8 }}>
                  <UploadOutlined style={{ fontSize: 24, color: '#eb2f96' }} />
                </div>
                <span style={{ color: '#8c8c8c' }}>活跃上传</span>
              </div>
              <Statistic
                value={metrics?.active_multipart_uploads || 0}
                suffix="个"
                valueStyle={{ fontSize: 32, fontWeight: 'bold', color: '#eb2f96' }}
              />
            </Space>
          </Card>
        </Col>
      </Row>

      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        <Col span={8}>
          <Card
            hoverable
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <Space direction="vertical" style={{ width: '100%', gap: 16 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <BarChartOutlined style={{ fontSize: 20, color: '#52c41a' }} />
                <span style={{ fontWeight: 500 }}>请求统计</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ color: '#8c8c8c' }}>PUT请求</span>
                <span style={{ fontWeight: 500, color: '#1890ff' }}>{formatNumber(metrics?.put_requests || 0)} 次</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ color: '#8c8c8c' }}>GET请求</span>
                <span style={{ fontWeight: 500, color: '#52c41a' }}>{formatNumber(metrics?.get_requests || 0)} 次</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ color: '#8c8c8c' }}>DELETE请求</span>
                <span style={{ fontWeight: 500, color: '#fa8c16' }}>{formatNumber(metrics?.delete_requests || 0)} 次</span>
              </div>
            </Space>
          </Card>
        </Col>
        <Col span={16}>
          <Card
            title="Bucket列表"
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
            extra={
              <Button type="primary" onClick={() => setCreateModalVisible(true)}>
                <PlusOutlined /> 创建Bucket
              </Button>
            }
          >
            <Table
              columns={bucketColumns}
              dataSource={buckets}
              rowKey="name"
              pagination={{ pageSize: 10 }}
              size="small"
            />
          </Card>
        </Col>
      </Row>

      <Tabs defaultActiveKey="objects" onChange={(key) => { if (key === 'uploads') loadMultipartUploads(); if (key === 'keys') loadAccessKeys(); }}>
        <Tabs.TabPane tab="对象浏览" key="objects">
          {selectedBucket ? (
            <Card
              title={`Bucket: ${selectedBucket}`}
              style={{ borderRadius: 12 }}
              bodyStyle={{ padding: '20px' }}
              extra={
                <Space>
                  <Button type="primary" onClick={() => setUploadModalVisible(true)}>
                    <UploadOutlined /> 上传文件
                  </Button>
                  <Button onClick={() => { setSelectedBucket(null); setObjects([]); }}>
                    返回Bucket列表
                  </Button>
                </Space>
              }
            >
              <Table
                columns={objectColumns}
                dataSource={objects}
                rowKey="key"
                pagination={{ pageSize: 10 }}
                size="small"
              />
            </Card>
          ) : (
            <Card
              style={{ borderRadius: 12 }}
              bodyStyle={{ padding: '40px', textAlign: 'center' }}
            >
              <Space direction="vertical" align="center">
                <FileOutlined style={{ fontSize: 48, color: '#d9d9d9' }} />
                <span style={{ color: '#8c8c8c' }}>请选择一个Bucket查看对象列表</span>
              </Space>
            </Card>
          )}
        </Tabs.TabPane>
        <Tabs.TabPane tab="分片上传管理" key="uploads">
          <Card
            title="分片上传列表"
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <Table
              columns={uploadColumns}
              dataSource={uploads}
              rowKey="upload_id"
              pagination={{ pageSize: 10 }}
              size="small"
            />
          </Card>
        </Tabs.TabPane>
        <Tabs.TabPane tab="访问密钥管理" key="keys">
          <Card
            title="S3访问密钥"
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
            extra={
              <Button type="primary" onClick={() => setKeyModalVisible(true)}>
                <PlusOutlined /> 创建密钥
              </Button>
            }
          >
            <Alert
              message="访问密钥用于S3客户端认证"
              description="S3客户端使用这些密钥通过PowerFS S3 Gateway访问存储。"
              type="info"
              showIcon
              style={{ marginBottom: 16 }}
            />
            <Table
              columns={accessKeyColumns}
              dataSource={accessKeys}
              rowKey="access_key"
              pagination={{ pageSize: 10 }}
              size="small"
            />
          </Card>
        </Tabs.TabPane>
        <Tabs.TabPane tab="S3网关" key="console">
          <Card
            title="PowerFS S3 Gateway"
            style={{ borderRadius: 12 }}
            bodyStyle={{ padding: '20px' }}
          >
            <Alert
              message="PowerFS S3网关模式"
              description="PowerFS内置S3兼容网关，支持AWS S3协议，元数据由Master统一管理，数据存储在分布式Volume Server节点上。"
              type="info"
              showIcon
              style={{ marginBottom: 16 }}
            />
            <Space direction="vertical" style={{ width: '100%', gap: 16 }}>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '16px 24px', background: '#fafafa', borderRadius: 8 }}>
                <Space>
                  <CloudServerOutlined style={{ fontSize: 32, color: '#1890ff' }} />
                  <div>
                    <div style={{ fontSize: 16, fontWeight: 500 }}>S3 API端点</div>
                    <div style={{ color: '#8c8c8c', fontSize: 12 }}>http://localhost:9000</div>
                  </div>
                </Space>
                <Tag color="green">运行中</Tag>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '16px 24px', background: '#fafafa', borderRadius: 8 }}>
                <Space>
                  <KeyOutlined style={{ fontSize: 32, color: '#52c41a' }} />
                  <div>
                    <div style={{ fontSize: 16, fontWeight: 500 }}>默认凭据</div>
                    <div style={{ color: '#8c8c8c', fontSize: 12 }}>Access Key: powerfs / Secret Key: powerfs123</div>
                  </div>
                </Space>
                <Button onClick={loadAccessKeys}>
                  查看所有密钥
                </Button>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '16px 24px', background: '#fafafa', borderRadius: 8 }}>
                <Space>
                  <DatabaseOutlined style={{ fontSize: 32, color: '#fa8c16' }} />
                  <div>
                    <div style={{ fontSize: 16, fontWeight: 500 }}>数据存储</div>
                    <div style={{ color: '#8c8c8c', fontSize: 12 }}>元数据存储在Master，实际数据存储在Volume Server节点</div>
                  </div>
                </Space>
                <Tag color="blue">分布式</Tag>
              </div>
            </Space>
          </Card>
        </Tabs.TabPane>
      </Tabs>

      <Modal
        title="创建Bucket"
        visible={createModalVisible}
        onCancel={() => setCreateModalVisible(false)}
        footer={null}
      >
        <Form form={form} layout="vertical" onFinish={handleCreateBucket}>
          <Form.Item
            name="name"
            label="Bucket名称"
            rules={[{ required: true, message: '请输入Bucket名称' }]}
          >
            <Input placeholder="请输入Bucket名称" />
          </Form.Item>
          <Form.Item>
            <Space>
              <Button onClick={() => setCreateModalVisible(false)}>取消</Button>
              <Button type="primary" htmlType="submit">创建</Button>
            </Space>
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title="上传文件"
        visible={uploadModalVisible}
        onCancel={() => { setUploadModalVisible(false); setUploadFile(null); setUploadKey(''); }}
        footer={null}
      >
        <Space direction="vertical" style={{ width: '100%' }}>
          <Input
            placeholder="对象名称（Key）"
            value={uploadKey}
            onChange={(e) => setUploadKey(e.target.value)}
          />
          <input
            type="file"
            onChange={(e) => setUploadFile(e.target.files?.[0] || null)}
            style={{ width: '100%' }}
          />
          {uploadFile && (
            <span style={{ color: '#8c8c8c' }}>
              已选择文件: {uploadFile.name} ({formatBytes(uploadFile.size)})
            </span>
          )}
          <Space>
            <Button onClick={() => { setUploadModalVisible(false); setUploadFile(null); setUploadKey(''); }}>取消</Button>
            <Button type="primary" onClick={handleUploadObject}>上传</Button>
          </Space>
        </Space>
      </Modal>

      <Modal
        title="创建访问密钥"
        visible={keyModalVisible}
        onCancel={() => setKeyModalVisible(false)}
        footer={null}
      >
        <Form form={keyForm} layout="vertical" onFinish={handleCreateAccessKey}>
          <Form.Item
            name="access_key"
            label="Access Key"
            rules={[{ required: true, message: '请输入Access Key' }]}
          >
            <Input placeholder="例如: AKIAEXAMPLE" />
          </Form.Item>
          <Form.Item
            name="secret_key"
            label="Secret Key"
            rules={[{ required: true, message: '请输入Secret Key' }]}
          >
            <Input.Password placeholder="请输入Secret Key" />
          </Form.Item>
          <Form.Item>
            <Space>
              <Button onClick={() => setKeyModalVisible(false)}>取消</Button>
              <Button type="primary" htmlType="submit">创建</Button>
            </Space>
          </Form.Item>
        </Form>
      </Modal>
    </div>
  )
}

export default S3
