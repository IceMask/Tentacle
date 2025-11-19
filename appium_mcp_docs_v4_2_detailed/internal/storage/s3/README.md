# internal/storage/s3

## client.go

### NewClient/PresignPut/PresignGet
**Purpose**：S3 基础封装。

**Logic — External Calls**
    - AWS SDK

**Logic — Local Implementation**
    - Presign 带内容类型/过期时间；最小权限策略。

**Errors & Edge Cases**
    - 凭证错误 -> 403；区域不匹配 -> 400。

**Acceptance**
    - Presign 链接可用且时效正确。


### InitiateMultipart/UploadPart/CompleteMultipart/AbortMultipart
**Purpose**：多分片上传。

**Logic — External Calls**
    - AWS SDK Multipart API

**Logic — Local Implementation**
    - 限制最小分片 5MB；校验 ETag 列表；失败自动 Abort。

**Errors & Edge Cases**
    - 分片丢失 -> 失败并清理。

**Acceptance**
    - 大文件成功率高；可断点续传；完成后可下载校验。
