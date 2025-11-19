# cmd/worker

## main.go


### computeConcurrency()
**Purpose**：根据设备数与 CPU 核数估算并发。

**Logic — External Calls**
    - （无）

**Logic — Local Implementation**
    - limit = min(int(devices*1.5), 2*NumCPU)。
    - 支持环境变量/配置强制覆盖。

**Errors & Edge Cases**
    - devices 未配置 -> 回退到 NumCPU。

**Acceptance**
    - 覆盖后可热更新或重启生效；并发观测指标随之变化。



### initDeps()
**Purpose**：初始化 Appium 客户端、并发控制器、快照缓存与制品上传。

**Logic — External Calls**
    - appium.NewClient()
    - worker.NewConcurrencyController()
    - artifacts.NewUploader()

**Logic — Local Implementation**
    - 初始化 SnapshotCache，配置 TTL=~1.5s。
    - 对 Appium 客户端设置 keep-alive 与超时。

**Errors & Edge Cases**
    - Appium 不可连 -> 阻断启动或进入降级（仅视觉定位）。

**Acceptance**
    - Appium RTT p95 < 350ms；快照命中率达到目标；上传通路可用。



### serveGRPC()/registerAndHeartbeat()/gracefulShutdown()
**Purpose**：暴露 gRPC 服务、注册至 Orchestrator 并定时心跳；优雅下线。

**Logic — External Calls**
    - orchestrator.RegisterWorker()
    - StartHeartbeat()

**Logic — Local Implementation**
    - 心跳失败累计 3 次仅报警；注册时带能力声明（caps）。

**Errors & Edge Cases**
    - 重复注册/ID 冲突 -> 采用租约键替换。

**Acceptance**
    - 中断网络后重连成功；停止时先注销再停服；≤30s 退出。
