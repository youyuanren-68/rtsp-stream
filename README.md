# RTSP转HLS/FLV流媒体服务

一个基于Spring Boot的高性能RTSP转HLS/FLV流媒体服务，支持实时视频流转换、自动重连、智能资源管理等功能。

## ✨ 特性

- 🎥 **双协议支持** - 同时支持HLS和FLV两种输出格式
- 🔄 **自动恢复** - FFmpeg进程异常退出时，通过拦截器和健康检查自动重启
- 🧹 **智能清理** - 自动清理过期文件，防止磁盘空间占用过多
- ⏱️ **自动停止** - 无客户端访问30分钟后自动停止FFmpeg进程，节省资源
- 📊 **实时监控** - 提供流状态查询和健康检查接口
- 🎬 **首帧优化** - 优化HLS首帧延迟，FLV支持流式传输
- 🔒 **安全防护** - 防止路径遍历攻击，输入验证

## 🛠️ 技术栈

- **后端框架**: Spring Boot 2.1.1
- **视频处理**: FFmpeg 4.0+
- **HLS播放**: HLS.js
- **FLV播放**: flv.js
- **构建工具**: Maven

## 📋 前置要求

- JDK 8+
- Maven 3.5+
- FFmpeg 4.0+

## 🚀 快速开始

### 1. 安装FFmpeg

**Windows:**
```bash
# 下载FFmpeg
# 访问 https://ffmpeg.org/download.html 下载Windows版本
# 解压到指定目录，例如: D:\AI\ffmpeg-8.1\
```

**Linux:**
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install ffmpeg

# CentOS/RHEL
sudo yum install epel-release
sudo yum install ffmpeg
```

**macOS:**
```bash
brew install ffmpeg
```

### 2. 克隆项目

```bash
git clone https://github.com/youyuanren-68/rtsp-stream.git
cd rtsp-stream
```

### 3. 配置FFmpeg路径

编辑 `src/main/resources/application.yml`:

```yaml
rtsp:
  ffmpeg:
    path: D:/AI/ffmpeg-8.1/bin/ffmpeg.exe  # Windows路径
    # path: /usr/bin/ffmpeg  # Linux路径
    # path: /usr/local/bin/ffmpeg  # macOS路径
  hls:
    output-path: D:/video/hls
    access-path: /rtspStream/hls
  flv:
    output-path: D:/video/flv
    access-path: /rtspStream/flv
```

### 4. 启动服务

```bash
# 方式一：使用启动脚本（Windows）
run.bat

# 方式二：使用Maven
mvn spring-boot:run

# 方式三：打包后运行
mvn clean package -DskipTests
java -jar target/rtsp-stream-1.0.0.jar
```

### 5. 访问播放器

- **HLS播放器**: http://localhost:9090/rtspStream/player
- **FLV播放器**: http://localhost:9090/rtspStream/flv-player

## 📖 API文档

### 启动HLS流

```bash
POST /rtspStream/start
Content-Type: application/x-www-form-urlencoded

rtspUrl=rtsp://example.com/stream&streamId=camera01
```

**响应:**
```json
{
  "code": 0,
  "msg": "HLS流已启动",
  "data": {
    "streamId": "camera01",
    "m3u8Url": "/rtspStream/hls/camera01/index.m3u8"
  }
}
```

### 启动FLV流

```bash
POST /rtspStream/startFlv
Content-Type: application/x-www-form-urlencoded

rtspUrl=rtsp://example.com/stream&streamId=camera01
```

**响应:**
```json
{
  "code": 0,
  "msg": "FLV流已启动",
  "data": {
    "streamId": "camera01",
    "flvUrl": "/rtspStream/flv/camera01/live.flv"
  }
}
```

### 停止流

```bash
POST /rtspStream/stop?streamId=camera01
```

### 停止所有流

```bash
POST /rtspStream/stopAll
```

### 查看所有活跃流

```bash
GET /rtspStream/streams
```

**响应:**
```json
{
  "code": 0,
  "msg": "success",
  "data": {
    "camera01": "running_hls",
    "camera02": "running_flv"
  }
}
```

### 检查流状态

```bash
GET /rtspStream/status?streamId=camera01
```

### 检查HLS文件就绪状态

```bash
GET /rtspStream/hlsReady?streamId=camera01
```

### 检查FLV文件就绪状态

```bash
GET /rtspStream/flvReady?streamId=camera01
```

## ⚙️ 配置说明

### 完整配置示例

```yaml
server:
  port: 9090

rtsp:
  hls:
    output-path: D:/video/hls          # HLS输出目录
    access-path: /rtspStream/hls       # HLS访问路径
    segment-time: 2                    # 分片时长（秒）
    list-size: 10                      # 播放列表大小
    cleanup-enabled: true              # 启用清理
    cleanup-interval: 120000           # 清理间隔（毫秒）

  flv:
    output-path: D:/video/flv          # FLV输出目录
    access-path: /rtspStream/flv       # FLV访问路径
    cleanup-enabled: true              # 启用清理
    cleanup-interval: 120000           # 清理间隔（毫秒）
    max-file-size-mb: 500              # 单个FLV文件最大MB
    max-file-age-hours: 24             # 文件保留小时数

  stream:
    auto-stop-enabled: true            # 启用自动停止
    auto-stop-timeout: 1800000         # 无访问超时（毫秒，30分钟）
    max-concurrent-streams: 50         # 最大并发流数量
    max-reconnect-attempts: 10         # 最大重连次数

  ffmpeg:
    path: D:\AI\ffmpeg-8.1\bin\ffmpeg.exe  # FFmpeg路径
    preset: ultrafast                  # 编码预设
    tune: zerolatency                  # 调优选项
    video-codec: libx264               # 视频编码
    audio-codec: aac                   # 音频编码
    audio-sample-rate: 44100           # 音频采样率
    rtsp-transport: tcp                # RTSP传输协议
```

### 配置项说明

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `rtsp.hls.output-path` | HLS输出目录 | D:/video/hls |
| `rtsp.hls.access-path` | HLS访问路径 | /rtspStream/hls |
| `rtsp.hls.segment-time` | HLS分片时长（秒） | 2 |
| `rtsp.hls.list-size` | HLS播放列表大小 | 10 |
| `rtsp.flv.output-path` | FLV输出目录 | D:/video/flv |
| `rtsp.flv.access-path` | FLV访问路径 | /rtspStream/flv |
| `rtsp.flv.max-file-size-mb` | FLV文件最大MB | 500 |
| `rtsp.stream.auto-stop-enabled` | 启用自动停止 | true |
| `rtsp.stream.auto-stop-timeout` | 自动停止超时（毫秒） | 1800000 (30分钟) |
| `rtsp.stream.max-concurrent-streams` | 最大并发流数量 | 50 |
| `rtsp.ffmpeg.path` | FFmpeg路径 | - |
| `rtsp.ffmpeg.preset` | 编码预设 | ultrafast |
| `rtsp.ffmpeg.tune` | 调优选项 | zerolatency |

## 🏗️ 项目结构

```
rtsp-stream/
├── src/main/java/com/video/stream/
│   ├── config/
│   │   ├── FilterConfig.java              # Filter配置
│   │   ├── HlsAccessInterceptor.java      # HLS拦截器
│   │   └── RtspHlsResourceConfig.java     # 资源映射配置
│   ├── controller/
│   │   ├── FlvStreamController.java       # FLV流式传输
│   │   └── RtspStreamController.java      # 主控制器
│   ├── filter/
│   │   └── StreamAccessFilter.java        # 访问记录Filter
│   ├── service/
│   │   ├── IRtspStreamService.java        # 服务接口
│   │   └── impl/
│   │       └── RtspStreamServiceImpl.java # 服务实现
│   └── RtspStreamApplication.java         # 启动类
├── src/main/resources/
│   ├── application.yml                    # 配置文件
│   ├── static/
│   │   ├── hls.min.js                     # HLS.js库
│   │   └── flv.min.js                     # flv.js库
│   └── templates/
│       ├── player.html                    # HLS播放器
│       └── flv-player.html                # FLV播放器
├── pom.xml                                # Maven配置
└── run.bat                                # Windows启动脚本
```

## 🔧 高级功能

### 自动恢复机制

系统内置了三层恢复机制，确保流媒体服务高可用：

1. **拦截器即时恢复** - 每次HLS/FLV请求时检查FFmpeg进程状态，发现进程死亡立即恢复（低延迟）
2. **定时健康检查** - 每60秒扫描一次所有流，发现异常自动重启（兜底保障）
3. **前端自动重连** - 播放器检测到错误时自动重新加载流

### 智能资源管理

- **文件大小限制** - FLV文件超过500MB自动重启FFmpeg进程并重建文件
- **定时清理** - 每2小时清理已停止的流目录，释放磁盘空间
- **自动停止** - 30分钟无访问自动停止FFmpeg进程，节省CPU和内存

### 流式传输

FLV使用`RandomAccessFile`实现流式传输，支持：
- 实时读取FFmpeg正在写入的文件
- 自动跟踪文件增长
- 智能等待新数据（最长720秒）
- 自动检测FFmpeg重启并切换文件

## 🐛 常见问题

### 1. FFmpeg路径配置错误

**问题**: 启动时提示"FFmpeg可执行文件不存在"

**解决**: 检查`application.yml`中的`rtsp.ffmpeg.path`配置是否正确

### 2. HLS首帧延迟久

**原因**: HLS需要等待FFmpeg生成第一个分片文件

**解决**:
- 减少`segment-time`配置（但会增加服务器压力）
- 使用FLV格式（首帧通常1-2秒）

### 3. FLV播放中断

**原因**: FFmpeg进程因网络问题退出

**解决**: 系统会自动检测并恢复，无需手动干预

### 4. 权限问题

**问题**: 无法创建输出目录或写入文件

**解决**: 确保输出目录有写入权限

## 📝 开发指南

### 本地开发

```bash
# 克隆项目
git clone https://github.com/your-username/rtsp-stream.git

# 进入项目目录
cd rtsp-stream

# 修改配置
# 编辑 src/main/resources/application.yml

# 运行项目
mvn spring-boot:run
```

### 打包部署

```bash
# 打包
mvn clean package -DskipTests

# 运行
java -jar target/rtsp-stream-1.0.0.jar
```

## 🤝 贡献

欢迎提交Issue和Pull Request！

## 📄 许可证

MIT License

## 📧 联系方式

如有问题或建议，请提交Issue。
