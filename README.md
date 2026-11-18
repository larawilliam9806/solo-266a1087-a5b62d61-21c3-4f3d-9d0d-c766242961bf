# FASTQ Demultiplexing Service

测序中心混合运行的双端 FASTQ 按样本拆分服务。基于 **FastAPI + SQLite + 本地文件**，
支持样本表版本化更正、失败原子回滚与重启恢复。

## 业务规则

1. 上传：运行号（`run_id`）、**未压缩** R1/R2 FASTQ、样本表（CSV：`sample_id,barcode`，可选表头）。
2. 样本表约束：
   - 标签必须是恰好 8 位 `A/C/G/T`；
   - 样本号唯一、标签互异；
   - 样本号只允许字母、数字、`_`、`.`、`-`，且不得为 `.` / `..`。
3. R1 前 8 位（含对应 8 位质量字符）作为标签并在输出中去除；R2 原样输出。
4. 逐条校验：
   - FASTQ 四行结构（`@` 头、序列、`+` 行、质量）；
   - 序列与质量等长；
   - R1 读名以 `/1` 结尾、R2 以 `/2` 结尾，且去掉后缀后两侧读名一致；
   - R1/R2 记录条数一致；R1 至少含 8 个标签碱基。
5. 归属：在汉明距离 ≤ 1 的样本中取**唯一最近者**；
   - 没有任何样本在距离 1 内 → 计入 `far_pairs`（超距）；
   - 距离 1 内存在并列最近 → 计入 `tie_pairs`（并列）；
   - 两类无法归属的读段**成对**写入未分配文件。
6. 样本表更正：携带客户端所见版号 `base_version` 提交整表，从**原始 R1/R2 重新拆分**；
   - 每个版本独立保存样本表、结果文件与计数，历史版本始终可读；
   - 所见版号不是当前版（过期更正）→ `409`，当前版不变；
   - 同一版并发更正只允许一份生效（进程内锁 + SQLite `BEGIN IMMEDIATE`）。
7. 原子性与恢复：
   - 结果先写入 `vN.tmp/`，全部 flush + fsync 后原子 rename；
   - 输入校验失败或写盘失败不改变当前版，半成品目录立即清除；
   - 重启时所有残留 `processing` 版本标记为 `failed` 并清理 `.tmp`，
     当前版回退到最近的就绪版，绝不暴露半成品。

## 目录与数据布局

```
/data
├── demux.db                  # SQLite（WAL）：runs / versions / samples
└── runs/<run_id>/
    ├── originals/            # 原始 R1/R2 与每版样本表（更正重拆的输入）
    │   ├── R1.fastq
    │   ├── R2.fastq
    │   ├── sample_sheet_v1.csv
    │   └── sample_sheet_v2.csv
    └── v1/                   # 每版结果，原子 rename 落地
        ├── samples/<sample_id>_R1.fastq
        ├── samples/<sample_id>_R2.fastq
        ├── undetermined_R1.fastq
        └── undetermined_R2.fastq
```

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/runs` | multipart 上传 `run_id`、`r1`、`r2`、`sample_sheet`，返回 v1 统计 `201` |
| `POST` | `/runs/{run_id}/corrections` | 表单 `base_version` + 新样本表，返回新版号 `201`；过期 `409` |
| `GET`  | `/runs` | 运行列表 |
| `GET`  | `/runs/{run_id}` | 运行详情（当前版、全部版号、状态/错误） |
| `GET`  | `/runs/{run_id}/versions/{version}` | 某版计数与各样本 reads |
| `GET`  | `/runs/{run_id}/versions/{version}/samples/{sample_id}/R1\|R2` | 下载样本 FASTQ |
| `GET`  | `/runs/{run_id}/versions/{version}/undetermined/R1\|R2` | 下载未分配 FASTQ |
| `GET`  | `/health` | 健康检查 |

状态码：校验错误 `422`，不存在 `404`，冲突/过期更正/读取未就绪版 `409`，
写盘失败 `507`，文件超限 `413`，重复运行号 `409`。
交互式文档：服务启动后访问 `/docs`。

### 示例

```bash
# 上传
curl -X POST http://localhost:8000/runs \
  -F run_id=RUN001 \
  -F r1=@R1.fastq -F r2=@R2.fastq \
  -F sample_sheet=@sheet.csv

# 基于所见的 v1 更正样本表
curl -X POST http://localhost:8000/runs/RUN001/corrections \
  -F base_version=1 -F sample_sheet=@sheet_v2.csv

# 读取 v2 的样本文件与未分配文件
curl -OJ http://localhost:8000/runs/RUN001/versions/2/samples/s1/R1
curl -OJ http://localhost:8000/runs/RUN001/versions/2/undetermined/R2
```

样本表格式（UTF-8 CSV，表头可省略）：

```csv
sample_id,barcode
s1,AAAAAAAA
s2,AAAAAAAC
```

## Docker Compose 启动

```bash
docker compose up --build -d
curl http://localhost:8000/health
```

数据持久化在命名卷 `demux-data`（容器内 `/data`）。调整上传大小上限可设置环境变量
`DEMUX_MAX_UPLOAD_BYTES`，数据目录由 `DEMUX_DATA_DIR` 控制（默认 `/data`）。

## 本地开发与测试

```bash
python -m pip install -r requirements.txt
python -m pip install pytest httpx
python -m pytest -q
DEMUX_DATA_DIR=./data uvicorn app.main:app --reload
```

## 并发与可靠性说明

- SQLite 开启 WAL、`busy_timeout=30s`，所有写操作使用 `BEGIN IMMEDIATE` 短事务。
- 同运行的更正由两层机制保证「同版并发只一份生效」：
  1. 进程内按 `run_id` 加锁，串行化单 worker 内的请求；
  2. 多 worker/多进程部署时，在数据库写锁内检查是否已存在版本号更大的
     `processing`/`ready` 行（`runs.current_ver` 在重拆完成后才更新，
     因此用在途版本行识别并发），失败者整体回滚且不留 `failed` 行，直接返回 `409`。
  历史上崩溃留下的 `failed` 版本不拦截新的更正。
- 输出文件整目录原子替换（`rename` + 目录 fsync），数据库状态与文件可见性一致：
  只有 `ready` 版本提供下载，`failed`/`processing` 一律 `409`。
- 原始 FASTQ 与每版样本表永久保留，任何更正都从原始读段重新拆分，不依赖历史输出。
- 重复上传已存在的 `run_id` 仅返回 `409`，不会改动该运行的任何现有数据。
