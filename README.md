# 📘 pat_etl 专利数据治理项目

## 工作交接说明文档

------

# 一、项目总体说明

本项目为专利数据治理 ETL 工程，采用分层架构设计，实现从原始数据到最终可用数据的全流程治理。

## 项目结构
pat_etl/
│
├── cli.py                # 命令行入口
│
├── jobs/
│     ├── ods_to_stage.py   #ods数据抽取到stage层，做简单标准化处理。
│     ├── stage_clean.py    #核心治理层，数据规范化
│     ├── stage_split.py    #分流数据到dwd解耦层
│     ├── finalize_instock.py   #抽取最终合格数据到最终表dwd_patents_in_stock
│     ├── finalize_unqualified.py   #抽取脏数据到最终表dwd_patents_unqualified
│
├── utils/
│
├── configs/
│     ├── test_a.yaml   #a类专利参数设置
│     ├── test_c.yaml   #c类专利参数设置
│     ├── test_d.yaml   #d类专利参数设置
│
│── original_version/   #原始代码版本
│     ├── ODS-Stage跑批.py    
│     ├── stage_clean_job.py
│     ├── 同时分流.py
│     ├── 插入unqualified.py
│     ├── 插入最终dwd表.py
│
│
│── csv_to_ods.py   #csv加载入库ods层
└── offset_migrate_stage.py     #stage表、dwd_patents_in_stock_x和dwd_patents_unqualified_x三表id平移功能



整体数据链路如下：

```
CSV
  ↓
ODS（原始入库层）
  ↓
Stage（标准化 + 规则清洗层）
  ↓
Stage 分流
  ↓
DWD 解耦层（文件校验层）
  ↓
ID 平移修正
  ↓
DWD 最终层
    ├── dwd_patents_in_stock
    └── dwd_patents_unqualified
```

------

# 二、设计目标

本项目设计目标：

- 支持千万级数据治理
- 支持分批执行（batch_id）
- 支持断点续跑
- 所有异常可追溯
- 清洗规则可扩展
- 支持多分区（A/C/D 等）合并

------

# 三、完整执行顺序（必须严格按顺序）

```
1️⃣ csv_to_ods
2️⃣ ods_to_stage
3️⃣ stage_clean
4️⃣ stage_split
5️⃣ 文件校验检查
6️⃣ finalize_instock
7️⃣ finalize_unqualified
```

⚠️ 不建议跳步骤执行。

------

# 四、各步骤详细说明

------

# Step 1：CSV → ODS

### 使用脚本

```
csv_to_ods.py ---需要手动调整csv所在文件路径和ods表名
```

### 功能

- 读取原始 CSV 文件
- 分块写入 ODS 表
- 不做业务清洗
- 保留所有原始字段

### 特点

- ODS 不做规则判断
- 不设强业务主键约束
- 仅作为原始数据缓冲层

### 目标表
ods_pat_raw_x_batch_01

------

# Step 2：ODS → Stage

### CLI 执行方式

```
python -m pat_etl.cli run --config configs/xxx.yaml --steps ods2stage
```

### 功能

- 从 ODS 抽取数据
- 做字段标准化处理
- 插入 Stage 表

### 当前实现的标准化内容包括：

- 法律状态映射
- ISO 语言码映射
- 日期格式统一
- IPC 主分类字段规范化
- 字段空字符串转 NULL

### 特点

- Stage 是正式治理层
- 保留原始字段用于对比
- 不删除数据，仅写入


------

# Step 3：Stage 清洗（核心治理层）

### 执行方式

```
python -m pat_etl.cli run --config configs/xxx.yaml --steps stage_clean
```

------

## 清洗设计原则

- 仅处理当前 batch_id
- 仅处理 is_valid=1 数据
- 不删除数据
- 所有异常记录 invalid_reason
- 支持分段处理
- 支持断点续跑
- 支持死锁自动重试

------

## 当前已实现清洗规则（详细）

------

### ① patent_id 重复判定

规则：

- 同一 batch_id 内
- 同一 patent_id
- 保留最小 stage_id
- 其余标记为：

```
is_valid = 0
invalid_reason = R_DUP_PATENT_ID
```

目的：

- 保证批次内主键唯一
- 防止重复写入最终表

------

### ② intci_main_name 规范化

处理内容：

- 处理 "$$$" 多值分隔
- 处理 "A > B" 层级结构
- 去除多余空格
- 统一分隔符格式
- 保留最完整分类路径

目的：

- 统一 IPC 主分类结构
- 提高后续统计准确性

------

### ③ 国家 / 省份修复

包括以下规则：

1. appl_country ≠ CN 且 appl_province 为空
    → 省份填充为国家中文名
2. appl_country = 'WO'
    → 省份置空
3. appl_country 属于 HK / TW / MO
    → 国家统一为 CN
4. application_origin 自动生成：

```
WO → PCT国际申请
CN → 境内专利
其他 → 境外专利
```

------

### ④ signory_item 格式修复

- 处理 “123.0” 形式
- 统一为整数形式

------

### ⑤ 其他字段修复

- 空字符串统一为 NULL
- 格式不合法字段修正
- 补齐部分缺失字段默认值

------

### 后续若有清洗规则的扩展，可以写成update语句直接放在代码中。

# Step 4：Stage 分流

### 执行

```
python -m pat_etl.cli run --config configs/xxx.yaml --steps stage_split
```

### 功能

按 is_valid 状态分流至：

```
dwd_patents_in_stock_x
dwd_patents_unqualified_x
```

------

# DWD 解耦层说明（重要）

Stage 分流后数据进入 DWD 解耦层。

该层主要用于：

- S3 文件存在性校验
- 文件大小合法性校验
- PDF / EPUB 可读性检测
- 文件损坏检测

此部分逻辑为独立项目执行。

设计目的：

- 清洗逻辑与文件检测解耦
- 提高可维护性
- 避免复杂逻辑耦合

------

# Step 5：Stage ID 平移 （优先使用方法1
### 1、先查询stage_id最大值，然后再建stage表的时候设定好stage_id起始值（默认使用）--需要执行在 ods数据导入stage层之前
### 2、使用脚本offset_migrate_stage.py（需要手动调整表名参数） --需要执行在 dwd解耦层导入到最终表之前

### SELECT COALESCE(MAX(stage_id), 0) AS off_d FROM dwd_patents_stage_d --查看最大stage_id
```
offset_migrate_stage.py
```

------

## 为什么必须执行？

由于：

- A / C / D 分区分别生成 stage_id
- 每个分区自增生成主键
- 合并写入最终表时必须保证全局唯一

必须执行：

```
stage_id = stage_id + offset
```

offset 计算方式：

- 获取目标表当前最大 ID
- 当前批次整体平移

目的：

- 避免主键冲突
- 支持多分区合并

⚠️ 若跳过此步骤，Final 表将发生主键冲突。

------

# Step 6：写入最终有效表

### 执行

```
python -m pat_etl.cli run --config configs/xxx.yaml --steps final_instock
```

### 生成表

```
dwd_patents_in_stock
```

### 写入规则

```
WHERE is_valid = 1
```

特点：

- INSERT IGNORE
- 分段推进
- 不再做清洗

------

# Step 7：写入最终脏数据表

### 执行

```
python -m pat_etl.cli run --config configs/xxx.yaml --steps final_unqualified
```

### 生成表

```
dwd_patents_unqualified
```

### 写入规则

```
WHERE is_valid = 0
```

特点：

- 保留 invalid_reason
- 追加 data_source
- 可审计可回溯

------

# 五、配置文件说明

路径：

```
configs/*.yaml
```

关键字段：

```
mysql:
  host:
  port:
  user:
  password:
  database:

dataset:
  table_id:
  data_source:
  batch_id:
  stage_table:
  instock_table:
  unqualified_table:
  final_unqualified_table:

run:
  segment:
  chunk:
  max_retries:
  sleep_seconds:
```

------

# 六、断点续跑机制

支持：

- 分段更新
- checkpoint 表记录
- 重试机制
- INSERT IGNORE 幂等执行

若中断：

- 可重新执行 stage_clean
- 可单独重跑 finalize
- ODS 数据不会丢失

------

# 七、性能说明

已验证运行环境：

- 千万级数据规模
- segment = 200000
- chunk = 50000

推荐默认配置：

```
segment: 200000
chunk: 50000
```

若出现锁等待 (1205)：

- 减小 segment
- 避开高峰期运行

------

# 八、治理原则

1. 不直接删除数据
2. ODS 不做清洗
3. 所有规则集中在 Stage 层
4. 所有异常记录 invalid_reason
5. 数据可回溯

------

# 九、项目定位总结

本项目是一个：

- 可扩展
- 可追溯
- 支持多分区
- 支持千万级规模
- 支持自动化运行

的专利数据治理 ETL 框架。