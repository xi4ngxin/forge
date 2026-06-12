文件标准化器 + ODS 表创建器
你之前的系统有 profile → analyze → preprocess → load 四个环节。按新视角，load 是数据库自己的事，preprocess 是核心。

旧 vs 新

旧：文件 → 配置 → 预处理 → Stream Load → Iceberg 表
     ↑_______________________|   ↑
         forge 管全套            Doris 只是目标

新：文件 → forge → 标准 CSV + DDL
                    ↑              ↑
               forge 只负责生成    Doris 自己导入
生成产物变化
程序的运行时职责变得非常聚焦：


# /programs/{name}/etl_{name}.py
# 依赖: pandas, openpyxl（或 vendored xlsx2csv）
# 不依赖: doris, airflow, 网络

import pandas as pd
from pathlib import Path

# ---- 所有边角情况在生成时已固化 ----
INPUT_CONFIG = {
    "encoding": "gbk",              # 探测结果
    "delimiter": "|",               # 探测结果
    "header_rows": 1,
    "skip_rows_before": 5,          # 跳过无关行头
    "skip_rows_after_pattern": "合计",  # 跳过汇总行
    "column_types": {
        "日期": "str",
        "金额": "float64",
        "门店编码": "str",
    },
    "output_columns": [             # 重命名 + 顺序
        "dt", "store_code", "amount", "category"
    ],
}

OUTPUT_CONFIG = {
    "csv_delimiter": ",",
    "csv_encoding": "utf-8",
    "csv_quoting": 1,               # QUOTE_ALL for safety
}

# ---- 生成的转换逻辑直接硬编码 ----
def convert(file_path: str, output_dir: str) -> str:
    """把输入文件转为标准 CSV，返回 CSV 路径。"""
    raw = pd.read_csv(
        file_path,
        encoding=INPUT_CONFIG["encoding"],
        sep=INPUT_CONFIG["delimiter"],
        skiprows=INPUT_CONFIG["skip_rows_before"],
        ...
    )
    # 列重命名、类型转换、异常值处理
    df = raw.rename(columns=COLUMN_MAP)
    df = df.astype(INPUT_CONFIG["column_types"])
    
    output_path = Path(output_dir) / f"{Path(file_path).stem}_standard.csv"
    df.to_csv(output_path, 
              sep=OUTPUT_CONFIG["csv_delimiter"],
              encoding=OUTPUT_CONFIG["csv_encoding"],
              index=False, quoting=OUTPUT_CONFIG["csv_quoting"])
    return str(output_path)
DDL 生成：


-- /programs/{name}/schema.sql
CREATE TABLE IF NOT EXISTS ods.abc_digital (
    dt          DATE,
    store_code  VARCHAR(64),
    amount      DECIMAL(16, 2),
    category    VARCHAR(128)
)
DUPLICATE KEY(dt, store_code)
DISTRIBUTED BY HASH(store_code) BUCKETS 3
PROPERTIES ("replication_num" = "1");
这里的 DDL 不是 AI 凭空生成的，而是：

列名来自 profile 后的列映射
类型来自 column_types（加上 AI 的推断）
分布键来自 AI 对业务的理解（按什么字段分桶）
所有内容都在 forge 代理循环中生成 + 验证
forge 的工作流

forge Workflow: generate_csv_converter
  
  tool: profile_file()
    → 输出: 列结构、编码、分隔符、空值率、异常样例
  
  tool: infer_schema()
    → 输出: 建议的列类型映射、ODS 分布键建议
  
  tool: generate_converter()          ← 核心
    → 输出: Python 脚本 + DDL
    → 基于 profile 把所有边角硬编码
  
  tool: validate_in_sandbox()
    → 用实际文件跑一次 convert()
    → 检查输出 CSV 是否符合标准（utf-8, 逗号分隔, 引号正确）
    → 返回: OK 或者错误详情
  
  tool: register_program()
    → 保存到 /programs/{name}/
    → 注册调度（cron file / scheduler 回调）
  
  Guardrails:
    · validate 失败 → LLM 分析 → 修脚本 → 再验证
    · 最多 3 次生成 → 失败上报
    · StepEnforcer: profile → infer → generate → validate → register
DDL 生成的特殊处理
DDL 生成需要额外的安全保障 —— 不是所有列都适合做 DISTRIBUTED KEY：


tool: infer_schema() 的输出:
  {
    "columns": [
      {"name": "dt", "inferred_type": "DATE", "null_ratio": 0.0, "distinct_ratio": 0.02},
      {"name": "store_code", "inferred_type": "VARCHAR(64)", "null_ratio": 0.0, "distinct_ratio": 0.3},
      {"name": "amount", "inferred_type": "DECIMAL(16,2)", "null_ratio": 0.0},
      {"name": "memo", "inferred_type": "VARCHAR(500)", "null_ratio": 0.1, "max_length": 120},
    ],
    "key_candidates": ["store_code", "dt"],  ← AI 建议
    "distributed_key": "store_code",
    "partition_by": "dt"
  }
这里 AI 的建议不是最终决定——可以在 register 之前需要人工确认，也可以像你现有的自动批准机制（连续成功 3 次后跳过确认）。

程序化后的运行方式

文件到达 /landing/
    ↓
/usr/bin/python /programs/abc_digital/etl_abc_digital.py /landing/20240612.csv /output/
    ↓
输出 /output/20240612_standard.csv
    ↓
数据库原生导入（Doris Stream Load / 或 COPY INTO / 或 LOAD DATA）
    ↓
程序退出码 0 → 清理临时文件
程序退出码 ≠0 → 写错误日志 → 触发 forge repair
这个模式的好处是生成的程序完全独立于 forge 和调度器，可以跑在任何能装 Python 的环境上。调度可以用 cron / systemd timer / Windows Task Scheduler / 甚至 CI/CD 触发。