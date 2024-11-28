# -*- coding:utf-8 -*-
# @FileName  :core.py
# @Time      :2024/11/27 18:13:47
# @Author    :xw

import sqlite3
import time
import inspect
import traceback
import json
import ast
from datetime import datetime, timezone, timedelta
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List, Optional
import textwrap
from abc import ABC, abstractmethod
import csv


class Exporter(ABC):
    """导出器基类"""

    @abstractmethod
    def export_detail(self, data: Dict, filepath: str) -> None:
        """导出详情"""
        pass

    @abstractmethod
    def export_statistics(self, data: Dict, filepath: str) -> None:
        """导出统计信息"""
        pass

    @abstractmethod
    def export_list(self, data: List[Dict], filepath: str) -> None:
        """导出列表"""
        pass


class TxtExporter(Exporter):
    """文本导出器"""

    def export_detail(self, data: Dict, filepath: str) -> None:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(f"记录ID: {data['id']}\n")
            f.write("\n=== 函数信息 ===\n")
            f.write(f"函数名: {data['func_name']}\n")
            f.write(f"模块路径: {data['module_path']}\n")
            if data["doc_string"]:
                f.write(f"文档说明: {data['doc_string']}\n")
            f.write("\n源代码:\n")
            f.write(f"{data['source_code']}\n")
            # ... 其他字段类似处理

    def export_statistics(self, data: Dict, filepath: str) -> None:
        with open(filepath, "w", encoding="utf-8") as f:
            for func_name, stats in data.items():
                f.write(f"\n=== 函数统计: {func_name} ===\n")
                for key, value in stats.items():
                    if key != "错误类型统计":
                        f.write(f"{key}: {value}\n")
                if "错误类型统计" in stats:
                    f.write("\n错误类型分布:\n")
                    for error_type, count in stats["错误类型统计"].items():
                        f.write(f"- {error_type}: {count}次\n")

    def export_list(self, data: List[Dict], filepath: str) -> None:
        with open(filepath, "w", encoding="utf-8") as f:
            for i, record in enumerate(data, 1):
                f.write(f"\n--- 记录 {i} ---\n")
                f.write(f"记录ID: {record['id']}\n")
                f.write(f"函数名: {record['function']}\n")
                f.write(f"文档说明: {record['documentation']}\n")
                exec_info = record["last_execution"]
                f.write(f"执行时间: {exec_info['timestamp']}\n")
                f.write(f"耗时: {exec_info['execution_time']}\n")
                # ... 其他字段


class CsvExporter(Exporter):
    """CSV导出器"""

    def export_detail(self, data: Dict, filepath: str) -> None:
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["字段", "值"])
            writer.writerow(["记录ID", data["id"]])
            writer.writerow(["函数名", data["func_name"]])
            writer.writerow(["模块路径", data["module_path"]])
            # ... 其他字段

    def export_statistics(self, data: Dict, filepath: str) -> None:
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            # 写入表头
            headers = [
                "函数名",
                "总调用次数",
                "成功次数",
                "失败次数",
                "成功率",
                "平均执行时间",
                "最短执行时间",
                "最长执行时间",
                "首次调用",
                "最后调用",
            ]
            writer.writerow(headers)
            # 写入数据
            for func_name, stats in data.items():
                row = [
                    func_name,
                    stats["总调用次数"],
                    stats["成功次数"],
                    stats["失败次数"],
                    stats["成功率"],
                    stats["平均执行时间"],
                    stats["最短执行时间"],
                    stats["最长执行时间"],
                    stats["首次调用"],
                    stats["最后调用"],
                ]
                writer.writerow(row)

    def export_list(self, data: List[Dict], filepath: str) -> None:
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            headers = [
                "记录ID",
                "函数名",
                "文档说明",
                "执行时间",
                "耗时",
                "参数",
                "返回值/错误",
            ]
            writer.writerow(headers)
            for record in data:
                exec_info = record["last_execution"]
                writer.writerow(
                    [
                        record["id"],
                        record["function"],
                        record["documentation"],
                        exec_info["timestamp"],
                        exec_info["execution_time"],
                        str(exec_info["arguments"]),
                        exec_info.get("return_value", exec_info.get("error", "")),
                    ]
                )


class HtmlExporter(Exporter):
    """HTML导出器"""

    def _get_html_template(self, title: str, content: str) -> str:
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>{title}</title>
            <style>
                body {{ 
                    font-family: Arial, sans-serif; 
                    margin: 20px; 
                    background-color: #f5f5f5;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    background-color: white;
                    padding: 20px;
                    border-radius: 8px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                .record {{ 
                    margin: 20px 0; 
                    padding: 15px;
                    border: 1px solid #e0e0e0;
                    border-radius: 4px;
                }}
                .section {{ 
                    margin: 10px 0; 
                    padding: 10px;
                    background-color: #f8f9fa;
                    border-radius: 4px;
                }}
                .error {{ color: #dc3545; }}
                .success {{ color: #28a745; }}
                pre {{ 
                    background: #f8f9fa; 
                    padding: 10px; 
                    border-radius: 4px;
                    overflow-x: auto;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin: 10px 0;
                }}
                th, td {{
                    padding: 8px;
                    border: 1px solid #dee2e6;
                    text-align: left;
                }}
                th {{
                    background-color: #f8f9fa;
                }}
                .stats {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                    gap: 10px;
                }}
                .stat-item {{
                    padding: 10px;
                    background-color: #f8f9fa;
                    border-radius: 4px;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>{title}</h1>
                {content}
            </div>
        </body>
        </html>
        """

    def export_detail(self, data: Dict, filepath: str) -> None:
        """导出详细信息到HTML"""
        content = f"""
        <div class="record">
            <div class="section">
                <h2>函数信息</h2>
                <p><strong>记录ID:</strong> {data['id']}</p>
                <p><strong>函数名:</strong> {data['func_name']}</p>
                <p><strong>模块路径:</strong> {data['module_path']}</p>
                <p><strong>文档说明:</strong> {data['doc_string']}</p>
                <h3>源代码:</h3>
                <pre><code>{data['source_code']}</code></pre>
            </div>
            
            <div class="section">
                <h2>执行信息</h2>
                <p><strong>执行时间:</strong> {data['timestamp']}</p>
                <p><strong>耗时:</strong> {data['execution_time']:.4f}s</p>
                <p><strong>执行状态:</strong> 
                    <span class="{'success' if data['status'] == 'success' else 'error'}">
                        {data['status']}
                    </span>
                </p>
                <p><strong>调用参数:</strong></p>
                <pre><code>args: {data['args']}\nkwargs: {data['kwargs']}</code></pre>
                
                {f'<p><strong>返回值:</strong> {data["return_value"]}</p>' 
                 if data["status"] == "success" else
                 f'<div class="error"><strong>错误信息:</strong><br>{data["error_message"]}<br><pre>{data["error_traceback"]}</pre></div>'}
            </div>
            
            <div class="section">
                <h2>依赖信息</h2>
                <pre><code>{data['dependencies']}</code></pre>
            </div>
            
            <div class="section">
                <h2>标签</h2>
                <p>{data['tags']}</p>
            </div>
        </div>
        """
        html = self._get_html_template("函数执行详情", content)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html)

    def export_statistics(self, data: Dict, filepath: str) -> None:
        """导出统计信息到HTML"""
        content = []
        for func_name, stats in data.items():
            content.append(
                f"""
            <div class="record">
                <h2>函数统计: {func_name}</h2>
                <div class="stats">
                    <div class="stat-item">
                        <strong>总调用次数:</strong> {stats['总调用次数']}
                    </div>
                    <div class="stat-item">
                        <strong>成功次数:</strong> {stats['成功次数']}
                    </div>
                    <div class="stat-item">
                        <strong>失败次数:</strong> {stats['失败次数']}
                    </div>
                    <div class="stat-item">
                        <strong>成功率:</strong> {stats['成功率']}
                    </div>
                </div>
                <div class="section">
                    <h3>执行时间统计</h3>
                    <p><strong>平均执行时间:</strong> {stats['平均执行时间']}</p>
                    <p><strong>最短执行时间:</strong> {stats['最短执行时间']}</p>
                    <p><strong>最长执行时间:</strong> {stats['最长执行时间']}</p>
                </div>
                <div class="section">
                    <h3>调用时间</h3>
                    <p><strong>首次调用:</strong> {stats['首次调用']}</p>
                    <p><strong>最后调用:</strong> {stats['最后调用']}</p>
                </div>
                {f'''
                <div class="section">
                    <h3>错误类型分布</h3>
                    <table>
                        <tr><th>错误类型</th><th>次数</th></tr>
                        {''.join(f"<tr><td>{error_type}</td><td>{count}</td></tr>"
                                for error_type, count in stats['错误类型统计'].items())}
                    </table>
                </div>
                ''' if '错误类型统计' in stats else ''}
            </div>
            """
            )
        html = self._get_html_template("函数统计信息", "\n".join(content))
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html)

    def export_list(self, data: List[Dict], filepath: str) -> None:
        """导出列表信息到HTML"""
        content = []
        for record in data:
            exec_info = record["last_execution"]
            content.append(
                f"""
            <div class="record">
                <h3>记录 {record['id']}</h3>
                <table>
                    <tr>
                        <th>函数名</th>
                        <td>{record['function']}</td>
                    </tr>
                    <tr>
                        <th>文档说明</th>
                        <td>{record['documentation']}</td>
                    </tr>
                    <tr>
                        <th>执行时间</th>
                        <td>{exec_info['timestamp']}</td>
                    </tr>
                    <tr>
                        <th>耗时</th>
                        <td>{exec_info['execution_time']}</td>
                    </tr>
                    <tr>
                        <th>参数</th>
                        <td><pre>{exec_info['arguments']}</pre></td>
                    </tr>
                    <tr>
                        <th>结果</th>
                        <td class="{'success' if 'return_value' in exec_info else 'error'}">
                            {exec_info.get('return_value', exec_info.get('error', ''))}
                        </td>
                    </tr>
                </table>
            </div>
            """
            )
        html = self._get_html_template("函数执行列表", "\n".join(content))
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(html)


class FuncKeeper:
    def __init__(self, db_path: str = "funckeeper.db", timezone_offset: float = None):
        """
        初始化FuncKeeper

        Args:
            db_path: 数据库文件路径
            timezone_offset: 时区偏移量（小时），如 8.0 表示 UTC+8，-5.0 表示 UTC-5
                           默认为None，使用系统本地时区
        """
        self.db_path = Path(db_path).resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # 设置时区
        if timezone_offset is not None:
            offset = timedelta(hours=timezone_offset)
            self.timezone = timezone(offset)
        else:
            # 使用系统本地时区
            self.timezone = datetime.now(timezone.utc).astimezone().tzinfo

        self._init_db()

    def _init_db(self):
        """初始化数据库表结构"""
        with sqlite3.connect(self.db_path) as conn:
            # 检查表是否存在
            cursor = conn.execute(
                """
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='function_records'
            """
            )
            table_exists = cursor.fetchone() is not None

            if not table_exists:
                # 如果表不存在，创建新表
                conn.execute(
                    """
                    CREATE TABLE function_records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        func_name TEXT,
                        module_path TEXT,
                        source_code TEXT,
                        doc_string TEXT,
                        dependencies TEXT,
                        args TEXT,
                        kwargs TEXT,
                        return_value TEXT,
                        execution_time REAL,
                        status TEXT,
                        error_type TEXT,
                        error_message TEXT,
                        error_traceback TEXT,
                        error_state TEXT,
                        tags TEXT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """
                )
            else:
                # 如果表存在，检查是否需要添加 doc_string 列
                cursor = conn.execute("PRAGMA table_info(function_records)")
                columns = [row[1] for row in cursor.fetchall()]

                if "doc_string" not in columns:
                    # 添加新列
                    conn.execute(
                        "ALTER TABLE function_records ADD COLUMN doc_string TEXT"
                    )
                    print("数据库已更新：添加了 doc_string 列")

    def _get_current_timestamp(self) -> str:
        """获取当前时区的时间戳"""
        return datetime.now(self.timezone).isoformat()

    def __call__(self, tags: List[str] = None):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                start_time = time.time()

                # 获取并处理源代码和文档字符串
                source_code = textwrap.dedent(inspect.getsource(func)).strip()
                doc_string = inspect.getdoc(func) or ""

                # 获取并打印依赖
                try:
                    deps = self._get_dependencies(func)
                except Exception as e:
                    deps = {"imports": []}

                try:
                    result = func(*args, **kwargs)
                    record = {
                        "func_name": func.__name__,
                        "module_path": inspect.getmodule(func).__file__,
                        "source_code": source_code,
                        "doc_string": doc_string,
                        "dependencies": deps,
                        "args": json.dumps(self._serialize_args(args)),
                        "kwargs": json.dumps(self._serialize_args(kwargs)),
                        "tags": ",".join(tags) if tags else "",
                        "status": "success",
                        "return_value": json.dumps(self._serialize_args(result)),
                        "error_type": None,
                        "error_message": None,
                        "error_traceback": None,
                        "error_state": None,
                        "execution_time": time.time() - start_time,
                    }
                    self._save_record(record)
                    return result
                except Exception as e:

                    record = {
                        "func_name": func.__name__,
                        "module_path": inspect.getmodule(func).__file__,
                        "source_code": source_code,
                        "doc_string": doc_string,
                        "dependencies": deps,
                        "args": json.dumps(self._serialize_args(args)),
                        "kwargs": json.dumps(self._serialize_args(kwargs)),
                        "tags": ",".join(tags) if tags else "",
                        "status": "error",
                        "return_value": None,
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "error_traceback": traceback.format_exc(),
                        "error_state": json.dumps(self._get_error_state(args, kwargs)),
                        "execution_time": time.time() - start_time,
                    }
                    self._save_record(record)
                    raise

            return wrapper

        return decorator

    def _serialize_args(self, value: Any) -> Any:
        """序列化参数为JSON可序列化格式"""
        if isinstance(value, (str, int, float, bool, type(None))):
            return value
        elif isinstance(value, (list, tuple)):
            return [self._serialize_args(item) for item in value]
        elif isinstance(value, dict):
            return {str(k): self._serialize_args(v) for k, v in value.items()}
        else:
            return str(value)

    def _get_dependencies(self, func) -> Dict[str, List[str]]:
        """获取函数的依赖包信息"""
        imports = []

        try:
            # 获取函数源代码并处理缩进
            func_source = textwrap.dedent(inspect.getsource(func))
            func_tree = ast.parse(func_source)

            # 分析函数内的导入
            for node in ast.walk(func_tree):
                if isinstance(node, ast.Import):
                    for name in node.names:
                        imports.append(name.name)
                elif isinstance(node, ast.ImportFrom):
                    module_name = node.module or ""
                    for name in node.names:
                        imports.append(f"{module_name}.{name.name}")

            # 获取函数所在模块
            module = inspect.getmodule(func)
            if module:
                # 获取模块源代码
                try:
                    module_source = inspect.getsource(module)
                    module_tree = ast.parse(module_source)

                    # 分析模块级别的导入（仅在 if __name__ == "__main__": 之前的部分）
                    for node in ast.walk(module_tree):
                        if isinstance(node, ast.If):
                            if isinstance(node.test, ast.Compare):
                                if (
                                    isinstance(node.test.left, ast.Name)
                                    and node.test.left.id == "__name__"
                                ):
                                    break
                        if isinstance(node, ast.Import):
                            for name in node.names:
                                if name.name not in imports:
                                    imports.append(name.name)
                        elif isinstance(node, ast.ImportFrom):
                            module_name = node.module or ""
                            for name in node.names:
                                import_name = f"{module_name}.{name.name}"
                                if import_name not in imports:
                                    imports.append(import_name)
                except Exception as e:
                    print(f"警告: 获取模块依赖时出错: {str(e)}")
        except Exception as e:
            print(f"警告: 获取函数依赖时出错: {str(e)}")

        return {"imports": sorted(list(set(imports)))}

    def _get_error_state(self, args, kwargs) -> Dict:
        """获取错误生成的参数状态"""
        return {
            "args": self._serialize_args(args),
            "kwargs": self._serialize_args(kwargs),
        }

    def _save_record(self, record: Dict):
        """保存执行记录到数据库"""
        # 确保tags是正确的JSON格式

        # 确保dependencies是正确的JSON格式
        if isinstance(record.get("dependencies"), dict):
            record["dependencies"] = json.dumps(record["dependencies"])

        # 使用指定时区的时间戳
        record["timestamp"] = self._get_current_timestamp()

        with sqlite3.connect(self.db_path) as conn:
            columns = ", ".join(record.keys())
            placeholders = ", ".join(["?" for _ in record])
            sql = f"INSERT INTO function_records ({columns}) VALUES ({placeholders})"
            conn.execute(sql, list(record.values()))

    def _format_search_result(self, result: Dict) -> str:
        """格式化单条搜索结果"""
        output = []
        output.append(f"记录ID: {result['id']}")
        output.append(f"函数名: {result['function']}")
        if result["documentation"]:
            output.append(f"文档说明: {result['documentation']}")

        # 执行信息
        exec_info = result["last_execution"]
        output.append(f"执行时间: {exec_info['timestamp']}")
        output.append(f"耗时: {exec_info['execution_time']}")

        # 参数信息
        args = exec_info["arguments"]["args"]
        kwargs = exec_info["arguments"]["kwargs"]
        args_str = ", ".join(str(arg) for arg in args)
        kwargs_str = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        params = []
        if args_str:
            params.append(args_str)
        if kwargs_str:
            params.append(kwargs_str)
        output.append(f"调用参数: {', '.join(params)}")

        # 执行结果
        if "return_value" in exec_info:
            output.append(f"返回值: {exec_info['return_value']}")
        elif "error" in exec_info:
            output.append(f"错误信息: {exec_info['error']}")

        return "\n".join(output)

    def search(
        self,
        keyword: str = None,
        tags: List[str] = None,
        status: str = None,
        start_date: datetime = None,
        end_date: datetime = None,
    ) -> List[Dict]:
        """搜索执行记录

        Args:
            keyword (str, optional): 搜索关键词，可匹配函数名、源代码、文档说明或错误信息
            tags (List[str], optional): 标签列表，用于按标签筛选记录
            status (str, optional): 执行状态筛选
            start_date (datetime, optional): 开始日期，用于按时间范围筛选
            end_date (datetime, optional): 结束日期，用于按时间范围筛选

        Returns:
            List[Dict]: 符合条件的执行记录列表
        """
        conditions = []
        params = []

        if keyword:
            conditions.append(
                """
                (func_name LIKE ? OR 
                 source_code LIKE ? OR 
                 doc_string LIKE ? OR 
                 error_message LIKE ?)
            """
            )
            params.extend([f"%{keyword}%"] * 4)

        if tags:
            tag_conditions = []
            for tag in tags:
                tag_conditions.append("tags LIKE ?")
                params.append(f"%{tag}%")
            if tag_conditions:
                conditions.append(f"({' OR '.join(tag_conditions)})")

        if status:
            conditions.append("status = ?")
            params.append(status)

        if start_date:
            # 确保start_date有时区信息
            if start_date.tzinfo is None:
                start_date = start_date.replace(tzinfo=self.timezone)
            conditions.append("timestamp >= ?")
            params.append(start_date.isoformat())

        if end_date:
            # 确保end_date有时区信息
            if end_date.tzinfo is None:
                end_date = end_date.replace(tzinfo=self.timezone)
            conditions.append("timestamp <= ?")
            params.append(end_date.isoformat())

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                f"""
                SELECT 
                    id,
                    func_name,
                    doc_string,
                    args,
                    kwargs,
                    return_value,
                    status,
                    error_message,
                    timestamp,
                    execution_time
                FROM function_records 
                WHERE {where_clause}
                ORDER BY timestamp DESC
                """,
                params,
            )

            results = []
            for row in cursor.fetchall():
                # 解析时间戳并添加时区信息
                timestamp = self._parse_timestamp(row["timestamp"])

                result = {
                    "id": row["id"],
                    "function": row["func_name"],
                    "documentation": row["doc_string"],
                    "last_execution": {
                        "timestamp": timestamp.isoformat(),
                        "execution_time": f"{row['execution_time']:.4f}s",
                        "arguments": {
                            "args": json.loads(row["args"]),
                            "kwargs": json.loads(row["kwargs"]),
                        },
                    },
                }

                if row["status"] == "success":
                    try:
                        result["last_execution"]["return_value"] = json.loads(
                            row["return_value"]
                        )
                    except:
                        result["last_execution"]["return_value"] = row["return_value"]
                else:
                    result["last_execution"]["error"] = row["error_message"]

                results.append(result)

            # 打印搜索结果
            if not results:
                print("未找到匹配的记录")
                return results

            print(f"\n找到 {len(results)} 条记录:")
            for i, result in enumerate(results, 1):
                print(f"\n--- 记录 {i} ---")
                print(self._format_search_result(result))

            return results

    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """解析时间戳字符串为datetime对象"""
        try:
            dt = datetime.fromisoformat(timestamp_str)
            if dt.tzinfo is None:
                # 如果时间戳没有时区信息，添加时区信息
                dt = dt.replace(tzinfo=self.timezone)
            return dt
        except ValueError as e:
            print(f"警告: 时间戳解析失败 ({timestamp_str}): {str(e)}")
            return datetime.now(self.timezone)

    def _format_record_detail(self, record: Dict) -> str:
        """格式化详细记录信息"""
        output = []

        # 基本信息
        output.append(f"记录ID: {record['id']}")

        # 函数信息
        output.append("\n=== 函数信息 ===")
        output.append(f"函数名: {record['func_name']}")
        output.append(f"模块路径: {record['module_path']}")
        if record["doc_string"]:
            output.append(f"文档说明: {record['doc_string']}")

        output.append("\n源代码:")
        source_code = record["source_code"]
        if isinstance(source_code, str):
            try:
                source_code = json.loads(source_code)
            except:
                pass
        output.append(textwrap.dedent(source_code).strip())

        # 执行信息
        output.append("\n=== 执行信息 ===")
        output.append(f"执行时间: {record['timestamp']}")
        output.append(f"耗时: {record['execution_time']:.4f}s")
        output.append(f"执行状态: {record['status']}")

        # 参数信息
        try:
            args = json.loads(record["args"])
            kwargs = json.loads(record["kwargs"])
            args_str = ", ".join(str(arg) for arg in args)
            kwargs_str = ", ".join(f"{k}={v}" for k, v in kwargs.items())
            params = []
            if args_str:
                params.append(args_str)
            if kwargs_str:
                params.append(kwargs_str)
            output.append(f"调用参数: {', '.join(params)}")
        except Exception as e:
            output.append(f"参数解析错误: {str(e)}")

        # 执行结果或错误信息
        if record["status"] == "success":
            try:
                return_value = json.loads(record["return_value"])
                output.append(f"返回值: {return_value}")
            except:
                output.append(f"返回值: {record['return_value']}")
        else:
            output.append("\n=== 错误信息 ===")
            output.append(f"错误类型: {record['error_type']}")
            output.append(f"错误消息: {record['error_message']}")
            output.append("\n错误追踪:")
            output.append(record["error_traceback"])

        # 依赖信息
        try:
            deps = json.loads(record["dependencies"])
            if isinstance(deps, dict) and deps.get("imports"):
                output.append("\n=== 依赖信息 ===")
                for imp in deps["imports"]:
                    output.append(f"- {imp}")
        except Exception as e:
            output.append(f"\n依赖解析错误: {str(e)}")

        # 标签信息
        output.append(f"\n=== 标签 ===: {record['tags']}")
        # try:
        #     tags = json.loads(record["tags"])
        #     if isinstance(tags, list) and tags:  # 确保tags是列表类型
        #         output.append("\n=== 标签 ===")
        #         output.append(", ".join(tags))  # 直接连接标签列表
        # except Exception as e:
        #     output.append(f"\n标签解析错误: {str(e)}")

        return "\n".join(output)

    def get_record_detail(self, record_id: int) -> Optional[Dict]:
        """获取执行记录的详细信息
        
        Args:
            record_id (int): 要获取详细信息的记录ID
            
        Returns:
            Optional[Dict]: 包含记录详细信息的字典,如果记录不存在则返回None。
            返回字典包含以下字段:
                - id: 记录ID
                - func_name: 函数名
                - module_path: 模块路径 
                - doc_string: 函数文档字符串
                - source_code: 函数源代码
                - timestamp: 执行时间戳
                - execution_time: 执行耗时
                - status: 执行状态
                - args: 位置参数
                - kwargs: 关键字参数
                - return_value: 返回值
                - error_type: 错误类型(如果有)
                - error_message: 错误信息(如果有)
                - error_traceback: 错误堆栈(如果有)
                - dependencies: 依赖信息
                - tags: 标签
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT * FROM function_records WHERE id = ?
                """,
                (record_id,),
            )
            record = cursor.fetchone()

            if not record:
                print(f"未找到ID为 {record_id} 的记录")
                return None

            # 构建详细信息字典
            detail = {
                "id": record["id"],
                "func_name": record["func_name"],
                "module_path": record["module_path"],
                "doc_string": record["doc_string"],
                "source_code": record["source_code"],
                "timestamp": record["timestamp"],
                "execution_time": record["execution_time"],
                "status": record["status"],
                "args": record["args"],
                "kwargs": record["kwargs"],
                "return_value": record["return_value"],
                "error_type": record["error_type"],
                "error_message": record["error_message"],
                "error_traceback": record["error_traceback"],
                "dependencies": record["dependencies"],
                "tags": record["tags"],
            }
            if detail["return_value"]:
                try:
                    detail["return_value"] = json.loads(detail["return_value"])
                except:
                    pass

            # 打印格式化输出
            print(self._format_record_detail(detail))

            # 返回构建的字典
            return detail

    def error_statistics(self) -> Dict:
        """获取错误统计信息"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT 
                    error_type,
                    COUNT(*) as count,
                    AVG(execution_time) as avg_execution_time
                FROM function_records 
                WHERE error_type IS NOT NULL 
                GROUP BY error_type
            """
            )
            return {
                row["error_type"]: {
                    "count": row["count"],
                    "avg_execution_time": row["avg_execution_time"],
                }
                for row in cursor.fetchall()
            }

    def print_function_info(self, func_name: str):
        """打印函数信息，包括源代码、文档和依赖
        
        Args:
            func_name (str): 要查询的函数名称
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT source_code, doc_string, dependencies FROM function_records WHERE func_name = ? ORDER BY timestamp DESC LIMIT 1",
                (func_name,),
            )
            record = cursor.fetchone()

            if record:
                print(f"\n=== 函数信息: {func_name} ===")
                print("\n源代码:")
                try:
                    source_code = json.loads(record["source_code"])
                    print(textwrap.dedent(source_code).strip())
                except:
                    print(record["source_code"])

                # 显示文档字符串
                if record["doc_string"]:
                    print("\n文档:")
                    print(record["doc_string"])

                try:
                    deps = json.loads(record["dependencies"])
                    if isinstance(deps, dict) and deps.get("imports"):
                        print("\n依赖:")
                        for imp in deps["imports"]:
                            print(f"- {imp}")
                except Exception as e:
                    print(f"警告: 解析依赖信息时出错: {str(e)}")
            else:
                print(f"未找到函数 {func_name} 的记录")

    def get_statistics(
        self,
        func_name: str = None,
        start_date: datetime = None,
        end_date: datetime = None,
        status: str = None,  # 'success' or 'error'
    ) -> Dict:
        """
        获取函数执行统计信息

        Args:
            func_name: 函数名称，可选
            start_date: 开始时间，可选
            end_date: 结束时间，可选
            status: 执行状态（'success' 或 'error'），可选

        Returns:
            包含统计信息的字典
        """
        conditions = []
        params = []

        if func_name:
            conditions.append("func_name = ?")
            params.append(func_name)

        if start_date:
            conditions.append("timestamp >= ?")
            params.append(start_date.isoformat())

        if end_date:
            conditions.append("timestamp <= ?")
            params.append(end_date.isoformat())

        if status:
            if status.lower() == "success":
                conditions.append("status = 'success'")
            elif status.lower() == "error":
                conditions.append("status = 'error'")

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                f"""
                SELECT 
                    func_name,
                    COUNT(*) as total_calls,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_count,
                    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as error_count,
                    AVG(execution_time) as avg_execution_time,
                    MIN(execution_time) as min_execution_time,
                    MAX(execution_time) as max_execution_time,
                    MIN(timestamp) as first_call,
                    MAX(timestamp) as last_call
                FROM function_records 
                WHERE {where_clause}
                GROUP BY func_name
                """,
                params,
            )

            stats = {}
            for row in cursor.fetchall():
                func_stats = {
                    "总调用次数": row["total_calls"],
                    "成功次数": row["success_count"],
                    "失败次数": row["error_count"],
                    "成功率": (
                        f"{(row['success_count'] / row['total_calls'] * 100):.2f}%"
                        if row["total_calls"] > 0
                        else "0%"
                    ),
                    "平均执行时间": f"{row['avg_execution_time']:.4f}s",
                    "最短执行时间": f"{row['min_execution_time']:.4f}s",
                    "最长执行时间": f"{row['max_execution_time']:.4f}s",
                    "首次调用": row["first_call"],
                    "最后调用": row["last_call"],
                }

                # 获取错误类型统计
                if row["error_count"] > 0:
                    error_cursor = conn.execute(
                        f"""
                        SELECT 
                            error_type,
                            COUNT(*) as count
                        FROM function_records 
                        WHERE func_name = ? AND status = 'error'
                        GROUP BY error_type
                        """,
                        (row["func_name"],),
                    )
                    func_stats["错误类型统计"] = {
                        row["error_type"]: row["count"]
                        for row in error_cursor.fetchall()
                    }

                stats[row["func_name"]] = func_stats

            return stats

    def print_statistics(self, stats: Dict):
        """打印统计信息"""
        if not stats:
            print("没有找到匹配的统计数据")
            return

        for func_name, func_stats in stats.items():
            print(f"\n=== 函数统计: {func_name} ===")
            for key, value in func_stats.items():
                if key != "错误类型统计":
                    print(f"{key}: {value}")

            if "错误类型统计" in func_stats:
                print("\n错误类型分布:")
                for error_type, count in func_stats["错误类型统计"].items():
                    print(f"- {error_type}: {count}次")

    def export_data(
        self, data: Any, export_type: str, output_dir: str = "exports"
    ) -> Path:
        """导出数据
        
        Args:
            data (Any): 要导出的数据,可以是详情字典、统计信息字典或记录列表
            export_type (str): 导出类型,可选值:
                - detail: 导出详情
                - statistics: 导出统计信息 
                - list: 导出记录列表
            output_dir (str, optional): 输出目录路径,默认为"exports"
            
        Returns:
            Path: 导出文件的完整路径
            
        Raises:
            ValueError: 当export_type不是支持的类型时抛出
        """
        output_path = Path(output_dir).resolve()
        output_path.mkdir(parents=True, exist_ok=True)

        # 使用当前时区的时间戳生成文件名
        timestamp = datetime.now(self.timezone).strftime("%Y%m%d_%H%M%S")
        filename = f"funckeeper_{export_type}_{timestamp}.html"
        filepath = output_path / filename

        # 创建导出器
        exporter = HtmlExporter()

        # 执行导出
        export_methods = {
            "detail": exporter.export_detail,
            "statistics": exporter.export_statistics,
            "list": exporter.export_list,
        }
        export_method = export_methods.get(export_type)
        if not export_method:
            raise ValueError(f"不支持的导出类型: {export_type}")

        export_method(data, str(filepath))
        return filepath

