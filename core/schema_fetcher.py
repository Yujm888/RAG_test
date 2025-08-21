import os
import sqlite3
import time

# --- 我们把缓存文件路径定义在这里，方便管理 ---
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
CACHE_FILE_PATH = os.path.join(PROJECT_ROOT, "schema_cache.json")


class BaseSchemaFetcher:
    """
    这是一个“提取器”的基类或蓝图。
    它优雅地封装了所有缓存逻辑。
    """

    def get_schema_with_comments(self) -> str:
        # 1. 优先检查本地文件缓存是否存在
        if os.path.exists(CACHE_FILE_PATH):
            print("【缓存】: 发现本地 Schema 缓存文件，正在从文件加载...")
            with open(CACHE_FILE_PATH, 'r', encoding='utf-8') as f:
                return f.read()

        # 2. 如果缓存不存在，才执行真正的数据库查询
        print("【数据库】: 未发现缓存，正在从数据库提取 Schema (此过程在第一次启动或刷新后执行)...")
        start_time = time.time()

        # 调用子类中具体的数据库查询方法
        schema_str = self._fetch_from_db()

        end_time = time.time()
        print(f"【数据库】: Schema 提取完毕，耗时 {end_time - start_time:.2f} 秒。")

        # 3. 将从数据库获取的结果，写入本地缓存文件，供下次使用
        try:
            with open(CACHE_FILE_PATH, 'w', encoding='utf-8') as f:
                f.write(schema_str)
            print(f"【缓存】: 已将最新的 Schema 写入本地缓存文件 '{CACHE_FILE_PATH}'。")
        except Exception as e:
            print(f"【警告】: 写入 Schema 缓存文件失败: {e}")

        return schema_str

    def _fetch_from_db(self) -> str:
        """这是一个必须被子类实现的“抽象”方法，负责真正地去连接数据库并查询。"""
        raise NotImplementedError("这个方法必须在子类中被实现！")

    @staticmethod
    def clear_cache():
        """提供一个静态方法来手动清除缓存，方便我们从其他地方调用。"""
        if os.path.exists(CACHE_FILE_PATH):
            os.remove(CACHE_FILE_PATH)
            print("【缓存】: 本地 Schema 缓存文件已被清除。")
        else:
            print("【缓存】: 无需清除，本地缓存文件不存在。")


class SQLiteSchemaFetcher(BaseSchemaFetcher):
    """这是为本地 SQLite 测试专门打造的提取器。"""

    def __init__(self, db_path):
        self.db_path = db_path
        print(f"【模式】: 已启用 SQLite Schema 提取器 (用于本地测试)。")

    def _fetch_from_db(self) -> str:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        comments = {}
        cursor.execute("SELECT table_name, column_name, comment FROM schema_comments")
        for row in cursor.fetchall():
            key = row['table_name'] if row[
                                           'column_name'] == 'table_comment' else f"{row['table_name']}.{row['column_name']}"
            comments[key] = row['comment']
        schema_info = ""
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'schema_comments';")
        tables = cursor.fetchall()
        for table_row in tables:
            table_name = table_row['name']
            table_comment = comments.get(table_name, "")
            schema_info += f"表名: {table_name} ({table_comment})\n"
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            col_descriptions = [f"{col['name']} ({comments.get(f'{table_name}.{col["name"]}', '')})" for col in columns]
            schema_info += f"列: [ {', '.join(col_descriptions)} ]\n\n"
        conn.close()
        return schema_info


class OracleSchemaFetcher(BaseSchemaFetcher):
    """这是为未来连接公司 Oracle 预留的提取器。"""

    def __init__(self, connection_details):
        self.connection_details = connection_details
        print(f"【模式】: 已启用 Oracle Schema 提取器 (用于生产环境)。")

    def _fetch_from_db(self) -> str:
        print("警告：OracleSchemaFetcher 尚未实现！")
        return "这是一个来自 Oracle 数据库的、带注释的 Schema (待实现)"


# --- 这是一个临时的测试入口，确保我们的新架构能正常工作 ---
if __name__ == '__main__':
    print("--- 缓存功能测试 ---")

    # 1. 手动清除一下之前的缓存，确保我们能看到第一次的完整流程
    BaseSchemaFetcher.clear_cache()

    # 2. 创建一个 SQLite 提取器实例
    fetcher = SQLiteSchemaFetcher(db_path=os.path.join(PROJECT_ROOT, "finance_reg.db"))

    # 3. 第一次运行，应该会从数据库读取
    print("\n--- 第一次运行 ---")
    schema1 = fetcher.get_schema_with_comments()

    # 4. 第二次运行，应该会从缓存文件读取
    print("\n--- 第二次运行 ---")
    schema2 = fetcher.get_schema_with_comments()

    print("\n--- 验证结果 ---")
    print("第一次和第二次获取的 Schema 是否一致:", schema1 == schema2)
    print("\n提取到的 Schema 内容：")
    print(schema1)