import os
import sqlite3


PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
TEXT_TO_SQL_DIR = os.path.join(PROJECT_ROOT, "text_to_sql")
CACHE_FILE_PATH = os.path.join(TEXT_TO_SQL_DIR, "schema_cache.json")
DB_PATH_FOR_FETCHER = os.path.join(TEXT_TO_SQL_DIR, "finance_reg.db")


class BaseSchemaFetcher:
    # (BaseSchemaFetcher 类的代码保持不变)
    def get_schema_with_comments(self) -> str:
        if os.path.exists(CACHE_FILE_PATH):
            print("【缓存】: 发现本地 Schema 缓存文件，正在从文件加载...")
            with open(CACHE_FILE_PATH, 'r', encoding='utf-8') as f:
                return f.read()
        print("【数据库】: 未发现缓存，正在从数据库提取 Schema...")
        schema_str = self._fetch_from_db()
        try:
            with open(CACHE_FILE_PATH, 'w', encoding='utf-8') as f:
                f.write(schema_str)
            print(f"【缓存】: 已将最新的 Schema 写入本地缓存文件。")
        except Exception as e:
            print(f"【警告】: 写入 Schema 缓存文件失败: {e}")
        return schema_str

    def _fetch_from_db(self) -> str:
        raise NotImplementedError("这个方法必须在子类中被实现！")

    @staticmethod
    def clear_cache():
        if os.path.exists(CACHE_FILE_PATH):
            os.remove(CACHE_FILE_PATH)
            print("【缓存】: 本地 Schema 缓存文件已被清除。")
        else:
            print("【缓存】: 无需清除，本地缓存文件不存在。")


class SQLiteSchemaFetcher(BaseSchemaFetcher):
    def __init__(self, db_path=DB_PATH_FOR_FETCHER):
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
            # --- 核心修改：将表名转为大写 ---
            schema_info += f"表名: {table_name.upper()} ({table_comment})\n"

            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()

            col_descriptions = []
            for col in columns:
                col_name = col['name']
                col_comment = comments.get(f"{table_name}.{col_name}", "")
                # --- 核心修改：将列名转为大写 ---
                col_descriptions.append(f"{col_name.upper()} ({col_comment})")

            schema_info += f"列: [ {', '.join(col_descriptions)} ]\n\n"
        conn.close()
        return schema_info


# ... (OracleSchemaFetcher 和 if __name__ == '__main__' 保持不变)
class OracleSchemaFetcher(BaseSchemaFetcher):
    def __init__(self, connection_details):
        self.connection_details = connection_details
        print(f"【模式】: 已启用 Oracle Schema 提取器 (用于生产环境)。")

    def _fetch_from_db(self) -> str:
        print("警告：OracleSchemaFetcher 尚未实现！")
        return "这是一个来自 Oracle 数据库的、带注释的 Schema (待实现)"


if __name__ == '__main__':
    print("--- 缓存功能测试 (强制大写) ---")
    BaseSchemaFetcher.clear_cache()
    fetcher = SQLiteSchemaFetcher()
    schema = fetcher.get_schema_with_comments()
    print("\n--- 提取成功 (全大写) ---")
    print(schema)