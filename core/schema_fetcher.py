# yujm888/rag_test/RAG_test-e24255c5e1374fa6b1b3218f66279298001f055a/core/schema_fetcher.py

import os
import oracledb
import config

# --- 初始化 Oracle 客户端 (Thin Mode) ---
try:
    oracledb.init_oracle_client()
except oracledb.DatabaseError:
    config.logger.info("【数据库】: oracledb 客户端已初始化。")

# --- 路径配置 ---
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
TEXT_TO_SQL_DIR = os.path.join(PROJECT_ROOT, "text_to_sql")
CACHE_FILE_PATH = os.path.join(TEXT_TO_SQL_DIR, "schema_cache.json")


class BaseSchemaFetcher:
    """基础 Schema 提取器，包含缓存逻辑。"""

    def get_schema_with_comments(self) -> str:
        if os.path.exists(CACHE_FILE_PATH):
            config.logger.info("【缓存】: 发现本地 Schema 缓存文件，正在从文件加载...")
            with open(CACHE_FILE_PATH, 'r', encoding='utf-8') as f:
                return f.read()

        config.logger.info("【数据库】: 未发现缓存，正在从数据库提取 Schema...")
        schema_str = self._fetch_from_db()

        try:
            with open(CACHE_FILE_PATH, 'w', encoding='utf-8') as f:
                f.write(schema_str)
            config.logger.info(f"【缓存】: 已将最新的 Schema 写入本地缓存文件。")
        except Exception as e:
            config.logger.warning(f"【警告】: 写入 Schema 缓存文件失败: {e}")

        return schema_str

    def _fetch_from_db(self) -> str:
        raise NotImplementedError("这个方法必须在子类中被实现！")

    @staticmethod
    def clear_cache():
        if os.path.exists(CACHE_FILE_PATH):
            os.remove(CACHE_FILE_PATH)
            config.logger.info("【缓存】: 本地 Schema 缓存文件已被清除。")
        else:
            config.logger.info("【缓存】: 无需清除，本地缓存文件不存在。")


class OracleSchemaFetcher(BaseSchemaFetcher):
    """
    从 Oracle 数据库中提取 Schema，并格式化为带注释的 DDL 字符串。
    """

    def __init__(self):
        self.user = config.ORACLE_USER
        self.password = config.ORACLE_PASSWORD
        self.dsn = config.ORACLE_DSN
        if not all([self.user, self.password, self.dsn]):
            raise ValueError("Oracle 连接配置不完整，请检查 .env 文件。")
        config.logger.info(f"【模式】: 已启用 Oracle Schema 提取器 (用户: {self.user})。")

    def _fetch_from_db(self) -> str:
        query = """
                SELECT cols.table_name, \
                       tabs.comments AS table_comment, \
                       cols.column_name, \
                       cols.data_type || \
                       CASE \
                           WHEN cols.data_type LIKE '%CHAR%' THEN '(' || cols.data_length || ')' \
                           WHEN cols.data_type = 'NUMBER' AND cols.data_precision IS NOT NULL \
                               THEN '(' || cols.data_precision || ',' || cols.data_scale || ')' \
                           ELSE '' \
                           END       AS column_type, \
                       coms.comments AS column_comment
                FROM all_tab_columns cols
                         JOIN all_tables tab ON cols.owner = tab.owner AND cols.table_name = tab.table_name
                         LEFT JOIN all_col_comments coms \
                                   ON cols.owner = coms.owner AND cols.table_name = coms.table_name AND \
                                      cols.column_name = coms.column_name
                         LEFT JOIN all_tab_comments tabs \
                                   ON cols.owner = tabs.owner AND cols.table_name = tabs.table_name
                WHERE cols.owner = :owner
                  AND REGEXP_LIKE(cols.table_name, '^(REGULATORY_DOCUMENTS|FINANCIAL_PRODUCTS)$')
                ORDER BY cols.table_name, cols.column_id \
                """

        schema_parts = []
        try:
            with oracledb.connect(user=self.user, password=self.password, dsn=self.dsn) as connection:
                cursor = connection.cursor()
                cursor.execute(query, owner=self.user.upper())

                tables_data = {}
                for row in cursor:
                    table_name, table_comment, col_name, col_type, col_comment = row
                    if table_name not in tables_data:
                        tables_data[table_name] = {
                            "comment": table_comment,
                            "columns": []
                        }
                    tables_data[table_name]["columns"].append({
                        "name": col_name,
                        "type": col_type,
                        "comment": col_comment
                    })

                for table_name, data in tables_data.items():
                    ddl = f"CREATE TABLE {table_name} (\n"
                    col_defs = []
                    for col in data["columns"]:
                        col_def = f"    {col['name']} {col['type']}"
                        if col['comment']:
                            col_def += f", -- {col['comment']}"
                        col_defs.append(col_def)
                    ddl += ",\n".join(col_defs)
                    ddl += "\n);"
                    if data['comment']:
                        ddl += f" -- {data['comment']}"
                    schema_parts.append(ddl)

        except oracledb.DatabaseError as e:
            error, = e.args
            config.logger.error(f"Oracle 数据库错误: {error.code} - {error.message}")
            return f"-- 错误：无法从 Oracle 数据库提取 Schema: {error.message}"
        except Exception as e:
            config.logger.error(f"提取 Oracle Schema 时发生未知错误: {e}")
            return f"-- 错误：提取 Oracle Schema 时发生未知错误: {e}"

        return "\n\n".join(schema_parts)


if __name__ == '__main__':
    print("--- 缓存功能测试 (Oracle & DDL 格式) ---")
    BaseSchemaFetcher.clear_cache()
    try:
        fetcher = OracleSchemaFetcher()
        schema = fetcher.get_schema_with_comments()
        print("\n--- 从 Oracle 提取 Schema 成功 ---")
        print(schema)
    except ValueError as e:
        print(f"\n错误：{e}")
        print("请确保你的 .env 文件中已正确配置 Oracle 连接信息。")