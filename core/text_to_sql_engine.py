import config
from core.embedding_utils import client as openai_client
from core.schema_fetcher import BaseSchemaFetcher

logger = config.logger


class TextToSQLEngine:
    """
    Text-to-SQL 功能的“大脑中枢”。
    它负责指挥整个从自然语言到最终答案的流程。
    """

    def __init__(self, schema_fetcher: BaseSchemaFetcher, llm_client):
        """
        初始化引擎。
        :param schema_fetcher: 一个“Schema 提取器”的实例。
        :param llm_client: 用于调用大语言模型的客户端。
        """
        self.schema_fetcher = schema_fetcher
        self.llm_client = llm_client
        # 我们把 Schema 缓存起来，避免每次调用 generate_sql 都重复提取
        self._db_schema_cache = None

    @property
    def db_schema(self) -> str:
        """这是一个巧妙的属性，它会使用缓存的 Schema，如果不存在则自动提取一次。"""
        if self._db_schema_cache is None:
            # 调用我们传入的“侦探”来获取情报
            self._db_schema_cache = self.schema_fetcher.get_schema_with_comments()
        return self._db_schema_cache

    def generate_sql(self, user_query: str) -> str | None:
        """
        这是核心的“翻译”功能。
        """
        prompt_template = f"""
你是一个专业的 SQLite 数据库专家，特别是在金融监管领域。你的任务是根据下面提供的数据库表结构，将用户的自然语言问题转换成一个精确、可执行的 SQLite 查询语句。

# 核心规则：
1.  绝对只返回 SQL 查询语句本身，不添加任何解释、注释或 "SQL:" 前缀。
2.  生成的 SQL 必须是安全的，只允许进行 SELECT 查询，严禁任何形式的修改或删除操作。
3.  仔细理解表名和列名的含义，特别是括号里的中文注释，确保查询逻辑正确。

# 数据库表结构 (Schema):
---
{self.db_schema}
---

# 用户问题:
{user_query}

# SQL 查询语句:
"""
        logger.info("正在使用 Prompt 调用 LLM 生成 SQL...")

        try:
            response = self.llm_client.chat.completions.create(
                model="qwen-plus",
                messages=[{"role": "user", "content": prompt_template}],
                temperature=0.0,
                stop=[";"]
            )
            generated_sql = response.choices[0].message.content.strip()

            if generated_sql.upper().strip().startswith("SELECT"):
                logger.info(f"LLM 成功生成 SQL: {generated_sql}")
                return generated_sql
            else:
                logger.warning(f"安全警告：LLM 尝试生成非 SELECT 语句，已拒绝: {generated_sql}")
                return None
        except Exception as e:
            logger.error(f"调用 LLM 生成 SQL 时出错: {e}")
            return None


# --- 这是一个临时的测试入口，确保我们的“大脑”能指挥“侦探”一起工作 ---
if __name__ == '__main__':
    import os
    from core.schema_fetcher import SQLiteSchemaFetcher

    # 1. 创建一个“SQLite 侦探”实例
    PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
    DB_PATH = os.path.join(PROJECT_ROOT, "finance_reg.db")
    sqlite_fetcher = SQLiteSchemaFetcher(db_path=DB_PATH)

    # 2. 创建“大脑中枢”实例，并把我们聘请的“侦探”派给他
    text_to_sql_engine = TextToSQLEngine(
        schema_fetcher=sqlite_fetcher,
        llm_client=openai_client
    )

    # 3. 让“大脑”开始工作
    test_query = "中国证券监督管理委员会发布了哪些文件？"
    print(f"\n--- 正在测试 TextToSQLEngine ---")
    print(f"用户问题: {test_query}\n")

    generated_sql = text_to_sql_engine.generate_sql(test_query)

    if generated_sql:
        print("翻译成功！生成的 SQL 语句是：")
        print(generated_sql)
    else:
        print("翻译失败。")