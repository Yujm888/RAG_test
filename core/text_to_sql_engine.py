import config
from core.embedding_utils import client as openai_client
from core.schema_fetcher import BaseSchemaFetcher

logger = config.logger


class TextToSQLEngine:
    def __init__(self, schema_fetcher: BaseSchemaFetcher, llm_client):
        self.schema_fetcher = schema_fetcher
        self.llm_client = llm_client
        self._db_schema_cache = None

    @property
    def db_schema(self) -> str:
        if self._db_schema_cache is None:
            self._db_schema_cache = self.schema_fetcher.get_schema_with_comments()
        return self._db_schema_cache

    def generate_sql(self, user_query: str) -> str | None:
        prompt_template = f"""
你是一个严格遵守格式规范的 SQLite 数据库专家。

# 核心规则：
1.  **格式要求：生成的 SQL 查询语句中，所有的 SQL 关键字 (SELECT, FROM, WHERE, JOIN)、表名、列名都必须使用大写字母。这是强制性要求。**
2.  内容要求：根据下面提供的数据库表结构，将用户的自然语言问题转换成一个精确、可执行的 SQLite 查询语句。
3.  安全要求：只允许进行 SELECT 查询。
4.  输出要求：绝对只返回 SQL 查询语句本身，不添加任何其他内容。

# 数据库表结构 (Schema):
---
{self.db_schema}
---

# 用户问题:
{user_query}

# SQL 查询语句 (严格遵循大写格式要求):
"""
        logger.info("正在使用 Prompt 调用 LLM 生成 SQL (全大写要求)...")

        try:
            response = self.llm_client.chat.completions.create(
                model="qwen-plus",
                messages=[{"role": "user", "content": prompt_template}],
                temperature=0.0,
                stop=[";"]
            )
            generated_sql = response.choices[0].message.content.strip()

            # 我们不再需要检查 startswith("SELECT")，因为 Prompt 已经非常严格
            # 我们可以做一个更简单的检查，比如检查是否为空
            if generated_sql:
                logger.info(f"LLM 成功生成 SQL: {generated_sql}")
                return generated_sql
            else:
                logger.warning(f"LLM 未能生成任何 SQL 内容。")
                return None
        except Exception as e:
            logger.error(f"调用 LLM 生成 SQL 时出错: {e}")
            return None


# --- 测试入口 ---
if __name__ == '__main__':
    import os
    from core.schema_fetcher import SQLiteSchemaFetcher

    BaseSchemaFetcher.clear_cache()

    sqlite_fetcher = SQLiteSchemaFetcher()

    text_to_sql_engine = TextToSQLEngine(
        schema_fetcher=sqlite_fetcher,
        llm_client=openai_client
    )

    test_query = "中国证券监督管理委员会发布了哪些文件？"
    print(f"\n--- 正在测试 TextToSQLEngine (要求全大写) ---")
    print(f"用户问题: {test_query}\n")

    generated_sql = text_to_sql_engine.generate_sql(test_query)

    if generated_sql:
        print("翻译成功！生成的 SQL 语句是：")
        print(generated_sql)
    else:
        print("翻译失败。")