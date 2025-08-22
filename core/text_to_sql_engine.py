#core/text_to_sql_engine.py

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

    def _validate_sql(self, sql: str) -> bool:
        """
        一个简单的 SQL 校验器，用于保证安全性和基础的合法性。
        """
        # 1. 安全性检查：禁止任何可能修改数据的操作
        forbidden_keywords = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE', 'GRANT', 'REVOKE']
        if any(keyword in sql.upper() for keyword in forbidden_keywords):
            logger.warning(f"SQL 校验失败：检测到禁用的关键字。SQL: '{sql}'")
            return False

        # 2. 合法性检查：必须是 SELECT 语句
        if not sql.strip().upper().startswith('SELECT'):
            logger.warning(f"SQL 校验失败：非 SELECT 语句。SQL: '{sql}'")
            return False

        return True

    def generate_sql(self, user_query: str) -> str | None:
        prompt_template = f"""
你是一个专业的 Oracle 数据库专家。

# 核心规则：
1.  **内容要求**: 根据下面提供的数据库 DDL 表结构，将用户的自然语言问题转换成一个精确、可执行的 Oracle SQL 查询语句。
2.  **安全要求**: 只能生成只读的 `SELECT` 查询。
3.  **输出要求**:
    * 绝对只返回 SQL 查询语句本身。
    * 不要添加任何解释、注释或 ```sql ... ``` 标记。
    * 生成的 SQL 必须以分号 `;` 结尾。

# 数据库表结构 (DDL Schema):
---
{self.db_schema}
---

# 用户问题:
{user_query}

# Oracle SQL 查询语句:
"""
        logger.info("正在使用新的 DDL Prompt 调用 LLM 生成 Oracle SQL...")

        try:
            response = self.llm_client.chat.completions.create(
                model="qwen-plus",
                messages=[{"role": "user", "content": prompt_template}],
                temperature=0.0,
                # --- 核心修改：移除 stop 参数 ---
            )
            generated_sql = response.choices[0].message.content.strip()

            # --- 核心修改：增加校验环节 ---
            if generated_sql and self._validate_sql(generated_sql):
                logger.info(f"LLM 成功生成并通过校验的 SQL: {generated_sql}")
                return generated_sql
            else:
                logger.warning(f"LLM 生成的 SQL 未通过校验或为空: '{generated_sql}'")
                return None

        except Exception as e:
            logger.error(f"调用 LLM 生成 SQL 时出错: {e}")
            return None


# --- 测试入口 ---
if __name__ == '__main__':
    from core.schema_fetcher import OracleSchemaFetcher

    BaseSchemaFetcher.clear_cache()

    try:
        oracle_fetcher = OracleSchemaFetcher()

        text_to_sql_engine = TextToSQLEngine(
            schema_fetcher=oracle_fetcher,
            llm_client=openai_client
        )

        test_query = "有哪些监管文件表？"
        print(f"\n--- 正在测试 TextToSQLEngine (Oracle & DDL) ---")
        print(f"用户问题: {test_query}\n")

        generated_sql = text_to_sql_engine.generate_sql(test_query)

        if generated_sql:
            print("翻译成功！生成的 Oracle SQL 语句是：")
            print(generated_sql)
        else:
            print("翻译失败。")

    except ValueError as e:
        print(f"\n错误：{e}")
        print("请确保你的 .env 文件中已正确配置 Oracle 连接信息。")
    except Exception as e:
        print(f"发生了一个意外错误: {e}")