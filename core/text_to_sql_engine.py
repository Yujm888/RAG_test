# core/text_to_sql_engine.py
import config
from core.schema_fetcher import BaseSchemaFetcher
from core.query_rewriter import rewrite_query_with_history
import pandas as pd

logger = config.logger


class TextToSQLEngine:
    def __init__(self, schema_fetcher: BaseSchemaFetcher, llm_client, db_engine):
        self.schema_fetcher = schema_fetcher
        self.llm_client = llm_client
        self.db_engine = db_engine  # 接收数据库引擎实例
        self._db_schema_cache = None
        self.MAX_RETRIES = 2  # 定义最大重试次数

    @property
    def db_schema(self) -> str:
        if self._db_schema_cache is None:
            self._db_schema_cache = self.schema_fetcher.get_schema_with_comments()
        return self._db_schema_cache


    def _normalize_sql_punctuation(self, text: str) -> str:
        """强制将中文全角标点替换为英文半角标点。"""
        replacements = {'（': '(', '）': ')', '‘': "'", '’': "'", '`': "'", '“': '"', '”': '"', '；': ';', '，': ',',
                        '＝': '='}
        for full, half in replacements.items():
            text = text.replace(full, half)
        return text

    def _validate_sql(self, sql: str) -> bool:
        forbidden = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE', 'GRANT', 'REVOKE']
        if any(key in sql.upper() for key in forbidden) or not sql.strip().upper().startswith('SELECT'):
            logger.warning(f"SQL 校验失败: '{sql}'")
            return False
        return True

    def _generate_initial_sql(self, standalone_query: str) -> str | None:
        """只负责根据问题生成初版SQL或答案"""
        prompt_template = f"""
你是一个双重角色的 Oracle 数据库专家：一个 Schema 解答器 和一个 SQL 生成器。

# 核心规则：
1.  **角色判断**: 首先仔细判断用户的【问题】意图。
    * **意图 A (描述表结构)**: 用户明确想知道表的【结构信息】，比如表的主键、有哪些列、列的含义、注释等。
    * **意图 B (查询表内容)**: 用户想知道表里面【有什么数据】。这是最常见的意图。

2.  **执行逻辑**:
    * 如果判断为 **意图 A**，你**必须直接根据下面提供的【数据库表结构】来回答问题**，绝对不要生成 SQL。
    * 如果判断为 **意图 B**，你**必须将问题转换成一条精确的 Oracle SQL 查询语句**。

3.  **SQL 生成要求 (仅当执行意图 B 时)**:
    * **格式要求（极其重要）**: 所有标点符号都必须使用半角（ASCII/英文）格式。
    * **安全要求**: 只能生成只读的 `SELECT` 查询。
    * **输出要求**: 绝对只返回 SQL 查询语句本身，并以分号 `;` 结尾。

# 数据库表结构 (DDL Schema):
---
{self.db_schema}
---

# 用户问题:
{standalone_query}

# 你的回答 (根据意图判断，直接回答或生成 SQL):
"""
        logger.info("正在调用 LLM 生成初版 SQL...")
        try:
            response = self.llm_client.chat.completions.create(
                model=config.LLM_MODEL_NAME,
                messages=[{"role": "user", "content": prompt_template}],
                temperature=0.0
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"生成初版 SQL 时调用 LLM 出错: {e}")
            return None

    def _fix_sql_with_error(self, standalone_query: str, wrong_sql: str, error_message: str) -> str | None:
        """根据错误信息修正SQL"""
        prompt_template = f"""
你是一个 Oracle 数据库专家，你的任务是修复一条有错误的 SQL 查询。

# 背景信息:
* **原始用户问题**: "{standalone_query}"
* **我尝试执行的错误SQL**:
    ```sql
    {wrong_sql}
    ```
* **数据库返回的错误信息**: "{error_message}"

# 你的任务:
1.  仔细分析上面的【错误信息】和【错误SQL】。
2.  根据【原始用户问题】的意图，重新生成一条**正确**的 Oracle SQL 查询。
3.  **输出要求**: 绝对只返回修正后的 SQL 查询语句本身，并以分号 `;` 结尾。不要包含任何解释。

# 数据库表结构 (DDL Schema) 供你参考:
---
{self.db_schema}
---

# 你修正后的 SQL:
"""
        logger.warning(f"SQL 执行失败，正在调用 LLM 尝试修正。错误: {error_message}")
        try:
            response = self.llm_client.chat.completions.create(
                model=config.LLM_MODEL_NAME,
                messages=[{"role": "user", "content": prompt_template}],
                temperature=0.0
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"修正 SQL 时调用 LLM 出错: {e}")
            return None

    def _execute_sql(self, sql: str) -> dict:
        """执行SQL并返回结果。"""
        if not self.db_engine:
            return {"error": "数据库引擎未初始化。"}

        try:
            with self.db_engine.connect() as connection:
                df = pd.read_sql_query(sql.strip().rstrip(';'), connection)

            if not df.empty:
                answer_data = df.to_dict(orient='records')
            else:
                answer_data = "查询成功，但未找到相关记录。"

            return {"answer": answer_data, "type": "database_result"}
        except Exception as e:
            logger.error(f"执行 SQL 时出错: {e}")
            raise e

    def run_text_to_sql_flow(self, user_query: str, history: list = None) -> dict:
        """
        完整的Text-to-SQL流程，包含自我修正机制。
        """
        standalone_query = rewrite_query_with_history(user_query, history or [], self.llm_client)

        response_from_engine = self._generate_initial_sql(standalone_query)
        if not response_from_engine:
            return {"answer": "抱歉，无法处理您的问题。", "type": "error"}

        generated_sql = ""

        for attempt in range(self.MAX_RETRIES):
            # --- 在这里调用修正函数 ---
            clean_response = self._normalize_sql_punctuation(response_from_engine)

            is_sql = "SELECT" in clean_response.upper() and clean_response.endswith(';')

            if not is_sql:
                return {"answer": clean_response, "type": "natural_language_answer"}

            if not self._validate_sql(clean_response):
                return {"answer": "生成的查询包含不允许的操作。", "type": "error"}

            generated_sql = clean_response
            try:
                logger.info(f"正在尝试执行 SQL (第 {attempt + 1} 次): {generated_sql}")
                result = self._execute_sql(generated_sql)
                result['generated_sql'] = generated_sql
                return result
            except Exception as e:
                error_message = str(e)
                logger.warning(f"SQL 执行失败: {error_message}")
                if attempt < self.MAX_RETRIES - 1:
                    response_from_engine = self._fix_sql_with_error(standalone_query, generated_sql, error_message)
                    if not response_from_engine:
                        return {"error": f"执行数据库查询时遇到问题，且自动修正失败: {error_message}",
                                "generated_sql": generated_sql, "type": "database_error"}
                else:
                    return {"error": f"执行数据库查询时遇到问题，已达最大重试次数: {error_message}",
                            "generated_sql": generated_sql, "type": "database_error"}

        return {"error": "未知错误，流程意外终止。", "generated_sql": generated_sql, "type": "error"}