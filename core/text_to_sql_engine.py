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

    def _normalize_sql_punctuation(self, text: str) -> str:
        replacements = {
            '（': '(', '）': ')',
            '‘': "'", '’': "'", '`': "'",
            '“': '"', '”': '"',
            '；': ';', '，': ',',
            '＝': '='
        }
        for full_width, half_width in replacements.items():
            text = text.replace(full_width, half_width)
        return text

    def _validate_sql(self, sql: str) -> bool:
        forbidden_keywords = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE', 'GRANT', 'REVOKE']
        if any(keyword in sql.upper() for keyword in forbidden_keywords):
            logger.warning(f"SQL 校验失败：检测到禁用的关键字。SQL: '{sql}'")
            return False
        if not sql.strip().upper().startswith('SELECT'):
            logger.warning(f"SQL 校验失败：非 SELECT 语句。SQL: '{sql}'")
            return False
        return True

    def _rewrite_query_for_sql(self, query: str, history: list) -> str:
        if not history:
            return query
        logger.info("检测到对话历史，正在为 Text-to-SQL 重写问题...")
        messages = history[-4:]
        messages.append({
            "role": "user",
            "content": f"请根据上述对话历史，将我下面这个可能依赖上下文的问题，改写成一个独立的、完整的、可以用于数据库查询的问题。请只返回改写后的问题本身，不要加任何多余的解释或前缀。\n\n我的问题是：'{query}'"
        })
        try:
            response = self.llm_client.chat.completions.create(
                model="qwen-plus",
                messages=messages,
                temperature=0.0
            )
            rewritten_q = response.choices[0].message.content
            logger.info(f"原始SQL问题: '{query}'")
            logger.info(f"重写后SQL问题: '{rewritten_q}'")
            return rewritten_q
        except Exception as e:
            logger.error(f"为 SQL 查询重写问题时出错: {e}, 将返回原始问题。")
            return query

    def generate_sql(self, user_query: str, history: list = None) -> str | None:
        if history:
            standalone_query = self._rewrite_query_for_sql(user_query, history)
        else:
            standalone_query = user_query

        # --- Prompt（后续可优化） ---
        prompt_template = f"""
# 核心规则：
1.  **角色判断**: 首先仔细判断用户的【问题】意图。
    * **意图 A (描述表结构)**: 用户明确想知道表的【结构信息】，比如表的主键、有哪些列、列的含义、注释等。
        * 例子: "REGULATORY_DOCUMENTS 表的主键是什么？", "金融产品表有哪些字段？", "描述一下监管文件表。"
    * **意图 B (查询表内容)**: 用户想知道表里面【有什么数据】。这是最常见的意图。如果问题涉及到一个或多个数据库中的具体实体（如产品名、机构名），则必须归为意图B。
        * 例子: "有哪些监管文件？", "列出所有高风险的金融产品。", "中国人民银行发布了哪些文件？", "结构性存款是什么风险等级？"

2.  **执行逻辑**:
    * 如果判断为 **意图 A**，你**必须直接根据下面提供的【数据库表结构】来回答问题**，绝对不要生成 SQL。
    * 如果判断为 **意图 B**，你**必须将问题转换成一条精确的 Oracle SQL 查询语句**来查询数据。

3.  **SQL 生成要求 (仅当执行意图 B 时)**:
    * 格式要求（极其重要）: 所有标点符号都必须使用半角（ASCII/英文）格式。
    * 安全要求: 只能生成只读的 `SELECT` 查询。
    * 输出要求: 绝对只返回 SQL 查询语句本身，并以分号 `;` 结尾

# 数据库表结构 (DDL Schema):
---
{self.db_schema}
---

# 用户问题:
{standalone_query}

# 你的回答 (根据意图判断，直接回答或生成 SQL):
"""
        logger.info("正在使用最终版 Prompt 调用 LLM...")

        try:
            response = self.llm_client.chat.completions.create(
                model="qwen-plus",
                messages=[{"role": "user", "content": prompt_template}],
                temperature=0.0,
            )
            raw_response = response.choices[0].message.content.strip()
            clean_response = self._normalize_sql_punctuation(raw_response)

            is_sql = "SELECT" in clean_response.upper() and clean_response.endswith(';')

            if is_sql:
                if self._validate_sql(clean_response):
                    logger.info(f"LLM 成功生成 SQL: {clean_response}")
                    return clean_response
                else:
                    logger.warning(f"LLM 生成的 SQL 未通过校验: '{clean_response}'")
                    return None
            else:
                logger.info(f"LLM 直接回答了 Schema 问题: {clean_response}")
                return clean_response

        except Exception as e:
            logger.error(f"调用 LLM 时出错: {e}")
            return None


# --- 测试入口 ---
# if __name__ == '__main__':
#     from core.schema_fetcher import OracleSchemaFetcher
#
#     BaseSchemaFetcher.clear_cache()
#
#     try:
#         oracle_fetcher = OracleSchemaFetcher()
#
#         text_to_sql_engine = TextToSQLEngine(
#             schema_fetcher=oracle_fetcher,
#             llm_client=openai_client
#         )
#
#         test_query = "有哪些监管文件表？"
#         print(f"\n--- 正在测试 TextToSQLEngine (Oracle & DDL) ---")
#         print(f"用户问题: {test_query}\n")
#
#         generated_sql = text_to_sql_engine.generate_sql(test_query)
#
#         if generated_sql:
#             print("翻译成功！生成的 Oracle SQL 语句是：")
#             print(generated_sql)
#         else:
#             print("翻译失败。")
#
#     except ValueError as e:
#         print(f"\n错误：{e}")
#         print("请确保你的 .env 文件中已正确配置 Oracle 连接信息。")
#     except Exception as e:
#         print(f"发生了一个意外错误: {e}")