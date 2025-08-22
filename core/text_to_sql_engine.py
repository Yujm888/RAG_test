#core/text_to_sql_engine.py
import config
from core.schema_fetcher import BaseSchemaFetcher
from core.query_rewriter import rewrite_query_with_history # <--- 1. 导入新函数

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

    # <--- 2. 整个 _rewrite_query_for_sql 方法被删除

    def generate_sql(self, user_query: str, history: list = None) -> str | None:
        # <--- 3. 调用新的外部函数
        standalone_query = rewrite_query_with_history(user_query, history or [], self.llm_client)

        prompt_template = f"""
你是一个双重角色的 Oracle 数据库专家：一个 Schema 解答器 和一个 SQL 生成器。

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
        logger.info("正在使用最终版 Prompt 调用 LLM...")
        try:
            response = self.llm_client.chat.completions.create(model=config.LLM_MODEL_NAME,
                                                               messages=[{"role": "user", "content": prompt_template}],
                                                               temperature=0.0)
            raw_response = response.choices[0].message.content.strip()
            clean_response = self._normalize_sql_punctuation(raw_response)
            is_sql = "SELECT" in clean_response.upper() and clean_response.endswith(';')
            if is_sql:
                if self._validate_sql(clean_response):
                    logger.info(f"LLM 成功生成 SQL: {clean_response}")
                    return clean_response
                return None
            else:
                logger.info(f"LLM 直接回答了 Schema 问题: {clean_response}")
                return clean_response
        except Exception as e:
            logger.error(f"调用 LLM 时出错: {e}")
            return None