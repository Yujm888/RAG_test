#hybrid_engine.py

import json
import oracledb
import pandas as pd
from sqlalchemy import create_engine
import config
from engine import RAGPipeline
from core.text_to_sql_engine import TextToSQLEngine

logger = config.logger


class HybridEngine:
    def __init__(self, rag_pipeline: RAGPipeline, text_to_sql_engine: TextToSQLEngine, llm_client):
        self.rag_pipeline = rag_pipeline
        self.text_to_sql_engine = text_to_sql_engine
        self.llm_client = llm_client

    def _get_db_connection(self):
        try:
            conn = oracledb.connect(
                user=config.ORACLE_USER,
                password=config.ORACLE_PASSWORD,
                dsn=config.ORACLE_DSN
            )
            logger.info("成功建立到 Oracle 数据库的连接。")
            return conn
        except oracledb.DatabaseError as e:
            logger.error(f"连接 Oracle 数据库失败: {e}")
            return None

    def _execute_sql_and_format(self, sql: str) -> str:
        """执行 SQL 查询并将其结果格式化为 Markdown 表格 (使用 SQLAlchemy 的精确连接)。"""
        if not sql:
            return "未能生成有效的 SQL 查询。"

        try:
            if config.ORACLE_SERVICE_NAME:
                dsn = oracledb.makedsn(config.ORACLE_HOST, config.ORACLE_PORT, service_name=config.ORACLE_SERVICE_NAME)
            elif config.ORACLE_SID:
                dsn = oracledb.makedsn(config.ORACLE_HOST, config.ORACLE_PORT, sid=config.ORACLE_SID)
            else:
                raise ValueError("必须在 .env 文件中提供 ORACLE_SERVICE_NAME 或 ORACLE_SID。")

            db_uri = f"oracle+oracledb://{config.ORACLE_USER}:{config.ORACLE_PASSWORD}@{dsn}"
            engine = create_engine(db_uri)
            logger.info("成功创建 SQLAlchemy 引擎。")

        except Exception as e:
            logger.error(f"创建 SQLAlchemy 引擎或 DSN 失败: {e}")
            return "抱歉，无法连接到数据库，请检查 .env 中的配置或联系管理员。"

        try:
            sql_to_execute = sql.strip().rstrip(';')
            logger.info(f"正在使用 SQLAlchemy 执行 SQL: {sql_to_execute}")

            with engine.connect() as connection:
                df = pd.read_sql_query(sql_to_execute, connection)

            if df.empty:
                return "查询成功，但未在数据库中找到相关记录。"

            return df.to_markdown(index=False)

        except Exception as e:
            logger.error(f"使用 SQLAlchemy 执行 SQL 时出错: {e}")
            return f"执行数据库查询时遇到问题。请检查生成的 SQL 语句是否正确。\n\n**错误详情**: {e}"

    def _router(self, query: str) -> str:
        prompt = f"""
        你是一个智能任务路由器。你的任务是根据用户的【问题】，判断应该使用哪个工具来回答。
        你必须以严格的 JSON 格式返回你的决定，只包含 "tool" 和 "reason" 两个键。

        # 可用工具:
        1.  `rag_search`: 用于回答关于金融法规、政策解读、专业概念、应用场景等基于文档内容的【开放式问题】。
        2.  `text_to_sql`: 用于从数据库中【精确查询】具体的金融产品信息、监管文件列表、发布机构等。

        # 示例:
        - 问题: "人工智能在办公领域有哪些应用？" -> `rag_search`
        - 问题: "中国人民银行发布了哪些文件？" -> `text_to_sql`
        - 问题: "什么是资产管理？" -> `rag_search`
        - 问题: "查询所有高风险的金融产品" -> `text_to_sql`

        # 用户问题:
        "{query}"

        # 你的 JSON 格式决策:
        """
        try:
            response = self.llm_client.chat.completions.create(
                model="qwen-plus",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                response_format={"type": "json_object"}
            )
            decision_json = json.loads(response.choices[0].message.content)
            tool_choice = decision_json.get("tool", "rag_search")
            logger.info(
                f"智能路由决策: 为问题 '{query}' 选择工具 '{tool_choice}' (原因: {decision_json.get('reason')})")
            return tool_choice
        except Exception as e:
            logger.error(f"智能路由决策失败: {e}，将默认使用 RAG 搜索。")
            return "rag_search"

    def execute(self, query: str, history: list) -> dict:
        """
        执行完整的混合查询流程。
        """
        tool_to_use = self._router(query)

        if tool_to_use == "text_to_sql":
            # 传入 history 以支持多轮对话
            response_from_engine = self.text_to_sql_engine.generate_sql(query, history)

            if not response_from_engine:
                return {"answer": "抱歉，我无法处理您的问题。请尝试换一种问法。", "sources": []}

            # --- 核心修改：判断返回的是 SQL 还是答案 ---
            is_sql = "SELECT" in response_from_engine.upper() and response_from_engine.endswith(';')

            if is_sql:
                # 是 SQL，执行它
                formatted_result = self._execute_sql_and_format(response_from_engine)
                return {"answer": formatted_result, "sources": [{"type": "database", "query": response_from_engine}]}
            else:
                # 是自然语言答案，直接返回
                return {"answer": response_from_engine, "sources": [{"type": "schema_info"}]}

        else:  # 默认为 rag_search
            return self.rag_pipeline.execute(query, history)
