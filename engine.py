# engine.py 逻辑编排
import config
from core.search_engine import SearchEngine
from core.embedding_utils import client as openai_client
import re

logger = config.logger


class RAGPipeline:
    """
    一个封装了完整 RAG (检索增强生成) 流程的类。
    它负责处理从查询重写、内容检索到最终答案生成的所有步骤。
    """


    def __init__(self, search_engine: SearchEngine, llm_client):
        """
        初始化 RAG 流程。
        :param search_engine: 一个已经初始化好的 SearchEngine 实例（依赖注入）。
        :param llm_client: 用于调用大语言模型的客户端。
        """
        if not isinstance(search_engine, SearchEngine) or search_engine.faiss_index is None:
            raise ValueError("必须提供一个正确初始化的 SearchEngine 实例。")

        self.search_engine = search_engine
        self.llm_client = llm_client
        self.MAX_CONTEXT_CHARS = 8000
        # LLM提示（根据具体情况做修改）
        self.SYSTEM_PROMPT = {
            "role": "system",
            "content": """
            你是一个专业的金融监管信息整合专家。你的任务是严格根据【原始上下文】中的内容，精准、简洁地回答用户的【问题】。
            
            # 核心规则
            1.  **绝对忠诚于原文**: 你的回答必须完全基于【原始上下文】提供的信息，禁止进行任何形式的推理、联想或使用外部知识。
            2.  **极致纯净的答案**: 你的回答**必须只直接回答用户的问题核心，必须只包含问题核心**。绝对禁止输出参考文档等类似信息，这些信息将由程序在外部自动添加。
            3.  **信息不足则明确告知**: 如果【原始上下文】中的信息不足以回答【问题】，你必须明确回答：“根据现有资料，无法回答该问题。”
            4.  **保留来源**: 你需要在最终答案的末尾，附上信息来源的文件名。
            """
        }


    def _rewrite_query(self, query: str, history: list) -> str:
        """
        根据对话，重现用户问题
        """
        if not history:
            return query

        logger.info(f"重写问题中")

        messages = []
        for message in history[-4:]:
            messages.append(message)

        messages.append({"role": "user",
                         "content": f"请根据上述对话历史，将我下面这个可能依赖上下文的问题，改写成一个独立的、完整的、对搜索引擎友好的问题。请只返回改写后的问题本身，不要加任何多余的解释或前缀。\n\n我的问题是：'{query}'"})

        try:
            response = self.llm_client.chat.completions.create(
                model="qwen-plus",
                messages=messages,
                temperature=0.0
            )
            rewritten_q = response.choices[0].message.content
            logger.info(f"原始问题: '{query}'")
            logger.info(f"重写后问题: '{rewritten_q}'")
            return rewritten_q
        except Exception as e:
            logger.error(f"查询重写时出错: {e},返回原始问题")
            return query


    def _prepare_context(self, retrieved_chunks: list) -> (str, list):
        """
        一个新增的私有方法，专门负责准备并截断上下文。
        """
        if not retrieved_chunks:
            return "没有提供任何上下文。", []

        context_with_sources = []
        sources_list = []
        seen_sources = set()
        current_context_len = 0

        for chunk_data in retrieved_chunks:
            chunk_text = chunk_data['text']
            # 检查加入这个 chunk 后是否会超长
            if current_context_len + len(chunk_text) > self.MAX_CONTEXT_CHARS:
                logger.warning(f"上下文达到最大长度 {self.MAX_CONTEXT_CHARS}，已截断。")
                break  # 停止添加更多的 chunks

            context_with_sources.append(chunk_text)
            current_context_len += len(chunk_text)

            meta = chunk_data.get("metadata", {})
            doc_title = meta.get("doc_title", "未知文档")
            chapter_title = meta.get("chapter_title", "无章节")
            source_tuple = (doc_title, chapter_title)
            if source_tuple not in seen_sources:
                sources_list.append(list(source_tuple))
                seen_sources.add(source_tuple)

        context_str = "\n---\n".join(context_with_sources)
        return context_str, sources_list


    def execute(self, query: str, history: list) -> dict:
        """
        执行完整的 RAG 流程。
        """
        rewritten_query = self._rewrite_query(query, history)
        retrieved_chunks = self.search_engine.search(rewritten_query, k=5)

        context_str, sources = self._prepare_context(retrieved_chunks)

        task_prompt = f"# 原始上下文\n---\n{context_str}\n---\n\n# 问题\n{query}"
        messages_for_api = [self.SYSTEM_PROMPT] + history + [{"role": "user", "content": task_prompt}]

        try:
            logger.info("正在调用 LLM 生成最终答案...")
            response = self.llm_client.chat.completions.create(
                model="qwen-plus",
                messages=messages_for_api,
                temperature=0.0
            )
            pure_answer = response.choices[0].message.content
            logger.info(f"LLM 原始返回答案: {pure_answer[:100]}...")

            source_pattern = r'\n?(以上信息来源于文件|来源|资料来源)[：:].*'
            cleaned_answer = re.sub(source_pattern, '', pure_answer).strip()

            logger.info(f"清洗后答案: {cleaned_answer[:100]}...")

            return {"answer": cleaned_answer, "sources": sources}

        except Exception as e:
            logger.error(f"调用聊天 API 时出错: {e}", exc_info=True)
            return {"answer": "抱歉，生成答案时遇到了内部问题。", "sources": []}
