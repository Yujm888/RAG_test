#engine.py
import config
from core.search_engine import SearchEngine
from core.query_rewriter import rewrite_query_with_history # <--- 1. 导入新函数
import re

logger = config.logger


class RAGPipeline:
    def __init__(self, search_engine: SearchEngine, llm_client):
        if not isinstance(search_engine, SearchEngine) or search_engine.faiss_index is None:
            raise ValueError("必须提供一个正确初始化的 SearchEngine 实例。")

        self.search_engine = search_engine
        self.llm_client = llm_client
        self.MAX_CONTEXT_CHARS = config.MAX_CONTEXT_CHARS

        # 稳定的 RAG 系统指令
        self.SYSTEM_PROMPT = {
            "role": "system",
            "content": """
            你是一个专业的金融监管信息整合专家。你的任务是严格、仅根据下面提供的【原始上下文】内容，来回答用户的【问题】。

            # 核心规则
            1.  **绝对忠诚于上下文**: 你的回答必须完全基于【原始上下文】提供的信息。禁止进行任何形式的推理、联想或使用外部知识。
            2.  **【重要】信息来源层级**:
                * 【原始上下文】是回答问题的唯一合法来源。
                * 【对话历史】仅用于理解用户问题的指代关系，绝不能作为回答问题的信息来源。
            3.  **信息不足则明确告知**: 如果【原始上下文】中的信息不足以回答【问题】，你必须明确回答：“根据我所掌握的文档资料，无法回答您的问题。”
            4.  **极致纯净的答案**: 你的回答必须只直接回答用户的问题核心。绝对禁止输出“参考文档”等类似信息，这些将由程序在外部自动添加。
            """
        }


    def _prepare_context(self, retrieved_chunks: list) -> (str, list):
        if not retrieved_chunks:
            return "没有提供任何上下文。", []
        context_with_sources = []
        sources_list = []
        seen_sources = set()
        current_context_len = 0
        for chunk_data in retrieved_chunks:
            chunk_text = chunk_data['text']
            if current_context_len + len(chunk_text) > self.MAX_CONTEXT_CHARS:
                logger.warning(f"上下文达到最大长度 {self.MAX_CONTEXT_CHARS}，已截断。")
                break
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
        rewritten_query = rewrite_query_with_history(query, history, self.llm_client)
        retrieved_chunks = self.search_engine.search(rewritten_query, k=config.SEARCH_TOP_K)
        context_str, sources = self._prepare_context(retrieved_chunks)
        task_prompt = f"# 原始上下文\n---\n{context_str}\n---\n\n# 问题\n{query}"
        messages_for_api = [self.SYSTEM_PROMPT] + history + [{"role": "user", "content": task_prompt}]
        try:
            logger.info("正在调用 LLM 生成最终答案...")
            response = self.llm_client.chat.completions.create(
                model=config.LLM_MODEL_NAME, messages=messages_for_api, temperature=0.0
            )
            pure_answer = response.choices[0].message.content
            logger.info(f"LLM 原始返回答案: {pure_answer[:100]}...")

            source_pattern = r'\n?(以上信息来源于文件|来源|资料来源)[：:].*'
            cleaned_answer = re.sub(source_pattern, '', pure_answer, flags=re.DOTALL).strip()

            logger.info(f"清洗后答案: {cleaned_answer[:100]}...")
            return {"answer": cleaned_answer, "sources": sources}
        except Exception as e:
            logger.error(f"调用聊天 API 时出错: {e}", exc_info=True)
            return {"answer": "抱歉，生成答案时遇到了内部问题。", "sources": []}