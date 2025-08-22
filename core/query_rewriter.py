# core/query_rewriter.py
import config

logger = config.logger

def rewrite_query_with_history(query: str, history: list, llm_client) -> str:
    """
    根据对话历史，将一个可能依赖上下文的问题，改写成一个独立的、完整的查询问题。
    """
    if not history:
        return query

    logger.info("重写问题中...")
    messages = history[-4:]
    messages.append({
        "role": "user",
        "content": f"请根据上述对话历史，将我下面这个可能依赖上下文的问题，改写成一个独立的、完整的、对后续处理（搜索引擎或数据库查询）友好的问题。请只返回改写后的问题本身，不要加任何多余的解释或前缀。\n\n我的问题是：'{query}'"
    })

    try:
        response = llm_client.chat.completions.create(
            model=config.LLM_MODEL_NAME,
            messages=messages,
            temperature=0.0
        )
        rewritten_q = response.choices[0].message.content.strip() # 增加strip()去除可能的首尾空格
        logger.info(f"原始问题: '{query}' -> 重写后问题: '{rewritten_q}'")
        return rewritten_q
    except Exception as e:
        logger.error(f"查询重写时出错: {e}, 返回原始问题。")
        return query