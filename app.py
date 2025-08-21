# app.py 应用
from flask import Flask, request, render_template, redirect, url_for
import json
from engine import RAGPipeline
from core.search_engine import SearchEngine
from core.embedding_utils import client as openai_client
import config
from markdown_it import MarkdownIt
import re

logger = config.logger
md = MarkdownIt().enable('table')
MAX_HISTORY_TURNS = 5


def clean_history_for_llm(history: list) -> list:
    """清除多余的参考文档HTML。"""
    cleaned_history = []
    for message in history:
        if message.get("role") == "assistant":
            cleaned_content = re.sub(r'<p><strong>参考文档:</strong></p><ul>.*?</ul>', '', message["content"],
                                     flags=re.DOTALL).strip()
            cleaned_history.append({"role": "assistant", "content": cleaned_content})
        else:
            cleaned_history.append(message)
    return cleaned_history


app = Flask(__name__)


# --- 在应用启动时，统一创建所有核心服务实例 ---
try:
    # 1. 创建搜索引擎实例
    search_engine = SearchEngine(
        index_path=config.INDEX_FILE_PATH,
        chunks_path=config.CHUNKS_FILE_PATH
    )
    # 2. 创建 RAG 流程实例，并将搜索引擎和 LLM 客户端注入进去
    rag_pipeline = RAGPipeline(search_engine=search_engine, llm_client=openai_client)
    logger.info("全局 SearchEngine 和 RAGPipeline 实例创建成功。")
except Exception as e:
    logger.error(f"创建全局实例失败: {e}", exc_info=True)
    rag_pipeline = None


@app.route('/')
def home():
    """渲染主页，并初始化对话。"""
    # 历史记录只包含系统级Prompt
    history = [rag_pipeline.SYSTEM_PROMPT] if rag_pipeline else []
    logger.info("新会话开始，渲染主页。")
    return render_template('index.html', history=history)


@app.route('/ask', methods=['POST'])
def ask():
    """处理用户提问。"""
    if rag_pipeline is None:
        logger.error("RAGPipeline 未初始化，无法处理请求。")
        return redirect(url_for('home'))

    user_query = request.form['query']
    history_json_str = request.form['history']

    try:
        history = json.loads(history_json_str)
        history_for_llm = clean_history_for_llm(history)
        history_without_system = [msg for msg in history_for_llm if msg.get("role") != "system"]

    except (json.JSONDecodeError, TypeError):
        logger.warning("无效的历史记录格式，重定向到主页。")
        return redirect(url_for('home'))

    result = rag_pipeline.execute(user_query, history_without_system)

    pure_answer = result["answer"]
    sources_data = result["sources"]

    pure_answer_html = md.render(pure_answer)

    sources_html = ""
    if sources_data and isinstance(sources_data, list) and len(sources_data) > 0:
        sources_list_items = [f"<li>《{doc}》(章节: {chap})</li>" for doc, chap in sources_data if
                              isinstance(doc, str) and isinstance(chap, str)]
        if sources_list_items:
            sources_html = f"<p><strong>参考文档:</strong></p><ul>{''.join(sources_list_items)}</ul>"

    display_html = pure_answer_html + sources_html

    updated_history = history + [
        {"role": "user", "content": user_query},
        {"role": "assistant", "content": display_html}
    ]

    return render_template('index.html', history=updated_history)


@app.route('/clear')
def clear_session():
    logger.info("会话已清除。")
    return redirect(url_for('home'))


if __name__ == '__main__':
    # 实例检查
    # 新的检查方式，更简洁
    if rag_pipeline is None:
        logger.error("错误：知识库或 RAG 流程未能成功加载，请检查启动日志。Flask 应用无法启动。")
    else:
        logger.info("Flask 应用准备启动...")
        print("=" * 80)
        print(">>> 应用已启动！请在浏览器中访问以下地址 <<<")
        print(f">>> http://127.0.0.1:5000")
        print("=" * 80)
        app.run(debug=False, host='0.0.0.0', port=5000)