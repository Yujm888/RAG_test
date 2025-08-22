#app.py

from flask import Flask, request, render_template, redirect, url_for, jsonify
import json
import re
from markdown_it import MarkdownIt

import config
from core.search_engine import SearchEngine
from engine import RAGPipeline
from core.schema_fetcher import OracleSchemaFetcher
from core.text_to_sql_engine import TextToSQLEngine
from hybrid_engine import HybridEngine  # <--- 1. 导入新引擎
from core.embedding_utils import client as openai_client

logger = config.logger
md = MarkdownIt().enable('table')
MAX_HISTORY_TURNS = 5


def clean_history_for_llm(history: list) -> list:
    cleaned_history = []
    for message in history:
        if message.get("role") == "assistant":
            # 移除参考文档和数据库查询信息的HTML
            content = message["content"]
            content = re.sub(r'<p><strong>参考文档:</strong></p><ul>.*?</ul>', '', content, flags=re.DOTALL)
            content = re.sub(r'<p><strong>数据库查询:</strong></p><pre><code>.*?</code></pre>', '', content, flags=re.DOTALL)
            cleaned_history.append({"role": "assistant", "content": content.strip()})
        else:
            cleaned_history.append(message)
    return cleaned_history

app = Flask(__name__)


try:
    # 1. 创建 RAG 流程所需的搜索引擎
    search_engine = SearchEngine(
        index_path=config.INDEX_FILE_PATH,
        chunks_path=config.CHUNKS_FILE_PATH
    )
    # 2. 创建 RAG 流程实例
    rag_pipeline = RAGPipeline(search_engine=search_engine, llm_client=openai_client)

    # 3. 创建 Text-to-SQL 引擎
    oracle_fetcher = OracleSchemaFetcher()
    text_to_sql_engine = TextToSQLEngine(schema_fetcher=oracle_fetcher, llm_client=openai_client)

    # 4. 创建混合引擎实例，并将其他引擎注入进去
    hybrid_engine = HybridEngine(
        rag_pipeline=rag_pipeline,
        text_to_sql_engine=text_to_sql_engine,
        llm_client=openai_client
    )
    logger.info("全局 HybridEngine 及所有子服务实例创建成功。")

except Exception as e:
    logger.error(f"创建全局实例失败: {e}", exc_info=True)
    hybrid_engine = None


@app.route('/')
def home():
    """渲染主页，并初始化对话。"""
    # 历史记录只包含系统级Prompt (可以考虑换成混合引擎的)
    history = [rag_pipeline.SYSTEM_PROMPT] if rag_pipeline else []
    logger.info("新会话开始，渲染主页。")
    return render_template('index.html', history=history)


@app.route('/ask', methods=['POST'])
def ask():
    """处理用户提问。"""
    if hybrid_engine is None: # <--- 3. 检查混合引擎
        logger.error("HybridEngine 未初始化，无法处理请求。")
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

    # --- 核心修改：调用混合引擎 --- <--- 4. 调用新引擎
    result = hybrid_engine.execute(user_query, history_without_system)

    pure_answer = result["answer"]
    sources_data = result["sources"]

    # 将 Markdown 答案（可能包含表格）渲染成 HTML
    pure_answer_html = md.render(pure_answer)

    # --- 更新来源信息展示逻辑 ---
    sources_html = ""
    if sources_data and isinstance(sources_data, list) and len(sources_data) > 0:
        # 检查第一个来源项的类型来决定如何格式化
        source_item = sources_data[0]

        if isinstance(source_item, dict):
            source_type = source_item.get("type")
            if source_type == "database":
                sql_query = source_item.get("query", "无SQL信息")
                sources_html = f"<p><strong>数据库查询:</strong></p><pre><code>{sql_query}</code></pre>"
            elif source_type == "schema_info":
                sources_html = ""

        elif isinstance(source_item, list):
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


@app.route('/api/rag_query', methods=['POST'])
def api_rag_query():
    """
    API 接口，现在也由混合引擎驱动。
    """
    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400

    data = request.get_json()
    user_query = data.get('query')
    history = data.get('history', [])

    if not user_query:
        return jsonify({"error": "Missing 'query' field in request"}), 400

    if hybrid_engine is None: # <--- 5. 检查混合引擎
        return jsonify({"error": "Hybrid engine is not available"}), 500

    logger.info(f"收到 API 请求: query='{user_query}'")

    result = hybrid_engine.execute(user_query, history) # <--- 6. 调用新引擎

    return jsonify({
        "answer": result.get("answer"),
        "sources": result.get("sources")
    })


if __name__ == '__main__':
    if hybrid_engine is None: # <--- 7. 检查混合引擎
        logger.error("错误：核心混合引擎未能成功加载，请检查启动日志。Flask 应用无法启动。")
    else:
        logger.info("Flask 应用准备启动...")
        print("=" * 80)
        print(">>> 应用已启动！请在浏览器中访问以下地址 <<<")
        print(f">>> http://127.0.0.1:5000")
        print("=" * 80)
        app.run(debug=False, host='0.0.0.0', port=5000)