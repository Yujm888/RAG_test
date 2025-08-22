# app.py
from flask import Flask, request, render_template, session, redirect, url_for, jsonify
from markdown_it import MarkdownIt
from sqlalchemy import create_engine
import pandas as pd
import oracledb
import config
from core.search_engine import SearchEngine
from engine import RAGPipeline
from core.schema_fetcher import OracleSchemaFetcher
from core.text_to_sql_engine import TextToSQLEngine
from core.embedding_utils import client as openai_client

# --- 初始化 ---
logger = config.logger
md = MarkdownIt().enable('table')
app = Flask(__name__)
app.secret_key = 'a_very_secret_key_for_session'

# --- 全局服务实例 ---
rag_pipeline = None
text_to_sql_engine = None
db_engine = None

try:
    # RAG
    search_engine = SearchEngine(config.INDEX_FILE_PATH, config.CHUNKS_FILE_PATH)
    rag_pipeline = RAGPipeline(search_engine=search_engine, llm_client=openai_client)
    logger.info("RAG 引擎实例创建成功。")

    # DB Engine
    if config.ORACLE_SERVICE_NAME:
        dsn = oracledb.makedsn(config.ORACLE_HOST, config.ORACLE_PORT, service_name=config.ORACLE_SERVICE_NAME)
    else:
        dsn = oracledb.makedsn(config.ORACLE_HOST, config.ORACLE_PORT, sid=config.ORACLE_SID)
    db_uri = f"oracle+oracledb://{config.ORACLE_USER}:{config.ORACLE_PASSWORD}@{dsn}"
    db_engine = create_engine(db_uri)
    logger.info("全局数据库引擎实例创建成功。")

    # Text-to-SQL Engine (现在需要传入 db_engine)
    oracle_fetcher = OracleSchemaFetcher()
    # 注意这里的变化：在初始化时传入了 db_engine
    text_to_sql_engine = TextToSQLEngine(schema_fetcher=oracle_fetcher, llm_client=openai_client, db_engine=db_engine)
    logger.info("Text-to-SQL 引擎实例创建成功。")

except Exception as e:
    logger.error(f"创建全局实例失败: {e}", exc_info=True)


# --- 渲染页面的路由 ---

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/rag')
def rag_chat():
    session['rag_history'] = [rag_pipeline.SYSTEM_PROMPT] if rag_pipeline else []
    return render_template('rag.html', history=session['rag_history'])


@app.route('/text_to_sql')
def text_to_sql_chat():
    session['sql_history'] = []
    return render_template('text_to_sql.html', history=session['sql_history'])


# --- 处理Web页面表单提交的路由 ---

@app.route('/ask_rag', methods=['POST'])
def ask_rag():
    if rag_pipeline is None:
        return redirect(url_for('rag_chat'))

    user_query = request.form['query']
    history = session.get('rag_history', [rag_pipeline.SYSTEM_PROMPT])
    history_for_llm = [msg for msg in history if msg.get("role") != "system"]
    result = rag_pipeline.execute(user_query, history_for_llm)
    pure_answer_html = md.render(result["answer"])

    sources_html = ""
    sources_data = result["sources"]
    if sources_data and isinstance(sources_data[0], list):
        sources_list_items = [f"<li>《{doc}》(章节: {chap})</li>" for doc, chap in sources_data]
        sources_html = f"<p><strong>参考文档:</strong></p><ul>{''.join(sources_list_items)}</ul>"

    display_html = pure_answer_html + sources_html

    history.append({"role": "user", "content": user_query})
    history.append({"role": "assistant", "content": display_html})
    session['rag_history'] = history

    return render_template('rag.html', history=history)


@app.route('/ask_sql', methods=['POST'])
def ask_sql():
    """处理 Text-to-SQL 问答请求 (已简化)"""
    if text_to_sql_engine is None:
        return redirect(url_for('text_to_sql_chat'))

    user_query = request.form['query']
    history = session.get('sql_history', [])

    # --- 1. 直接调用引擎的完整流程 ---
    result = text_to_sql_engine.run_text_to_sql_flow(user_query, history)

    # --- 2. 根据返回结果的类型格式化HTML ---
    display_html = ""
    result_type = result.get("type")

    if result_type == "database_result":
        answer_data = result.get("answer", [])
        if isinstance(answer_data, list):
            df = pd.DataFrame(answer_data)
            answer = df.to_markdown(index=False) if not df.empty else "查询成功，但未找到相关记录。"
        else:
            answer = str(answer_data)

        answer_html = md.render(answer)
        sql_html = f"<p><strong>数据库查询:</strong></p><pre><code>{result.get('generated_sql', '')}</code></pre>"
        display_html = answer_html + sql_html
    elif result_type == "natural_language_answer":
        display_html = md.render(result["answer"])
    else:  # 涵盖了 database_error 和其他 error
        error_message = result.get("error", "未知错误")
        error_html = md.render(f"执行时遇到问题。\n\n**详情**: {error_message}")
        sql_html = f"<p><strong>失败的数据库查询:</strong></p><pre><code>{result.get('generated_sql', '未能生成SQL')}</code></pre>"
        display_html = error_html + sql_html

    history.append({"role": "user", "content": user_query})
    history.append({"role": "assistant", "content": display_html})
    session['sql_history'] = history

    return render_template('text_to_sql.html', history=history)


# --- API接口路由 ---

@app.route('/api/rag/ask', methods=['POST'])
def api_ask_rag():
    if rag_pipeline is None:
        return jsonify({"error": "RAG engine not initialized"}), 500
    data = request.json
    user_query = data.get('query')
    history = data.get('history', [])
    if not user_query:
        return jsonify({"error": "Query is required"}), 400
    result = rag_pipeline.execute(user_query, history)
    return jsonify(result)


@app.route('/api/sql/ask', methods=['POST'])
def api_ask_sql():
    """为Text-to-SQL引擎提供API接口 (已简化)"""
    if text_to_sql_engine is None:
        return jsonify({"error": "Text-to-SQL engine not initialized"}), 500

    data = request.json
    user_query = data.get('query')
    history = data.get('history', [])

    if not user_query:
        return jsonify({"error": "Query is required"}), 400

    result = text_to_sql_engine.run_text_to_sql_flow(user_query, history)

    if "error" in result:
        return jsonify(result), 500

    return jsonify(result)


if __name__ == '__main__':
    if not rag_pipeline or not text_to_sql_engine or not db_engine:
        logger.error("错误：核心引擎未能成功加载。Flask 应用无法启动。")
    else:
        logger.info("Flask 应用准备启动...")
        print("=" * 80)
        print(">>> 应用已启动！请在浏览器中访问 http://127.0.0.1:5000 <<<")
        print("=" * 80)
        app.run(debug=False, host='0.0.0.0', port=5000)