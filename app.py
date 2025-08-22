#app.py

from flask import Flask, request, render_template, session, redirect, url_for
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
# 设置一个密钥，以便使用 session
app.secret_key = 'a_very_secret_key_for_session'

# --- 全局服务实例 ---
rag_pipeline = None
text_to_sql_engine = None
try:
    search_engine = SearchEngine(config.INDEX_FILE_PATH, config.CHUNKS_FILE_PATH)
    rag_pipeline = RAGPipeline(search_engine=search_engine, llm_client=openai_client)
    logger.info("RAG 引擎实例创建成功。")

    oracle_fetcher = OracleSchemaFetcher()
    text_to_sql_engine = TextToSQLEngine(schema_fetcher=oracle_fetcher, llm_client=openai_client)
    logger.info("Text-to-SQL 引擎实例创建成功。")
except Exception as e:
    logger.error(f"创建全局实例失败: {e}", exc_info=True)


# --- 渲染页面的路由 ---

@app.route('/')
def index():
    """渲染主导航页。"""
    return render_template('index.html')


@app.route('/rag')
def rag_chat():
    """渲染 RAG 聊天页面，并清空 RAG 的历史记录。"""
    session['rag_history'] = [rag_pipeline.SYSTEM_PROMPT] if rag_pipeline else []
    return render_template('rag.html', history=session['rag_history'])


@app.route('/text_to_sql')
def text_to_sql_chat():
    """渲染 Text-to-SQL 聊天页面，并清空其历史记录。"""
    session['sql_history'] = []  # SQL 模式不需要系统级 Prompt
    return render_template('text_to_sql.html', history=session['sql_history'])


# --- 处理表单提交的路由 ---

@app.route('/ask_rag', methods=['POST'])
def ask_rag():
    """处理 RAG 问答请求。"""
    if rag_pipeline is None:
        return redirect(url_for('rag_chat'))

    user_query = request.form['query']
    # 从 session 中获取当前 RAG 的历史记录
    history = session.get('rag_history', [rag_pipeline.SYSTEM_PROMPT])

    # 清理历史记录以供 LLM 使用
    history_for_llm = [msg for msg in history if msg.get("role") != "system"]

    result = rag_pipeline.execute(user_query, history_for_llm)
    pure_answer_html = md.render(result["answer"])

    # 格式化来源信息
    sources_html = ""
    sources_data = result["sources"]
    if sources_data and isinstance(sources_data[0], list):
        sources_list_items = [f"<li>《{doc}》(章节: {chap})</li>" for doc, chap in sources_data]
        sources_html = f"<p><strong>参考文档:</strong></p><ul>{''.join(sources_list_items)}</ul>"

    display_html = pure_answer_html + sources_html

    # 更新 session 中的历史记录
    history.append({"role": "user", "content": user_query})
    history.append({"role": "assistant", "content": display_html})
    session['rag_history'] = history

    return render_template('rag.html', history=history)


@app.route('/ask_sql', methods=['POST'])
def ask_sql():
    """处理 Text-to-SQL 问答请求。"""
    if text_to_sql_engine is None:
        return redirect(url_for('text_to_sql_chat'))

    user_query = request.form['query']
    history = session.get('sql_history', [])

    response_from_engine = text_to_sql_engine.generate_sql(user_query, history)

    display_html = ""

    if not response_from_engine:
        display_html = md.render("抱歉，无法处理您的问题。")
    else:
        is_sql = "SELECT" in response_from_engine.upper() and response_from_engine.endswith(';')
        if is_sql:
            try:
                if config.ORACLE_SERVICE_NAME:
                    dsn = oracledb.makedsn(config.ORACLE_HOST, config.ORACLE_PORT,
                                           service_name=config.ORACLE_SERVICE_NAME)
                else:
                    dsn = oracledb.makedsn(config.ORACLE_HOST, config.ORACLE_PORT, sid=config.ORACLE_SID)
                db_uri = f"oracle+oracledb://{config.ORACLE_USER}:{config.ORACLE_PASSWORD}@{dsn}"
                engine = create_engine(db_uri)
                with engine.connect() as connection:
                    df = pd.read_sql_query(response_from_engine.strip().rstrip(';'), connection)

                answer = df.to_markdown(index=False) if not df.empty else "查询成功，但未找到相关记录。"
                answer_html = md.render(answer)
                sql_html = f"<p><strong>数据库查询:</strong></p><pre><code>{response_from_engine}</code></pre>"
                display_html = answer_html + sql_html
            except Exception as e:
                logger.error(f"执行 SQL 时出错: {e}")
                error_html = md.render(f"执行数据库查询时遇到问题。\n\n**错误详情**: {e}")
                sql_html = f"<p><strong>失败的数据库查询:</strong></p><pre><code>{response_from_engine}</code></pre>"
                display_html = error_html + sql_html
        else:
            display_html = md.render(response_from_engine)

    history.append({"role": "user", "content": user_query})
    history.append({"role": "assistant", "content": display_html})
    session['sql_history'] = history

    return render_template('text_to_sql.html', history=history)


if __name__ == '__main__':
    if not rag_pipeline or not text_to_sql_engine:
        logger.error("错误：核心引擎未能成功加载。Flask 应用无法启动。")
    else:
        logger.info("Flask 应用准备启动...")
        print("=" * 80)
        print(">>> 应用已启动！请在浏览器中访问 http://127.0.0.1:5000 <<<")
        print("=" * 80)
        app.run(debug=False, host='0.0.0.0', port=5000)