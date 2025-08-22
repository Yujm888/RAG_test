import sqlite3
import os

DB_FILE = "finance_reg.db"

# 重新开始，先删除旧文件
if os.path.exists(DB_FILE):
    os.remove(DB_FILE)

conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

print("--- 正在创建数据库 ---")

# --- 1. 创建业务数据表 ---
cursor.execute('''
               CREATE TABLE regulatory_documents
               (
                   id                TEXT PRIMARY KEY,
                   title             TEXT NOT NULL,
                   issuing_authority TEXT NOT NULL,
                   effective_date    DATE,
                   category          TEXT
               );
               ''')
cursor.execute('''
               CREATE TABLE financial_products
               (
                   product_id        INTEGER PRIMARY KEY AUTOINCREMENT,
                   product_name      TEXT NOT NULL,
                   risk_level        TEXT NOT NULL,
                   key_regulation_id TEXT,
                   FOREIGN KEY (key_regulation_id) REFERENCES regulatory_documents (id)
               );
               ''')
print("业务表创建成功。")

# --- 2. 创建专门用于存储注释的“数据字典表” ---
cursor.execute('''
               CREATE TABLE schema_comments
               (
                   table_name  TEXT NOT NULL,
                   column_name TEXT NOT NULL,
                   comment     TEXT
               );
               ''')
print("数据库内的“注释表”创建成功。")

# --- 3. 插入业务数据 ---
documents_data = [
    ('doc_001', '关于规范金融机构资产管理业务的指导意见', '中国人民银行', '2018-04-27', '资产管理'),
    ('doc_002', '商业银行理财业务监督管理办法', '中国银行保险监督管理委员会', '2018-09-28', '银行理财'),
    ('doc_003', '证券期货投资者适当性管理办法', '中国证券监督管理委员会', '2017-07-01', '投资者保护'),
    ('doc_004', '个人信息保护法', '全国人大常委会', '2021-11-01', '数据安全')
]
products_data = [
    ('稳健增益理财A款', '中低风险', 'doc_002'),
    ('高成长股票型基金', '高风险', 'doc_003'),
    ('货币市场基金', '低风险', 'doc_001'),
    ('智能投顾服务', '中风险', 'doc_003'),
    ('结构性存款', '中低风险', 'doc_002')
]
cursor.executemany('INSERT INTO regulatory_documents VALUES (?,?,?,?,?)', documents_data)
cursor.executemany('INSERT INTO financial_products (product_name, risk_level, key_regulation_id) VALUES (?,?,?)',
                   products_data)
print("业务数据插入成功。")

# --- 4. 将我们宝贵的注释，插入到“数据字典表”中 ---
comments_data = [
    ('regulatory_documents', 'table_comment', '监管文件表，包含了所有法规文件的基本信息。'),
    ('regulatory_documents', 'id', '文件ID (主键)'),
    ('regulatory_documents', 'title', '文件官方标题'),
    ('regulatory_documents', 'issuing_authority', '发布该文件的机构或部门'),
    ('regulatory_documents', 'effective_date', '文件生效日期'),
    ('regulatory_documents', 'category', '文件所属分类，例如：资产管理, 银行理财, 投资者保护等'),

    ('financial_products', 'table_comment', '金融产品表，记录了公司发行的金融产品列表。'),
    ('financial_products', 'product_id', '产品ID (主键)'),
    ('financial_products', 'product_name', '金融产品的全称'),
    ('financial_products', 'risk_level', "产品的风险等级，可能的值有：'低风险', '中低风险', '中风险', '高风险'"),
    ('financial_products', 'key_regulation_id', '该产品必须遵守的核心法规文件ID (外键，关联到 regulatory_documents.id)')
]
cursor.executemany('INSERT INTO schema_comments VALUES (?,?,?)', comments_data)
print("注释数据插入成功。")

conn.commit()
conn.close()

print(f"\n成功！数据库 '{DB_FILE}' 已重建，并包含了完整的结构、数据和注释。")
import sqlite3
import os

DB_FILE = "finance_reg.db"

# 重新开始，先删除旧文件
if os.path.exists(DB_FILE):
    os.remove(DB_FILE)

conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

print("--- 正在创建数据库 ---")

# --- 1. 创建业务数据表 ---
cursor.execute('''
CREATE TABLE regulatory_documents (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    issuing_authority TEXT NOT NULL,
    effective_date DATE,
    category TEXT
);
''')
cursor.execute('''
CREATE TABLE financial_products (
    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_name TEXT NOT NULL,
    risk_level TEXT NOT NULL,
    key_regulation_id TEXT,
    FOREIGN KEY (key_regulation_id) REFERENCES regulatory_documents(id)
);
''')
print("业务表创建成功。")

# --- 2. 创建专门用于存储注释的“数据字典表” ---
cursor.execute('''
CREATE TABLE schema_comments (
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    comment TEXT
);
''')
print("数据库内的“注释表”创建成功。")

# --- 3. 插入业务数据 ---
documents_data = [
    ('doc_001', '关于规范金融机构资产管理业务的指导意见', '中国人民银行', '2018-04-27', '资产管理'),
    ('doc_002', '商业银行理财业务监督管理办法', '中国银行保险监督管理委员会', '2018-09-28', '银行理财'),
    ('doc_003', '证券期货投资者适当性管理办法', '中国证券监督管理委员会', '2017-07-01', '投资者保护'),
    ('doc_004', '个人信息保护法', '全国人大常委会', '2021-11-01', '数据安全')
]
products_data = [
    ('稳健增益理财A款', '中低风险', 'doc_002'),
    ('高成长股票型基金', '高风险', 'doc_003'),
    ('货币市场基金', '低风险', 'doc_001'),
    ('智能投顾服务', '中风险', 'doc_003'),
    ('结构性存款', '中低风险', 'doc_002')
]
cursor.executemany('INSERT INTO regulatory_documents VALUES (?,?,?,?,?)', documents_data)
cursor.executemany('INSERT INTO financial_products (product_name, risk_level, key_regulation_id) VALUES (?,?,?)', products_data)
print("业务数据插入成功。")

# --- 4. 将我们宝贵的注释，插入到“数据字典表”中 ---
comments_data = [
    ('regulatory_documents', 'table_comment', '监管文件表，包含了所有法规文件的基本信息。'),
    ('regulatory_documents', 'id', '文件ID (主键)'),
    ('regulatory_documents', 'title', '文件官方标题'),
    ('regulatory_documents', 'issuing_authority', '发布该文件的机构或部门'),
    ('regulatory_documents', 'effective_date', '文件生效日期'),
    ('regulatory_documents', 'category', '文件所属分类，例如：资产管理, 银行理财, 投资者保护等'),

    ('financial_products', 'table_comment', '金融产品表，记录了公司发行的金融产品列表。'),
    ('financial_products', 'product_id', '产品ID (主键)'),
    ('financial_products', 'product_name', '金融产品的全称'),
    ('financial_products', 'risk_level', "产品的风险等级，可能的值有：'低风险', '中低风险', '中风险', '高风险'"),
    ('financial_products', 'key_regulation_id', '该产品必须遵守的核心法规文件ID (外键，关联到 regulatory_documents.id)')
]
cursor.executemany('INSERT INTO schema_comments VALUES (?,?,?)', comments_data)
print("注释数据插入成功。")


conn.commit()
conn.close()

print(f"\n成功！数据库 '{DB_FILE}' 已重建，并包含了完整的结构、数据和注释。")