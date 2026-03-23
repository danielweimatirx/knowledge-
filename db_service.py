"""
数据库服务层 — 封装所有数据库查询逻辑
路由层只调这里的函数，不直接写 SQL
"""
import json
from datetime import datetime

import pymysql

# ==================== 连接配置 ====================

DB_CONFIGS = {
    "local": dict(
        host="127.0.0.1", port=16001,
        user="dump", password="111",
        database="moi", charset="utf8mb4",
    ),
    "remote": dict(
        host="freetier-01.cn-hangzhou.cluster.cn-dev.matrixone.tech",
        port=6001,
        user="ws_bf2d347f:moi_core_system:accountadmin",
        password="moi_2d76c2c1a5eb95b160e10e0b1dc47109ded45fbc9ad7641d3adcbd07ce09da78",
        database="moi", charset="utf8mb4",
    ),
}


def _get_conn(target: str):
    """获取数据库连接，target 不合法时抛 ValueError"""
    cfg = DB_CONFIGS.get(target)
    if not cfg:
        raise ValueError(f"未知 target: {target}")
    return pymysql.connect(**cfg)


def _serialize(obj):
    """递归将 datetime 转为 ISO 字符串"""
    if isinstance(obj, list):
        for r in obj:
            if isinstance(r, dict):
                _serialize(r)
    elif isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, datetime):
                obj[k] = v.isoformat(timespec="seconds")
    return obj


# ==================== 原始数据查询（迁移面板用） ====================

def get_raw_data(target: str) -> dict:
    """查询 knowledge_base 和 nl2sql_knowledge 原始数据"""
    conn = _get_conn(target)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT id, name, usage_notes, "
                "CAST(`tables` AS CHAR) AS tables_json, "
                "created_by, updated_by, created_at, updated_at "
                "FROM moi.knowledge_base ORDER BY id"
            )
            kb_rows = cur.fetchall()

            cur.execute(
                "SELECT id, knowledge_base_id, knowledge_type, "
                "knowledge_key, name, "
                "CAST(knowledge_value AS CHAR) AS knowledge_value, "
                "CAST(associate_tables AS CHAR) AS associate_tables, "
                "explanation_type, created_by, updated_by, "
                "created_at, updated_at "
                "FROM moi.nl2sql_knowledge ORDER BY id"
            )
            nk_rows = cur.fetchall()

        return {
            "ok": True,
            "knowledge_base": _serialize(kb_rows),
            "nl2sql_knowledge": _serialize(nk_rows),
        }
    finally:
        conn.close()


# ==================== 知识库列表 ====================

def get_knowledge_base_list(target: str) -> dict:
    """获取所有知识库，附带知识条目计数"""
    conn = _get_conn(target)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT kb.id, kb.name, kb.usage_notes, "
                "CAST(kb.`tables` AS CHAR) AS tables_json, "
                "kb.created_by, kb.updated_by, kb.created_at, kb.updated_at, "
                "IFNULL(cnt.c, 0) AS knowledge_count "
                "FROM moi.knowledge_base kb "
                "LEFT JOIN ("
                "  SELECT knowledge_base_id, COUNT(*) AS c "
                "  FROM moi.nl2sql_knowledge GROUP BY knowledge_base_id"
                ") cnt ON kb.id = cnt.knowledge_base_id "
                "ORDER BY kb.id DESC"
            )
            rows = cur.fetchall()
        return {"ok": True, "data": _serialize(rows)}
    finally:
        conn.close()


# ==================== 知识库详情 ====================

def get_knowledge_base_detail(target: str, kb_id: int) -> dict:
    """获取单个知识库信息 + 其下所有知识条目"""
    conn = _get_conn(target)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT id, name, usage_notes, "
                "CAST(`tables` AS CHAR) AS tables_json, "
                "CAST(files AS CHAR) AS files_json, "
                "created_by, updated_by, created_at, updated_at "
                "FROM moi.knowledge_base WHERE id = %s", (kb_id,)
            )
            kb = cur.fetchone()
            if not kb:
                return {"ok": False, "msg": f"知识库 {kb_id} 不存在"}

            cur.execute(
                "SELECT id, knowledge_base_id, knowledge_type, knowledge_key, "
                "name, CAST(knowledge_value AS CHAR) AS knowledge_value, "
                "CAST(associate_tables AS CHAR) AS associate_tables, "
                "explanation_type, created_by, updated_by, created_at, updated_at "
                "FROM moi.nl2sql_knowledge WHERE knowledge_base_id = %s ORDER BY id",
                (kb_id,)
            )
            nk_rows = cur.fetchall()

        return {
            "ok": True,
            "knowledge_base": _serialize(kb),
            "knowledge_items": _serialize(nk_rows),
        }
    finally:
        conn.close()


# ==================== 新增知识库 ====================

def create_knowledge_base(target: str, data: dict) -> dict:
    """新增知识库，返回新记录 ID"""
    conn = _get_conn(target)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO moi.knowledge_base "
                "(name, usage_notes, `tables`, files, created_by, updated_by) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    data["name"],
                    data.get("usage_notes"),
                    data.get("tables_json"),
                    data.get("files_json"),
                    data.get("created_by", "admin"),
                    data.get("updated_by", "admin"),
                ),
            )
            conn.commit()
            new_id = cur.lastrowid
        return {"ok": True, "id": new_id}
    finally:
        conn.close()


# ==================== 编辑知识库 ====================

def update_knowledge_base(target: str, kb_id: int, data: dict) -> dict:
    """更新知识库基本信息"""
    conn = _get_conn(target)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE moi.knowledge_base SET "
                "name = %s, usage_notes = %s, `tables` = %s, "
                "files = %s, updated_by = %s "
                "WHERE id = %s",
                (
                    data["name"],
                    data.get("usage_notes"),
                    data.get("tables_json"),
                    data.get("files_json"),
                    data.get("updated_by", "admin"),
                    kb_id,
                ),
            )
            conn.commit()
            if cur.rowcount == 0:
                return {"ok": False, "msg": f"知识库 {kb_id} 不存在"}
        return {"ok": True}
    finally:
        conn.close()


# ==================== 新增知识条目 ====================

def create_knowledge_item(target: str, data: dict) -> dict:
    """新增一条 nl2sql_knowledge"""
    conn = _get_conn(target)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO moi.nl2sql_knowledge "
                "(knowledge_base_id, knowledge_type, knowledge_key, name, "
                "knowledge_value, associate_tables, explanation_type, "
                "created_by, updated_by) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    data["knowledge_base_id"],
                    data["knowledge_type"],
                    data["knowledge_key"],
                    data.get("name"),
                    data.get("knowledge_value"),
                    data.get("associate_tables"),
                    data.get("explanation_type"),
                    data.get("created_by", "admin"),
                    data.get("updated_by", "admin"),
                ),
            )
            conn.commit()
            new_id = cur.lastrowid
        return {"ok": True, "id": new_id}
    finally:
        conn.close()


# ==================== 编辑知识条目 ====================

def update_knowledge_item(target: str, item_id: int, data: dict) -> dict:
    """更新一条 nl2sql_knowledge"""
    conn = _get_conn(target)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE moi.nl2sql_knowledge SET "
                "knowledge_type = %s, knowledge_key = %s, name = %s, "
                "knowledge_value = %s, associate_tables = %s, "
                "explanation_type = %s, updated_by = %s "
                "WHERE id = %s",
                (
                    data["knowledge_type"],
                    data["knowledge_key"],
                    data.get("name"),
                    data.get("knowledge_value"),
                    data.get("associate_tables"),
                    data.get("explanation_type"),
                    data.get("updated_by", "admin"),
                    item_id,
                ),
            )
            conn.commit()
            if cur.rowcount == 0:
                return {"ok": False, "msg": f"知识条目 {item_id} 不存在"}
        return {"ok": True}
    finally:
        conn.close()


# ==================== 可选关联表 ====================

# 常量来自 migrate_nl2sql.py
_NEW_DB_NAME = "jst_flat_table"
_NEW_DATABASE_ID = 1
_NEW_CATALOG_ID = 10001
_TABLE_ID_MAP = {
    "revenue_cost": 40169,
    "bpc_consolidated_report": 40148,
    "sales_orders_result": 40170,
    "open_orders_result": 40162,
    "output_value_lg": 40164,
    "output_amount_lg": 40163,
    "output_value_pc": 40165,
    "staff_info": 40172,
    "sales_vat_invoice": 40171,
    "tax_ledger": 40173,
    "main_companies": 40160,
    "main_business_unit": 40159,
    "logistics": 40157,
    "capacity": 40150,
    "electricity_bill_summary": 40152,
    "inventory_pc": 40155,
    "inventory_aging_pc": 40154,
}


def get_available_databases(target: str) -> dict:
    """获取所有可用数据库列表（排除系统库）"""
    conn = _get_conn(target)
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW DATABASES")
            rows = cur.fetchall()
        system_dbs = {'information_schema', 'mysql', 'system', 'system_metrics', 'mo_catalog'}
        dbs = sorted([r[0] for r in rows if r[0] not in system_dbs])
        return {"ok": True, "databases": dbs}
    finally:
        conn.close()


def get_available_tables(target: str, db_name: str = None) -> dict:
    """获取指定数据库中的可选表列表（含描述）"""
    if not db_name:
        db_name = _NEW_DB_NAME
    conn = _get_conn(target)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT TABLE_NAME, TABLE_COMMENT "
                "FROM information_schema.tables "
                "WHERE TABLE_SCHEMA = %s "
                "ORDER BY TABLE_NAME",
                (db_name,)
            )
            rows = cur.fetchall()

        tables = []
        for name, comment in rows:
            tables.append({
                "name": name,
                "table_id": _TABLE_ID_MAP.get(name) if db_name == _NEW_DB_NAME else None,
                "comment": comment or "",
            })

        return {
            "ok": True,
            "db_name": db_name,
            "database_id": _NEW_DATABASE_ID if db_name == _NEW_DB_NAME else None,
            "catalog_id": _NEW_CATALOG_ID if db_name == _NEW_DB_NAME else None,
            "tables": tables,
        }
    finally:
        conn.close()


def build_tables_json(table_names: list) -> str:
    """根据选中的表名列表，构建 knowledge_base.tables 字段的 JSON"""
    table_ids = []
    valid_names = []
    for name in table_names:
        tid = _TABLE_ID_MAP.get(name)
        if tid is not None:
            table_ids.append(tid)
            valid_names.append(name)
        else:
            valid_names.append(name)

    entry = {
        "db_name": _NEW_DB_NAME,
        "table_ids": table_ids,
        "table_names": valid_names,
        "parents": [f"catalog-{_NEW_CATALOG_ID}", f"database-{_NEW_DATABASE_ID}"],
    }
    return json.dumps([entry], ensure_ascii=False)


# ==================== 删除知识条目 ====================

def delete_knowledge_item(target: str, item_id: int) -> dict:
    """删除一条 nl2sql_knowledge"""
    conn = _get_conn(target)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM moi.nl2sql_knowledge WHERE id = %s", (item_id,)
            )
            conn.commit()
            if cur.rowcount == 0:
                return {"ok": False, "msg": f"知识条目 {item_id} 不存在"}
        return {"ok": True}
    finally:
        conn.close()


def delete_knowledge_base(target: str, kb_id: int) -> dict:
    """删除知识库及其下所有知识条目"""
    conn = _get_conn(target)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM moi.nl2sql_knowledge WHERE knowledge_base_id = %s", (kb_id,)
            )
            cur.execute(
                "DELETE FROM moi.knowledge_base WHERE id = %s", (kb_id,)
            )
            conn.commit()
            if cur.rowcount == 0:
                return {"ok": False, "msg": f"知识库 {kb_id} 不存在"}
        return {"ok": True}
    finally:
        conn.close()
