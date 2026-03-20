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
                "SUBSTRING(knowledge_key, 1, 200) AS knowledge_key_preview, "
                "name, explanation_type, created_at, updated_at "
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
                "ORDER BY kb.id"
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
