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
        database="moi", charset="utf8mb4", autocommit=True,
    ),
    "remote": dict(
        host="freetier-01.cn-hangzhou.cluster.cn-dev.matrixone.tech",
        port=6001,
        user="ws_bf2d347f:moi_core_system:accountadmin",
        password="moi_2d76c2c1a5eb95b160e10e0b1dc47109ded45fbc9ad7641d3adcbd07ce09da78",
        database="moi", charset="utf8mb4", autocommit=True,
    ),
    "new_dev": dict(
        host="freetier-01.cn-hangzhou.cluster.cn-dev.matrixone.tech",
        port=6001,
        user="ws_0a52bbd0:u_fc5a80864a514c67b565d520aeb5f9d1",
        password="moi_6b1eaec993750d88fe87002e32380a81b25594bc084564379c92c5f126dd5eab",
        database="moi", charset="utf8mb4", autocommit=True,
    ),
    "portal": dict(
        host="freetier-01.cn-hangzhou.cluster.cn-dev.matrixone.tech",
        port=6001,
        user="ws_bfb9ca8d:qa_manual_20260330185108_x9f3k2:accountadmin",
        password="moi_216a042120beaf5cdf357dfbc7a335a29c4b5d6641feeb598da2f3ccd824d342",
        database="moi", charset="utf8mb4", autocommit=True,
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


# ==================== 知识库差异对比 ====================

def compare_knowledge_bases(ws_a: str, ws_b: str) -> dict:
    """对比两个工作区的知识库差异，按 name 匹配，并逐条对比知识条目"""
    label_a = WORKSPACE_LABELS.get(ws_a, ws_a)
    label_b = WORKSPACE_LABELS.get(ws_b, ws_b)
    conn_a = conn_b = None
    try:
        conn_a = _get_conn(ws_a)
    except Exception as e:
        return {"ok": False, "msg": f"连接工作区 [{label_a}] 失败: {e}"}
    try:
        conn_b = _get_conn(ws_b)
    except Exception as e:
        conn_a.close()
        return {"ok": False, "msg": f"连接工作区 [{label_b}] 失败: {e}"}
    try:
        def _fetch_all(conn):
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(
                    "SELECT kb.id, kb.name, kb.usage_notes, "
                    "CAST(kb.`tables` AS CHAR) AS tables_json, "
                    "kb.created_at, kb.updated_at "
                    "FROM moi.knowledge_base kb "
                    "ORDER BY kb.id"
                )
                rows = cur.fetchall()
            return _serialize(rows)

        def _fetch_items(conn, kb_ids):
            """获取指定知识库的所有知识条目"""
            if not kb_ids:
                return {}
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                placeholders = ",".join(["%s"] * len(kb_ids))
                cur.execute(
                    f"SELECT knowledge_base_id, knowledge_type, knowledge_key, name, "
                    f"CAST(knowledge_value AS CHAR) AS knowledge_value, "
                    f"CAST(associate_tables AS CHAR) AS associate_tables "
                    f"FROM moi.nl2sql_knowledge WHERE knowledge_base_id IN ({placeholders}) "
                    f"ORDER BY knowledge_base_id, knowledge_type, knowledge_key",
                    kb_ids
                )
                rows = cur.fetchall()
            # 按 kb_id 分组
            result = {kb_id: [] for kb_id in kb_ids}
            for r in rows:
                result[r["knowledge_base_id"]].append(r)
            return result

        rows_a = _fetch_all(conn_a)
        rows_b = _fetch_all(conn_b)

        map_a = {r["name"]: r for r in rows_a}
        map_b = {r["name"]: r for r in rows_b}

        all_names = sorted(set(map_a.keys()) | set(map_b.keys()))

        # 找出两边都有的知识库，需要对比条目
        both_names = [n for n in all_names if n in map_a and n in map_b]
        both_a_ids = [map_a[n]["id"] for n in both_names]
        both_b_ids = [map_b[n]["id"] for n in both_names]

        items_a = _fetch_items(conn_a, both_a_ids)
        items_b = _fetch_items(conn_b, both_b_ids)

        only_a = []
        only_b = []
        both = []
        diff = []

        for name in all_names:
            in_a = name in map_a
            in_b = name in map_b
            if in_a and not in_b:
                ka = map_a[name]
                ka["nk_count"] = len(_fetch_items(conn_a, [ka["id"]]).get(ka["id"], []))
                only_a.append(ka)
            elif in_b and not in_a:
                kb_item = map_b[name]
                kb_item["nk_count"] = len(_fetch_items(conn_b, [kb_item["id"]]).get(kb_item["id"], []))
                only_b.append(kb_item)
            else:
                ka, kb_item = map_a[name], map_b[name]
                items_of_a = items_a.get(ka["id"], [])
                items_of_b = items_b.get(kb_item["id"], [])
                ka["nk_count"] = len(items_of_a)
                kb_item["nk_count"] = len(items_of_b)
                both.append({"name": name, "a": ka, "b": kb_item})

                # 逐条对比知识条目（按 knowledge_type + knowledge_key 匹配）
                def item_key(item):
                    return (item["knowledge_type"] or "", item["knowledge_key"] or "")

                def item_content(item):
                    return (
                        item.get("name") or "",
                        item.get("knowledge_value") or "",
                        item.get("associate_tables") or "",
                    )

                a_map = {item_key(i): i for i in items_of_a}
                b_map = {item_key(i): i for i in items_of_b}
                all_keys = sorted(set(a_map.keys()) | set(b_map.keys()))

                item_diffs = []
                for key in all_keys:
                    ia = a_map.get(key)
                    ib = b_map.get(key)
                    if ia and not ib:
                        item_diffs.append({
                            "type": key[0], "key": key[1],
                            "status": "only_a",
                            "a_name": ia.get("name"),
                            "a_value": (ia.get("knowledge_value") or "")[:200],
                        })
                    elif ib and not ia:
                        item_diffs.append({
                            "type": key[0], "key": key[1],
                            "status": "only_b",
                            "b_name": ib.get("name"),
                            "b_value": (ib.get("knowledge_value") or "")[:200],
                        })
                    elif item_content(ia) != item_content(ib):
                        item_diffs.append({
                            "type": key[0], "key": key[1],
                            "status": "different",
                            "a_name": ia.get("name"),
                            "b_name": ib.get("name"),
                            "a_value": (ia.get("knowledge_value") or "")[:200],
                            "b_value": (ib.get("knowledge_value") or "")[:200],
                        })

                # 知识库元信息差异
                diffs_detail = {}
                if ka.get("tables_json") != kb_item.get("tables_json"):
                    diffs_detail["tables"] = {"a": ka.get("tables_json"), "b": kb_item.get("tables_json")}
                if (ka.get("usage_notes") or "") != (kb_item.get("usage_notes") or ""):
                    diffs_detail["usage_notes"] = {"a": ka.get("usage_notes") or "", "b": kb_item.get("usage_notes") or ""}

                if item_diffs or diffs_detail:
                    diff.append({
                        "name": name,
                        "a_id": ka["id"],
                        "b_id": kb_item["id"],
                        "a_nk": ka["nk_count"],
                        "b_nk": kb_item["nk_count"],
                        "diffs": diffs_detail,
                        "item_diffs": item_diffs,
                    })

        return {
            "ok": True,
            "a_key": ws_a,
            "a_label": label_a,
            "a_info": WORKSPACE_INFO.get(ws_a, {}),
            "b_key": ws_b,
            "b_label": label_b,
            "b_info": WORKSPACE_INFO.get(ws_b, {}),
            "a_total": len(rows_a),
            "b_total": len(rows_b),
            "only_a": only_a,
            "only_b": only_b,
            "both_count": len(both),
            "same_count": len(both) - len(diff),
            "diff": diff,
        }
    finally:
        if conn_a:
            conn_a.close()
        if conn_b:
            conn_b.close()


def sync_knowledge_base(source: str, target: str, kb_name: str) -> dict:
    """
    将源工作区的某个知识库同步到目标工作区（按 name 匹配）。
    如果目标不存在则创建，存在则更新并覆盖知识条目。
    """
    src_label = WORKSPACE_LABELS.get(source, source)
    dst_label = WORKSPACE_LABELS.get(target, target)

    try:
        src_conn = _get_conn(source)
    except Exception as e:
        return {"ok": False, "msg": f"连接源工作区 [{src_label}] 失败: {e}"}
    try:
        dst_conn = _get_conn(target)
    except Exception as e:
        src_conn.close()
        return {"ok": False, "msg": f"连接目标工作区 [{dst_label}] 失败: {e}"}

    try:
        # 1. 从源读取知识库
        with src_conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT id, name, usage_notes, CAST(`tables` AS CHAR) AS tables_json, "
                "CAST(files AS CHAR) AS files_json "
                "FROM moi.knowledge_base WHERE name = %s LIMIT 1",
                (kb_name,)
            )
            src_kb = cur.fetchone()
        if not src_kb:
            return {"ok": False, "msg": f"源工作区不存在知识库「{kb_name}」"}

        # 2. 从源读取知识条目
        with src_conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT knowledge_type, knowledge_key, name, "
                "CAST(knowledge_value AS CHAR) AS knowledge_value, "
                "CAST(associate_tables AS CHAR) AS associate_tables, "
                "explanation_type, created_by, updated_by "
                "FROM moi.nl2sql_knowledge WHERE knowledge_base_id = %s",
                (src_kb["id"],)
            )
            src_items = cur.fetchall()

        # 3. 查找目标知识库
        with dst_conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT id FROM moi.knowledge_base WHERE name = %s LIMIT 1",
                (kb_name,)
            )
            dst_kb = cur.fetchone()

        created_new = False
        deleted_count = 0

        if not dst_kb:
            # 目标不存在，创建新知识库
            with dst_conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO moi.knowledge_base (name, usage_notes, `tables`, files, "
                    "created_by, updated_by, created_at, updated_at) "
                    "VALUES (%s, %s, %s, %s, 'sync', 'sync', NOW(), NOW())",
                    (kb_name, src_kb.get("usage_notes"), src_kb.get("tables_json"), src_kb.get("files_json"))
                )
                dst_kb_id = cur.lastrowid
            created_new = True
        else:
            dst_kb_id = dst_kb["id"]
            # 4. 更新目标知识库元信息
            with dst_conn.cursor() as cur:
                cur.execute(
                    "UPDATE moi.knowledge_base SET usage_notes = %s, `tables` = %s, files = %s, "
                    "updated_by = 'sync', updated_at = NOW() WHERE id = %s",
                    (src_kb.get("usage_notes"), src_kb.get("tables_json"), src_kb.get("files_json"), dst_kb_id)
                )
            # 5. 删除目标的旧知识条目
            with dst_conn.cursor() as cur:
                cur.execute("DELETE FROM moi.nl2sql_knowledge WHERE knowledge_base_id = %s", (dst_kb_id,))
                deleted_count = cur.rowcount

        # 6. 插入源的知识条目
        inserted_count = 0
        for item in src_items:
            with dst_conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO moi.nl2sql_knowledge "
                    "(knowledge_base_id, knowledge_type, knowledge_key, name, "
                    "knowledge_value, associate_tables, explanation_type, "
                    "created_by, updated_by, created_at, updated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'sync', NOW(), NOW())",
                    (
                        dst_kb_id,
                        item["knowledge_type"],
                        item["knowledge_key"],
                        item.get("name"),
                        item.get("knowledge_value"),
                        item.get("associate_tables"),
                        item.get("explanation_type"),
                        item.get("created_by") or "sync",
                    )
                )
                inserted_count += 1

        dst_conn.commit()

        return {
            "ok": True,
            "msg": f"同步完成：从 {src_label} → {dst_label}",
            "kb_name": kb_name,
            "created_new": created_new,
            "deleted_items": deleted_count,
            "inserted_items": inserted_count,
        }
    finally:
        src_conn.close()
        dst_conn.close()


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


# ==================== 跨工作区迁移 ====================

WORKSPACE_LABELS = {
    "local": "Local (Docker)",
    "remote": "问数Dev",
    "new_dev": "新Dev",
    "portal": "AI Portal",
}

WORKSPACE_INFO = {
    "local": {"account": "dump", "host": "127.0.0.1:16001", "database": "moi", "workspace_id": "-", "workspace_name": "local-docker"},
    "remote": {"account": "ws_bf2d347f", "host": "freetier-01.cn-hangzhou.cluster.cn-dev.matrixone.tech:6001", "database": "moi", "workspace_id": "ws_bf2d347f", "workspace_name": "moi_core_system"},
    "new_dev": {"account": "ws_0a52bbd0", "host": "freetier-01.cn-hangzhou.cluster.cn-dev.matrixone.tech:6001", "database": "moi", "workspace_id": "ws_0a52bbd0", "workspace_name": "new-dev"},
    "portal": {"account": "ws_bfb9ca8d", "host": "freetier-01.cn-hangzhou.cluster.cn-dev.matrixone.tech:6001", "database": "moi", "workspace_id": "6a0d513b-9b28-5c5c-0e5f-145427bde36c", "workspace_name": "qa-manual-ws-20260330185108-x9f3k2"},
}


def migrate_knowledge_bases(source: str, target: str, kb_ids: list = None, dry_run: bool = False, overwrite: bool = False) -> dict:
    """
    跨工作区迁移知识库及其知识条目。
    dry_run=True 时只读取源库统计数量，不写入目标库。
    overwrite=True 时先清空目标库的 knowledge_base 和 nl2sql_knowledge 再插入。
    """
    src_label = WORKSPACE_LABELS.get(source, source)
    dst_label = WORKSPACE_LABELS.get(target, target)
    try:
        src_conn = _get_conn(source)
    except Exception as e:
        return {"ok": False, "msg": f"连接源工作区 [{src_label}] 失败: {e}"}
    try:
        # 1. 读取源库知识库
        with src_conn.cursor(pymysql.cursors.DictCursor) as cur:
            if kb_ids:
                placeholders = ",".join(["%s"] * len(kb_ids))
                cur.execute(
                    f"SELECT id, name, usage_notes, "
                    f"CAST(`tables` AS CHAR) AS tables_json, "
                    f"CAST(files AS CHAR) AS files_json, "
                    f"created_by, updated_by, created_at, updated_at "
                    f"FROM moi.knowledge_base WHERE id IN ({placeholders}) ORDER BY id",
                    kb_ids,
                )
            else:
                cur.execute(
                    "SELECT id, name, usage_notes, "
                    "CAST(`tables` AS CHAR) AS tables_json, "
                    "CAST(files AS CHAR) AS files_json, "
                    "created_by, updated_by, created_at, updated_at "
                    "FROM moi.knowledge_base ORDER BY id"
                )
            kb_rows = cur.fetchall()

        if not kb_rows:
            return {"ok": True, "msg": "没有需要迁移的知识库", "kb_count": 0, "nk_count": 0}

        # 2. 读取源库知识条目
        actual_kb_ids = [kb["id"] for kb in kb_rows]
        with src_conn.cursor(pymysql.cursors.DictCursor) as cur:
            placeholders = ",".join(["%s"] * len(actual_kb_ids))
            cur.execute(
                f"SELECT id, knowledge_base_id, knowledge_type, knowledge_key, "
                f"name, CAST(knowledge_value AS CHAR) AS knowledge_value, "
                f"CAST(associate_tables AS CHAR) AS associate_tables, "
                f"explanation_type, created_by, updated_by, created_at, updated_at "
                f"FROM moi.nl2sql_knowledge WHERE knowledge_base_id IN ({placeholders}) ORDER BY id",
                actual_kb_ids,
            )
            nk_rows = cur.fetchall()

        # 空跑模式：返回详细预览
        if dry_run:
            kb_details = []
            for kb in kb_rows:
                nk_cnt = sum(1 for nk in nk_rows if nk["knowledge_base_id"] == kb["id"])
                kb_details.append({
                    "id": kb["id"],
                    "name": kb["name"],
                    "nk_count": nk_cnt,
                })
            return {
                "ok": True,
                "dry_run": True,
                "source": source,
                "source_label": src_label,
                "source_info": WORKSPACE_INFO.get(source, {}),
                "target": target,
                "target_label": dst_label,
                "target_info": WORKSPACE_INFO.get(target, {}),
                "kb_count": len(kb_rows),
                "nk_count": len(nk_rows),
                "kb_details": kb_details,
                "nk_errors": [],
            }

        # 3. 写入目标库
        try:
            dst_conn = _get_conn(target)
        except Exception as e:
            return {"ok": False, "msg": f"连接目标工作区 [{dst_label}] 失败: {e}"}
        try:
            # 覆盖模式：先清空目标库
            deleted_kb = 0
            deleted_nk = 0
            if overwrite:
                with dst_conn.cursor() as cur:
                    cur.execute("DELETE FROM moi.nl2sql_knowledge")
                    deleted_nk = cur.rowcount
                    cur.execute("DELETE FROM moi.knowledge_base")
                    deleted_kb = cur.rowcount

            kb_id_map = {}
            for kb in kb_rows:
                with dst_conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO moi.knowledge_base "
                        "(name, usage_notes, `tables`, files, created_by, updated_by, created_at, updated_at) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                        (
                            kb["name"],
                            kb.get("usage_notes"),
                            kb["tables_json"],
                            kb["files_json"],
                            kb.get("created_by") or "admin",
                            kb.get("updated_by") or "admin",
                            kb["created_at"],
                            kb["updated_at"],
                        ),
                    )
                    kb_id_map[kb["id"]] = cur.lastrowid

            nk_success = 0
            nk_errors = []
            for nk in nk_rows:
                old_kb_id = nk["knowledge_base_id"]
                if old_kb_id not in kb_id_map:
                    continue
                try:
                    with dst_conn.cursor() as cur:
                        cur.execute(
                            "INSERT INTO moi.nl2sql_knowledge "
                            "(knowledge_base_id, knowledge_type, knowledge_key, name, "
                            "knowledge_value, associate_tables, explanation_type, "
                            "created_by, updated_by, created_at, updated_at) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            (
                                kb_id_map[old_kb_id],
                                nk["knowledge_type"],
                                nk["knowledge_key"],
                                nk.get("name"),
                                nk["knowledge_value"],
                                nk["associate_tables"],
                                nk.get("explanation_type"),
                                nk.get("created_by") or "admin",
                                nk.get("updated_by") or "admin",
                                nk["created_at"],
                                nk["updated_at"],
                            ),
                        )
                        nk_success += 1
                except Exception as e:
                    nk_errors.append({"id": nk["id"], "error": str(e)})

            return {
                "ok": True,
                "kb_count": len(kb_id_map),
                "nk_count": nk_success,
                "nk_errors": nk_errors,
                "kb_id_map": {str(k): v for k, v in kb_id_map.items()},
                "overwrite": overwrite,
                "deleted_kb": deleted_kb if overwrite else 0,
                "deleted_nk": deleted_nk if overwrite else 0,
            }
        finally:
            dst_conn.close()
    finally:
        src_conn.close()


# ==================== V2 语义模型 ====================

def get_semantic_model_list(target: str) -> dict:
    """获取语义模型列表"""
    conn = _get_conn(target)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT sm.id, sm.name, sm.description, "
                "CAST(sm.`tables` AS CHAR) AS tables_json, "
                "sm.created_by, sm.updated_by, "
                "sm.created_at, sm.updated_at, "
                "IFNULL(cnt.c, 0) AS entry_count "
                "FROM moi.semantic_models sm "
                "LEFT JOIN ("
                "  SELECT model_id, COUNT(*) AS c "
                "  FROM moi.semantic_entries GROUP BY model_id"
                ") cnt ON sm.id = cnt.model_id "
                "ORDER BY sm.id"
            )
            rows = cur.fetchall()
        return {"ok": True, "data": _serialize(rows)}
    finally:
        conn.close()


def get_semantic_model_detail(target: str, model_id: int) -> dict:
    """获取语义模型详情及其条目"""
    conn = _get_conn(target)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT id, name, description, "
                "CAST(`tables` AS CHAR) AS tables_json, "
                "created_by, updated_by, created_at, updated_at "
                "FROM moi.semantic_models WHERE id = %s",
                (model_id,)
            )
            model = cur.fetchone()
        if not model:
            return {"ok": False, "msg": f"语义模型 {model_id} 不存在"}

        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT id, model_id, kind, key_name, "
                "CAST(`tables` AS CHAR) AS tables_json, "
                "CAST(spec AS CHAR) AS spec_json, "
                "created_by, updated_by, created_at, updated_at "
                "FROM moi.semantic_entries WHERE model_id = %s "
                "ORDER BY kind, key_name",
                (model_id,)
            )
            entries = cur.fetchall()

        return {"ok": True, "model": _serialize(model), "entries": _serialize(entries)}
    finally:
        conn.close()


def create_semantic_model(target: str, data: dict) -> dict:
    """创建语义模型"""
    conn = _get_conn(target)
    try:
        tables_json = data.get("tables_json") or "[]"
        # 计算 table_set_hash
        try:
            tables_list = sorted(json.loads(tables_json))
            table_set_hash = str(hash(tuple(tables_list)))[:16]
        except:
            table_set_hash = ""

        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO moi.semantic_models "
                "(name, description, `tables`, table_set_hash, created_by, updated_by) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    data["name"],
                    data.get("description"),
                    tables_json,
                    table_set_hash,
                    data.get("created_by", "admin"),
                    data.get("updated_by", "admin"),
                )
            )
            new_id = cur.lastrowid
        conn.commit()
        return {"ok": True, "id": new_id}
    finally:
        conn.close()


def update_semantic_model(target: str, model_id: int, data: dict) -> dict:
    """更新语义模型"""
    conn = _get_conn(target)
    try:
        tables_json = data.get("tables_json")
        table_set_hash = None
        if tables_json:
            try:
                tables_list = sorted(json.loads(tables_json))
                table_set_hash = str(hash(tuple(tables_list)))[:16]
            except:
                table_set_hash = ""

        with conn.cursor() as cur:
            if tables_json:
                cur.execute(
                    "UPDATE moi.semantic_models SET name = %s, description = %s, "
                    "`tables` = %s, table_set_hash = %s, updated_by = %s, updated_at = NOW() "
                    "WHERE id = %s",
                    (data["name"], data.get("description"), tables_json, table_set_hash,
                     data.get("updated_by", "admin"), model_id)
                )
            else:
                cur.execute(
                    "UPDATE moi.semantic_models SET name = %s, description = %s, "
                    "updated_by = %s, updated_at = NOW() WHERE id = %s",
                    (data["name"], data.get("description"), data.get("updated_by", "admin"), model_id)
                )
            if cur.rowcount == 0:
                return {"ok": False, "msg": f"语义模型 {model_id} 不存在"}
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


def delete_semantic_model(target: str, model_id: int) -> dict:
    """删除语义模型及其条目"""
    conn = _get_conn(target)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM moi.semantic_entries WHERE model_id = %s", (model_id,))
            entries_deleted = cur.rowcount
            cur.execute("DELETE FROM moi.semantic_models WHERE id = %s", (model_id,))
            if cur.rowcount == 0:
                return {"ok": False, "msg": f"语义模型 {model_id} 不存在"}
        conn.commit()
        return {"ok": True, "entries_deleted": entries_deleted}
    finally:
        conn.close()


def create_semantic_entry(target: str, data: dict) -> dict:
    """创建语义条目"""
    conn = _get_conn(target)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO moi.semantic_entries "
                "(model_id, kind, key_name, `tables`, spec, created_by, updated_by) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    data["model_id"],
                    data["kind"],
                    data["key_name"],
                    data.get("tables_json"),
                    data.get("spec_json", "{}"),
                    data.get("created_by", "admin"),
                    data.get("updated_by", "admin"),
                )
            )
            new_id = cur.lastrowid
        conn.commit()
        return {"ok": True, "id": new_id}
    finally:
        conn.close()


def update_semantic_entry(target: str, entry_id: int, data: dict) -> dict:
    """更新语义条目"""
    conn = _get_conn(target)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE moi.semantic_entries SET kind = %s, key_name = %s, "
                "`tables` = %s, spec = %s, updated_by = %s, updated_at = NOW() "
                "WHERE id = %s",
                (
                    data["kind"],
                    data["key_name"],
                    data.get("tables_json"),
                    data.get("spec_json", "{}"),
                    data.get("updated_by", "admin"),
                    entry_id,
                )
            )
            if cur.rowcount == 0:
                return {"ok": False, "msg": f"语义条目 {entry_id} 不存在"}
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


def delete_semantic_entry(target: str, entry_id: int) -> dict:
    """删除语义条目"""
    conn = _get_conn(target)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM moi.semantic_entries WHERE id = %s", (entry_id,))
            if cur.rowcount == 0:
                return {"ok": False, "msg": f"语义条目 {entry_id} 不存在"}
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


# ==================== 问数过滤条件配置 ====================

def _get_jst_conn(target: str):
    """获取 jst 数据库连接（过滤规则存储在 jst 库）"""
    cfg = DB_CONFIGS.get(target)
    if not cfg:
        raise ValueError(f"未知 target: {target}")
    jst_cfg = dict(cfg)
    jst_cfg["database"] = "jst"
    return pymysql.connect(**jst_cfg)


def get_filter_rules(target: str) -> dict:
    """
    读取 fin_explore_filter_rule_set + fin_explore_filter_rule，
    按 config_key 分组返回，每条 rule_set 附带其下的 rule 列表。
    """
    conn = _get_jst_conn(target)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT id, config_key, config_value, table_name, note, "
                "created_at, updated_at "
                "FROM jst.fin_explore_filter_rule_set ORDER BY config_key, table_name"
            )
            rule_sets = cur.fetchall()

            cur.execute(
                "SELECT id, rule_set_id, field, op, literal_value, "
                "CAST(literal_values AS CHAR) AS literal_values, "
                "value_source, order_idx "
                "FROM jst.fin_explore_filter_rule ORDER BY rule_set_id, order_idx"
            )
            rules = cur.fetchall()

        # 按 rule_set_id 分组 rules
        rules_by_set = {}
        for r in rules:
            sid = r["rule_set_id"]
            if sid not in rules_by_set:
                rules_by_set[sid] = []
            rules_by_set[sid].append(r)

        # 组装结果
        grouped = {}
        for rs in rule_sets:
            key = rs["config_key"]
            if key not in grouped:
                grouped[key] = []
            rs["rules"] = rules_by_set.get(rs["id"], [])
            if isinstance(rs.get("created_at"), datetime):
                rs["created_at"] = rs["created_at"].isoformat(timespec="seconds")
            if isinstance(rs.get("updated_at"), datetime):
                rs["updated_at"] = rs["updated_at"].isoformat(timespec="seconds")
            grouped[key].append(rs)

        return {"ok": True, "data": grouped}
    except Exception as e:
        return {"ok": False, "msg": str(e)}
    finally:
        conn.close()


def get_jst_flat_tables(target: str) -> dict:
    """获取 jst_flat_table 数据库中所有表名"""
    conn = _get_jst_conn(target)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT table_name FROM information_schema.columns "
                "WHERE table_schema = 'jst_flat_table' ORDER BY table_name"
            )
            tables = [r[0] for r in cur.fetchall()]
        return {"ok": True, "tables": tables}
    except Exception as e:
        return {"ok": False, "msg": str(e)}
    finally:
        conn.close()


def get_table_columns(target: str, table_name: str) -> dict:
    """获取 jst_flat_table 中指定表的所有列名"""
    conn = _get_jst_conn(target)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, column_comment "
                "FROM information_schema.columns "
                "WHERE table_schema = 'jst_flat_table' AND table_name = %s "
                "ORDER BY ordinal_position",
                (table_name,),
            )
            cols = [{"name": r[0], "comment": r[1] or ""} for r in cur.fetchall()]
        return {"ok": True, "columns": cols}
    except Exception as e:
        return {"ok": False, "msg": str(e)}
    finally:
        conn.close()


def save_filter_rule(target: str, data: dict) -> dict:
    """
    新增或更新一条过滤规则 (rule_set + rule)。
    data: {id?, config_key, config_value, table_name, note,
           field, op, literal_value?, literal_values?, value_source?}
    """
    conn = _get_jst_conn(target)
    try:
        rule_set_id = data.get("id")
        with conn.cursor() as cur:
            if rule_set_id:
                # 更新 rule_set
                cur.execute(
                    "UPDATE jst.fin_explore_filter_rule_set "
                    "SET config_key=%s, config_value=%s, table_name=%s, note=%s "
                    "WHERE id=%s",
                    (data["config_key"], data["config_value"],
                     data["table_name"], data.get("note", ""),
                     rule_set_id),
                )
                # 删除旧 rules 再重建
                cur.execute(
                    "DELETE FROM jst.fin_explore_filter_rule WHERE rule_set_id=%s",
                    (rule_set_id,),
                )
            else:
                # 新增 rule_set
                cur.execute(
                    "INSERT INTO jst.fin_explore_filter_rule_set "
                    "(config_key, config_value, table_name, note) "
                    "VALUES (%s, %s, %s, %s)",
                    (data["config_key"], data["config_value"],
                     data["table_name"], data.get("note", "")),
                )
                rule_set_id = cur.lastrowid

            # 插入 rule
            field = data.get("field", "")
            if field:
                lit_val = data.get("literal_value") or None
                lit_vals = data.get("literal_values") or None
                if lit_vals and isinstance(lit_vals, list):
                    lit_vals = json.dumps(lit_vals, ensure_ascii=False)
                cur.execute(
                    "INSERT INTO jst.fin_explore_filter_rule "
                    "(rule_set_id, field, op, literal_value, literal_values, "
                    "value_source, order_idx) "
                    "VALUES (%s, %s, %s, %s, %s, %s, 0)",
                    (rule_set_id, field, data.get("op", "eq"),
                     lit_val, lit_vals, data.get("value_source") or None),
                )
            conn.commit()
        return {"ok": True, "id": rule_set_id}
    except Exception as e:
        return {"ok": False, "msg": str(e)}
    finally:
        conn.close()


def delete_filter_rule(target: str, rule_set_id: int) -> dict:
    """删除一条过滤规则 (rule_set + 关联的 rules)"""
    conn = _get_jst_conn(target)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM jst.fin_explore_filter_rule WHERE rule_set_id=%s",
                (rule_set_id,),
            )
            cur.execute(
                "DELETE FROM jst.fin_explore_filter_rule_set WHERE id=%s",
                (rule_set_id,),
            )
            conn.commit()
            if cur.rowcount == 0:
                return {"ok": False, "msg": f"规则 {rule_set_id} 不存在"}
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "msg": str(e)}
    finally:
        conn.close()


def get_semantic_model_export(target: str) -> dict:
    """获取所有语义模型及其条目，用于导出"""
    conn = _get_conn(target)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT id, name, description, "
                "CAST(`tables` AS CHAR) AS tables_json, "
                "created_by, updated_by, created_at, updated_at "
                "FROM moi.semantic_models ORDER BY id"
            )
            models = cur.fetchall()

            cur.execute(
                "SELECT id, model_id, kind, key_name, "
                "CAST(`tables` AS CHAR) AS tables_json, "
                "CAST(spec AS TEXT) AS spec_json, "
                "created_by, updated_by, created_at, updated_at "
                "FROM moi.semantic_entries ORDER BY model_id, kind, key_name"
            )
            entries = cur.fetchall()

        return {
            "ok": True,
            "models": _serialize(models),
            "entries": _serialize(entries),
        }
    except Exception as e:
        return {"ok": False, "msg": str(e)}
    finally:
        conn.close()


def get_system_config(target: str, config_name: str) -> dict:
    """读取 jst.system_config 的某个配置项"""
    conn = _get_jst_conn(target)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT config_value FROM jst.system_config WHERE config_name = %s",
                (config_name,),
            )
            row = cur.fetchone()
        if row is None:
            return {"ok": False, "msg": f"配置项 {config_name} 不存在"}
        return {"ok": True, "config_name": config_name, "config_value": row[0]}
    except Exception as e:
        return {"ok": False, "msg": str(e)}
    finally:
        conn.close()


def set_system_config(target: str, config_name: str, config_value: str) -> dict:
    """更新 jst.system_config 的某个配置项"""
    conn = _get_jst_conn(target)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE jst.system_config SET config_value = %s WHERE config_name = %s",
                (config_value, config_name),
            )
            if cur.rowcount == 0:
                return {"ok": False, "msg": f"配置项 {config_name} 不存在"}
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "msg": str(e)}
    finally:
        conn.close()
