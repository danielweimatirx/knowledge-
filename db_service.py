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
    "portal": dict(
        host="freetier-01.cn-hangzhou.cluster.cn-dev.matrixone.tech",
        port=6001,
        user="ws_bfb9ca8d:qa_manual_20260330185108_x9f3k2:accountadmin",
        password="moi_216a042120beaf5cdf357dfbc7a335a29c4b5d6641feeb598da2f3ccd824d342",
        database="moi", charset="utf8mb4", autocommit=True,
    ),
    "contract_dev": dict(
        host="freetier-01.cn-hangzhou.cluster.cn-dev.matrixone.tech",
        port=6001,
        user="ws_1c82a8eb:moi_core_system",
        password="moi_2d76c2c1a5eb95b160e10e0b1dc47109ded45fbc9ad7641d3adcbd07ce09da78",
        database="moi", charset="utf8mb4", autocommit=True,
    ),
    "contract_prod": dict(
        host="main.mo.shanghai.idc.matrixorigin.cn",
        port=6001,
        user="ws_697656b0:u_dc09869eab244203a2b6707147dc5f6c",
        password="moi_d3f317d3e76788df46ebe3f2e87eaed8bc5f7e469edca63fcec6d972c22cc602",
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
    "portal": "AI Portal",
    "contract_dev": "合同问询",
    "contract_prod": "MOI",
}

WORKSPACE_INFO = {
    "local": {"account": "dump", "host": "127.0.0.1:16001", "database": "moi", "workspace_id": "-", "workspace_name": "local-docker"},
    "remote": {"account": "ws_bf2d347f", "host": "freetier-01.cn-hangzhou.cluster.cn-dev.matrixone.tech:6001", "database": "moi", "workspace_id": "ws_bf2d347f", "workspace_name": "moi_core_system"},
    "portal": {"account": "ws_bfb9ca8d", "host": "freetier-01.cn-hangzhou.cluster.cn-dev.matrixone.tech:6001", "database": "moi", "workspace_id": "6a0d513b-9b28-5c5c-0e5f-145427bde36c", "workspace_name": "qa-manual-ws-20260330185108-x9f3k2"},
    "contract_dev": {"account": "ws_1c82a8eb", "host": "freetier-01.cn-hangzhou.cluster.cn-dev.matrixone.tech:6001", "database": "moi", "workspace_id": "deb09f72-d398-36e6-2dc4-7f4157e7808a", "workspace_name": "合同问询"},
    "contract_prod": {"account": "ws_697656b0", "host": "main.mo.shanghai.idc.matrixorigin.cn:6001", "database": "moi", "workspace_id": "ws_697656b0", "workspace_name": "MOI"},
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
                "CAST(sm.`tables` AS TEXT) AS tables_json, "
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
                "CAST(`tables` AS TEXT) AS tables_json, "
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
                "CAST(`tables` AS TEXT) AS tables_json, "
                "CAST(spec AS TEXT) AS spec_json, "
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
    """获取配置管理（过滤规则 + 系统开关）的数据库连接。
    AI Portal 的配置管理库 jst_qa 实际位于 remote 工作区，需借用 remote 凭证。
    """
    if target == "portal":
        cfg = DB_CONFIGS.get("remote")
        if not cfg:
            raise ValueError("缺少 remote 配置，无法访问 AI Portal 的 jst_qa")
        jst_cfg = dict(cfg)
        jst_cfg["database"] = "jst_qa"
    else:
        cfg = DB_CONFIGS.get(target)
        if not cfg:
            raise ValueError(f"未知 target: {target}")
        jst_cfg = dict(cfg)
        jst_cfg["database"] = "jst"
    try:
        return pymysql.connect(**jst_cfg)
    except Exception as e:
        if "1049" in str(e) or "Unknown database" in str(e):
            raise ValueError(f"工作区 [{WORKSPACE_LABELS.get(target, target)}] 没有配置管理数据库，该功能不可用")
        raise


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
                "FROM fin_explore_filter_rule_set ORDER BY config_key, table_name"
            )
            rule_sets = cur.fetchall()

            cur.execute(
                "SELECT id, rule_set_id, field, op, literal_value, "
                "CAST(literal_values AS CHAR) AS literal_values, "
                "value_source, order_idx, apply_bucket "
                "FROM fin_explore_filter_rule ORDER BY rule_set_id, order_idx"
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
    """获取 jst_flat_table 数据库中所有表名 + 表描述（TABLE_COMMENT）"""
    conn = _get_jst_conn(target)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT TABLE_NAME, TABLE_COMMENT FROM information_schema.tables "
                "WHERE table_schema = 'jst_flat_table' ORDER BY TABLE_NAME"
            )
            tables = [{"name": r[0], "comment": r[1] or ""} for r in cur.fetchall()]
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
    # 防御：必填字段不能为空，避免历史 bug 重现（前端 select 失配会传空值）
    for k in ("config_key", "config_value", "table_name"):
        if not (data.get(k) or "").strip():
            return {"ok": False, "msg": f"{k} 不能为空"}
    conn = _get_jst_conn(target)
    try:
        rule_set_id = data.get("id")
        with conn.cursor() as cur:
            if rule_set_id:
                # 更新 rule_set
                cur.execute(
                    "UPDATE fin_explore_filter_rule_set "
                    "SET config_key=%s, config_value=%s, table_name=%s, note=%s "
                    "WHERE id=%s",
                    (data["config_key"], data["config_value"],
                     data["table_name"], data.get("note", ""),
                     rule_set_id),
                )
                # 删除旧 rules 再重建
                cur.execute(
                    "DELETE FROM fin_explore_filter_rule WHERE rule_set_id=%s",
                    (rule_set_id,),
                )
            else:
                # 新增 rule_set
                cur.execute(
                    "INSERT INTO fin_explore_filter_rule_set "
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
                    "INSERT INTO fin_explore_filter_rule "
                    "(rule_set_id, field, op, literal_value, literal_values, "
                    "value_source, order_idx, apply_bucket) "
                    "VALUES (%s, %s, %s, %s, %s, %s, 0, %s)",
                    (rule_set_id, field, data.get("op", "eq"),
                     lit_val, lit_vals, data.get("value_source") or None,
                     data.get("apply_bucket") or "values"),
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
                "DELETE FROM fin_explore_filter_rule WHERE rule_set_id=%s",
                (rule_set_id,),
            )
            cur.execute(
                "DELETE FROM fin_explore_filter_rule_set WHERE id=%s",
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
                "SELECT config_value FROM system_config WHERE config_name = %s",
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
                "UPDATE system_config SET config_value = %s WHERE config_name = %s",
                (config_value, config_name),
            )
            if cur.rowcount == 0:
                return {"ok": False, "msg": f"配置项 {config_name} 不存在"}
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "msg": str(e)}
    finally:
        conn.close()


def compare_semantic_models(ws_a: str, ws_b: str) -> dict:
    """对比两个工作区的 V2 语义模型，按 name 匹配，并逐条对比 entries"""
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
        def _fetch_models(conn):
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(
                    "SELECT id, name, description, "
                    "CAST(`tables` AS TEXT) AS tables_json, "
                    "CAST(files AS TEXT) AS files_json, "
                    "created_at, updated_at "
                    "FROM moi.semantic_models ORDER BY id"
                )
                rows = cur.fetchall()
            return _serialize(rows)

        def _fetch_entries(conn, model_ids):
            if not model_ids:
                return {}
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                placeholders = ",".join(["%s"] * len(model_ids))
                cur.execute(
                    f"SELECT model_id, kind, key_name, "
                    f"CAST(`tables` AS TEXT) AS tables_json, "
                    f"CAST(spec AS TEXT) AS spec_json "
                    f"FROM moi.semantic_entries WHERE model_id IN ({placeholders}) "
                    f"ORDER BY model_id, kind, key_name",
                    model_ids
                )
                rows = cur.fetchall()
            result = {mid: [] for mid in model_ids}
            for r in rows:
                result[r["model_id"]].append(r)
            return result

        rows_a = _fetch_models(conn_a)
        rows_b = _fetch_models(conn_b)

        map_a = {r["name"]: r for r in rows_a}
        map_b = {r["name"]: r for r in rows_b}
        all_names = sorted(set(map_a.keys()) | set(map_b.keys()))

        both_names = [n for n in all_names if n in map_a and n in map_b]
        both_a_ids = [map_a[n]["id"] for n in both_names]
        both_b_ids = [map_b[n]["id"] for n in both_names]
        entries_a = _fetch_entries(conn_a, both_a_ids)
        entries_b = _fetch_entries(conn_b, both_b_ids)

        only_a = []
        only_b = []
        both = []
        diff = []

        for name in all_names:
            in_a = name in map_a
            in_b = name in map_b
            if in_a and not in_b:
                ma = map_a[name]
                ma["entries_count"] = len(_fetch_entries(conn_a, [ma["id"]]).get(ma["id"], []))
                only_a.append(ma)
            elif in_b and not in_a:
                mb = map_b[name]
                mb["entries_count"] = len(_fetch_entries(conn_b, [mb["id"]]).get(mb["id"], []))
                only_b.append(mb)
            else:
                ma, mb = map_a[name], map_b[name]
                ea = entries_a.get(ma["id"], [])
                eb = entries_b.get(mb["id"], [])
                ma["entries_count"] = len(ea)
                mb["entries_count"] = len(eb)
                both.append({"name": name, "a": ma, "b": mb})

                def entry_key(e):
                    return (e.get("kind") or "", e.get("key_name") or "")

                def _norm(s):
                    """对比时把 None / 'null' / '[]' / '{}' / 空串视作等价的'空'"""
                    if s is None:
                        return ""
                    t = str(s).strip()
                    if t in ("null", "[]", "{}"):
                        return ""
                    return t

                def entry_content(e):
                    return (_norm(e.get("tables_json")), _norm(e.get("spec_json")))

                a_map = {entry_key(i): i for i in ea}
                b_map = {entry_key(i): i for i in eb}
                all_keys = sorted(set(a_map.keys()) | set(b_map.keys()))

                entry_diffs = []
                for key in all_keys:
                    ia = a_map.get(key)
                    ib = b_map.get(key)
                    if ia and not ib:
                        entry_diffs.append({
                            "kind": key[0], "key": key[1],
                            "status": "only_a",
                            "a_spec": ia.get("spec_json") or "",
                            "a_tables": ia.get("tables_json") or "",
                        })
                    elif ib and not ia:
                        entry_diffs.append({
                            "kind": key[0], "key": key[1],
                            "status": "only_b",
                            "b_spec": ib.get("spec_json") or "",
                            "b_tables": ib.get("tables_json") or "",
                        })
                    elif entry_content(ia) != entry_content(ib):
                        entry_diffs.append({
                            "kind": key[0], "key": key[1],
                            "status": "different",
                            "a_spec": ia.get("spec_json") or "",
                            "b_spec": ib.get("spec_json") or "",
                            "a_tables": ia.get("tables_json") or "",
                            "b_tables": ib.get("tables_json") or "",
                            "spec_diff": _norm(ia.get("spec_json")) != _norm(ib.get("spec_json")),
                            "tables_diff": _norm(ia.get("tables_json")) != _norm(ib.get("tables_json")),
                        })

                diffs_detail = {}
                if (ma.get("description") or "").strip() != (mb.get("description") or "").strip():
                    diffs_detail["description"] = {"a": ma.get("description") or "", "b": mb.get("description") or ""}
                if _norm(ma.get("tables_json")) != _norm(mb.get("tables_json")):
                    diffs_detail["tables"] = {"a": ma.get("tables_json"), "b": mb.get("tables_json")}

                if entry_diffs or diffs_detail:
                    diff.append({
                        "name": name,
                        "a_id": ma["id"],
                        "b_id": mb["id"],
                        "a_entries": ma["entries_count"],
                        "b_entries": mb["entries_count"],
                        "diffs": diffs_detail,
                        "entry_diffs": entry_diffs,
                    })

        return {
            "ok": True,
            "a_key": ws_a, "a_label": label_a,
            "a_info": WORKSPACE_INFO.get(ws_a, {}),
            "b_key": ws_b, "b_label": label_b,
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


def apply_v2_model_meta(source: str, target: str, model_name: str, fields: list) -> dict:
    """把 source 工作区的某个语义模型的指定 meta 字段（description / tables）覆盖到 target。
    模型按 name 匹配。
    """
    src_label = WORKSPACE_LABELS.get(source, source)
    dst_label = WORKSPACE_LABELS.get(target, target)
    fields = [f for f in (fields or []) if f in {"description", "tables"}]
    if not fields:
        return {"ok": False, "msg": "未指定要同步的字段"}
    src_conn = dst_conn = None
    try:
        src_conn = _get_conn(source)
        dst_conn = _get_conn(target)
        with src_conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT description, CAST(`tables` AS TEXT) AS tables_txt "
                "FROM moi.semantic_models WHERE name=%s",
                (model_name,)
            )
            src_m = cur.fetchone()
            if not src_m:
                return {"ok": False, "msg": f"源 [{src_label}] 没有模型 [{model_name}]"}
        sets = []
        vals = []
        if "description" in fields:
            sets.append("description=%s")
            vals.append(src_m["description"])
        if "tables" in fields:
            sets.append("`tables`=%s")
            vals.append(src_m["tables_txt"])
        vals.append(model_name)
        with dst_conn.cursor() as cur:
            cur.execute(
                f"UPDATE moi.semantic_models SET {', '.join(sets)} WHERE name=%s",
                vals
            )
            if cur.rowcount == 0:
                return {"ok": False, "msg": f"目标 [{dst_label}] 没有模型 [{model_name}]"}
        return {"ok": True, "fields": fields, "src": src_label, "dst": dst_label}
    except Exception as e:
        return {"ok": False, "msg": str(e)}
    finally:
        if src_conn:
            src_conn.close()
        if dst_conn:
            dst_conn.close()


def apply_v2_entry_to_target(source: str, target: str, model_name: str, kind: str, key_name: str) -> dict:
    """把 source 中 (model_name, kind, key_name) 这条 entry 同步到 target。
    - source 有、target 没有 → 在 target 插入
    - source 有、target 也有 → 在 target 更新
    - source 没有、target 有 → 在 target 删除
    - 都没有 → noop
    """
    src_label = WORKSPACE_LABELS.get(source, source)
    dst_label = WORKSPACE_LABELS.get(target, target)
    src_conn = dst_conn = None
    try:
        src_conn = _get_conn(source)
        dst_conn = _get_conn(target)

        with src_conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute("SELECT id FROM moi.semantic_models WHERE name=%s", (model_name,))
            sm = cur.fetchone()
            src_entry = None
            if sm:
                cur.execute(
                    "SELECT kind, key_name, "
                    "CAST(`tables` AS TEXT) AS tables_txt, "
                    "CAST(spec AS TEXT) AS spec_txt, "
                    "created_by, updated_by, created_at, updated_at "
                    "FROM moi.semantic_entries WHERE model_id=%s AND kind=%s AND key_name=%s",
                    (sm["id"], kind, key_name)
                )
                src_entry = cur.fetchone()

        with dst_conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute("SELECT id FROM moi.semantic_models WHERE name=%s", (model_name,))
            dm = cur.fetchone()
            if not dm:
                return {"ok": False, "msg": f"目标 [{dst_label}] 没有模型 [{model_name}]"}
            cur.execute(
                "SELECT id FROM moi.semantic_entries WHERE model_id=%s AND kind=%s AND key_name=%s",
                (dm["id"], kind, key_name)
            )
            dst_entry = cur.fetchone()

        with dst_conn.cursor() as cur:
            if src_entry and dst_entry:
                cur.execute(
                    "UPDATE moi.semantic_entries SET `tables`=%s, spec=%s, updated_by=%s, updated_at=%s "
                    "WHERE id=%s",
                    (src_entry["tables_txt"], src_entry["spec_txt"],
                     src_entry["updated_by"], src_entry["updated_at"], dst_entry["id"])
                )
                action = "updated"
            elif src_entry and not dst_entry:
                cur.execute(
                    "INSERT INTO moi.semantic_entries "
                    "(model_id, kind, key_name, `tables`, spec, "
                    "created_by, updated_by, created_at, updated_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (dm["id"], kind, key_name,
                     src_entry["tables_txt"], src_entry["spec_txt"],
                     src_entry["created_by"], src_entry["updated_by"],
                     src_entry["created_at"], src_entry["updated_at"])
                )
                action = "inserted"
            elif not src_entry and dst_entry:
                cur.execute("DELETE FROM moi.semantic_entries WHERE id=%s", (dst_entry["id"],))
                action = "deleted"
            else:
                return {"ok": True, "action": "noop", "src": src_label, "dst": dst_label}
        return {"ok": True, "action": action, "src": src_label, "dst": dst_label}
    except Exception as e:
        return {"ok": False, "msg": str(e)}
    finally:
        if src_conn:
            src_conn.close()
        if dst_conn:
            dst_conn.close()


def migrate_semantic_models(source: str, target: str, overwrite: bool = False) -> dict:
    """迁移V2语义模型（semantic_models + semantic_entries）"""
    src_label = WORKSPACE_LABELS.get(source, source)
    dst_label = WORKSPACE_LABELS.get(target, target)
    try:
        src_conn = _get_conn(source)
    except Exception as e:
        return {"ok": False, "msg": f"连接源 [{src_label}] 失败: {e}"}
    try:
        dst_conn = _get_conn(target)
    except Exception as e:
        src_conn.close()
        return {"ok": False, "msg": f"连接目标 [{dst_label}] 失败: {e}"}
    try:
        # 读取源
        with src_conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT id, name, description, CAST(`tables` AS TEXT) AS tables_txt, "
                "CAST(files AS TEXT) AS files_txt, table_set_hash, "
                "created_by, updated_by, created_at, updated_at "
                "FROM moi.semantic_models ORDER BY id"
            )
            models = cur.fetchall()
            cur.execute(
                "SELECT id, model_id, kind, key_name, "
                "CAST(`tables` AS TEXT) AS tables_txt, "
                "CAST(spec AS TEXT) AS spec_txt, "
                "created_by, updated_by, created_at, updated_at "
                "FROM moi.semantic_entries ORDER BY id"
            )
            entries = cur.fetchall()

        # 覆盖模式
        deleted_m = deleted_e = 0
        if overwrite:
            with dst_conn.cursor() as cur:
                cur.execute("DELETE FROM moi.semantic_entries")
                deleted_e = cur.rowcount
                cur.execute("DELETE FROM moi.semantic_models")
                deleted_m = cur.rowcount

        # 写入
        model_id_map = {}
        for m in models:
            with dst_conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO moi.semantic_models "
                    "(name, description, `tables`, files, table_set_hash, "
                    "created_by, updated_by, created_at, updated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (m['name'], m['description'], m['tables_txt'], m['files_txt'],
                     m['table_set_hash'], m['created_by'], m['updated_by'],
                     m['created_at'], m['updated_at'])
                )
                model_id_map[m['id']] = cur.lastrowid

        e_ok = 0
        for e in entries:
            new_mid = model_id_map.get(e['model_id'])
            if not new_mid:
                continue
            with dst_conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO moi.semantic_entries "
                    "(model_id, kind, key_name, `tables`, spec, "
                    "created_by, updated_by, created_at, updated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (new_mid, e['kind'], e['key_name'], e['tables_txt'],
                     e['spec_txt'], e['created_by'], e['updated_by'],
                     e['created_at'], e['updated_at'])
                )
                e_ok += 1

        return {
            "ok": True,
            "models_count": len(model_id_map),
            "entries_count": e_ok,
            "deleted_models": deleted_m,
            "deleted_entries": deleted_e,
        }
    except Exception as e:
        return {"ok": False, "msg": str(e)}
    finally:
        src_conn.close()
        dst_conn.close()


def _norm_jsonish(s):
    """对比时把 None / 'null' / '[]' / '{}' / 空串视作等价的'空'"""
    if s is None:
        return ""
    t = str(s).strip()
    if t in ("null", "[]", "{}"):
        return ""
    return t


def compare_filter_rules(ws_a: str, ws_b: str) -> dict:
    """对比两个工作区的过滤条件配置（fin_explore_filter_rule_set + fin_explore_filter_rule）。
    rule_set 按 (config_key, config_value, table_name) 匹配；rule_set 内 rule 按 (field, op) 匹配。
    """
    label_a = WORKSPACE_LABELS.get(ws_a, ws_a)
    label_b = WORKSPACE_LABELS.get(ws_b, ws_b)
    conn_a = conn_b = None
    try:
        conn_a = _get_jst_conn(ws_a)
    except Exception as e:
        return {"ok": False, "msg": f"连接工作区 [{label_a}] 失败: {e}"}
    try:
        conn_b = _get_jst_conn(ws_b)
    except Exception as e:
        conn_a.close()
        return {"ok": False, "msg": f"连接工作区 [{label_b}] 失败: {e}"}
    try:
        def _fetch_sets(conn):
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(
                    "SELECT id, config_key, config_value, table_name, note "
                    "FROM fin_explore_filter_rule_set ORDER BY id"
                )
                return cur.fetchall()

        def _fetch_rules(conn, set_ids):
            if not set_ids:
                return {}
            with conn.cursor(pymysql.cursors.DictCursor) as cur:
                placeholders = ",".join(["%s"] * len(set_ids))
                cur.execute(
                    f"SELECT rule_set_id, field, op, literal_value, "
                    f"CAST(literal_values AS CHAR) AS literal_values, "
                    f"value_source, order_idx, apply_bucket "
                    f"FROM fin_explore_filter_rule WHERE rule_set_id IN ({placeholders}) "
                    f"ORDER BY rule_set_id, order_idx",
                    set_ids
                )
                rows = cur.fetchall()
            res = {sid: [] for sid in set_ids}
            for r in rows:
                res[r["rule_set_id"]].append(r)
            return res

        sets_a = _fetch_sets(conn_a)
        sets_b = _fetch_sets(conn_b)

        def set_key(s):
            return (s["config_key"] or "", s["config_value"] or "", s["table_name"] or "")

        map_a = {set_key(s): s for s in sets_a}
        map_b = {set_key(s): s for s in sets_b}
        all_keys = sorted(set(map_a.keys()) | set(map_b.keys()))

        both_keys = [k for k in all_keys if k in map_a and k in map_b]
        rules_a = _fetch_rules(conn_a, [map_a[k]["id"] for k in both_keys])
        rules_b = _fetch_rules(conn_b, [map_b[k]["id"] for k in both_keys])

        only_a, only_b, diff = [], [], []
        same_count = 0

        def rule_key(r):
            return (r.get("field") or "", r.get("op") or "")

        def rule_content(r):
            return (
                _norm_jsonish(r.get("literal_value")),
                _norm_jsonish(r.get("literal_values")),
                _norm_jsonish(r.get("value_source")),
                r.get("order_idx") or 0,
                _norm_jsonish(r.get("apply_bucket")),
            )

        for k in all_keys:
            if k in map_a and k not in map_b:
                sa = map_a[k]
                sa["rules_count"] = len(_fetch_rules(conn_a, [sa["id"]]).get(sa["id"], []))
                only_a.append(sa)
            elif k in map_b and k not in map_a:
                sb = map_b[k]
                sb["rules_count"] = len(_fetch_rules(conn_b, [sb["id"]]).get(sb["id"], []))
                only_b.append(sb)
            else:
                sa, sb = map_a[k], map_b[k]
                ra = rules_a.get(sa["id"], [])
                rb = rules_b.get(sb["id"], [])
                a_rule_map = {rule_key(r): r for r in ra}
                b_rule_map = {rule_key(r): r for r in rb}
                all_rule_keys = sorted(set(a_rule_map.keys()) | set(b_rule_map.keys()))

                rule_diffs = []
                for rk in all_rule_keys:
                    ia = a_rule_map.get(rk)
                    ib = b_rule_map.get(rk)
                    if ia and not ib:
                        rule_diffs.append({
                            "field": rk[0], "op": rk[1],
                            "status": "only_a",
                            "a_literal": ia.get("literal_value") or "",
                            "a_literals": ia.get("literal_values") or "",
                            "a_source": ia.get("value_source") or "",
                            "a_order": ia.get("order_idx") or 0,
                            "a_bucket": ia.get("apply_bucket") or "",
                        })
                    elif ib and not ia:
                        rule_diffs.append({
                            "field": rk[0], "op": rk[1],
                            "status": "only_b",
                            "b_literal": ib.get("literal_value") or "",
                            "b_literals": ib.get("literal_values") or "",
                            "b_source": ib.get("value_source") or "",
                            "b_order": ib.get("order_idx") or 0,
                            "b_bucket": ib.get("apply_bucket") or "",
                        })
                    elif rule_content(ia) != rule_content(ib):
                        rule_diffs.append({
                            "field": rk[0], "op": rk[1],
                            "status": "different",
                            "a_literal": ia.get("literal_value") or "",
                            "b_literal": ib.get("literal_value") or "",
                            "a_literals": ia.get("literal_values") or "",
                            "b_literals": ib.get("literal_values") or "",
                            "a_source": ia.get("value_source") or "",
                            "b_source": ib.get("value_source") or "",
                            "a_order": ia.get("order_idx") or 0,
                            "b_order": ib.get("order_idx") or 0,
                            "a_bucket": ia.get("apply_bucket") or "",
                            "b_bucket": ib.get("apply_bucket") or "",
                            "literal_diff": _norm_jsonish(ia.get("literal_value")) != _norm_jsonish(ib.get("literal_value")),
                            "literals_diff": _norm_jsonish(ia.get("literal_values")) != _norm_jsonish(ib.get("literal_values")),
                            "source_diff": _norm_jsonish(ia.get("value_source")) != _norm_jsonish(ib.get("value_source")),
                            "order_diff": (ia.get("order_idx") or 0) != (ib.get("order_idx") or 0),
                            "bucket_diff": _norm_jsonish(ia.get("apply_bucket")) != _norm_jsonish(ib.get("apply_bucket")),
                        })

                meta_diffs = {}
                if (sa.get("note") or "").strip() != (sb.get("note") or "").strip():
                    meta_diffs["note"] = {"a": sa.get("note") or "", "b": sb.get("note") or ""}

                if rule_diffs or meta_diffs:
                    diff.append({
                        "config_key": k[0],
                        "config_value": k[1],
                        "table_name": k[2],
                        "a_id": sa["id"],
                        "b_id": sb["id"],
                        "a_rules": len(ra),
                        "b_rules": len(rb),
                        "diffs": meta_diffs,
                        "rule_diffs": rule_diffs,
                    })
                else:
                    same_count += 1

        return {
            "ok": True,
            "a_key": ws_a, "a_label": label_a,
            "b_key": ws_b, "b_label": label_b,
            "a_total": len(sets_a),
            "b_total": len(sets_b),
            "only_a": only_a,
            "only_b": only_b,
            "same_count": same_count,
            "diff": diff,
        }
    finally:
        if conn_a:
            conn_a.close()
        if conn_b:
            conn_b.close()


def migrate_filter_rules(source: str, target: str, overwrite: bool = False) -> dict:
    """全量迁移过滤条件配置（fin_explore_filter_rule_set + fin_explore_filter_rule）"""
    src_label = WORKSPACE_LABELS.get(source, source)
    dst_label = WORKSPACE_LABELS.get(target, target)
    src_conn = dst_conn = None
    try:
        try:
            src_conn = _get_jst_conn(source)
        except Exception as e:
            return {"ok": False, "msg": f"连接源 [{src_label}] 失败: {e}"}
        try:
            dst_conn = _get_jst_conn(target)
        except Exception as e:
            return {"ok": False, "msg": f"连接目标 [{dst_label}] 失败: {e}"}

        with src_conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT id, config_key, config_value, table_name, note "
                "FROM fin_explore_filter_rule_set ORDER BY id"
            )
            sets = cur.fetchall()
            cur.execute(
                "SELECT id, rule_set_id, field, op, literal_value, "
                "CAST(literal_values AS CHAR) AS literal_values, "
                "value_source, order_idx, apply_bucket "
                "FROM fin_explore_filter_rule ORDER BY id"
            )
            rules = cur.fetchall()

        deleted_sets = deleted_rules = 0
        if overwrite:
            with dst_conn.cursor() as cur:
                cur.execute("DELETE FROM fin_explore_filter_rule")
                deleted_rules = cur.rowcount
                cur.execute("DELETE FROM fin_explore_filter_rule_set")
                deleted_sets = cur.rowcount

        set_id_map = {}
        for s in sets:
            with dst_conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO fin_explore_filter_rule_set "
                    "(config_key, config_value, table_name, note) "
                    "VALUES (%s, %s, %s, %s)",
                    (s["config_key"], s["config_value"], s["table_name"], s.get("note") or "")
                )
                set_id_map[s["id"]] = cur.lastrowid

        r_ok = 0
        for r in rules:
            new_sid = set_id_map.get(r["rule_set_id"])
            if not new_sid:
                continue
            with dst_conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO fin_explore_filter_rule "
                    "(rule_set_id, field, op, literal_value, literal_values, "
                    "value_source, order_idx, apply_bucket) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (new_sid, r["field"], r["op"],
                     r.get("literal_value"), r.get("literal_values"),
                     r.get("value_source"), r.get("order_idx") or 0,
                     r.get("apply_bucket") or "values")
                )
                r_ok += 1

        return {
            "ok": True,
            "sets_count": len(set_id_map),
            "rules_count": r_ok,
            "deleted_sets": deleted_sets,
            "deleted_rules": deleted_rules,
        }
    except Exception as e:
        return {"ok": False, "msg": str(e)}
    finally:
        if src_conn:
            src_conn.close()
        if dst_conn:
            dst_conn.close()


def apply_filter_rule_set_meta(source: str, target: str,
                               config_key: str, config_value: str, table_name: str,
                               fields: list) -> dict:
    """同步 rule_set 的 meta 字段（目前仅 note）"""
    fields = [f for f in (fields or []) if f in {"note"}]
    if not fields:
        return {"ok": False, "msg": "未指定要同步的字段"}
    src_label = WORKSPACE_LABELS.get(source, source)
    dst_label = WORKSPACE_LABELS.get(target, target)
    src_conn = dst_conn = None
    try:
        src_conn = _get_jst_conn(source)
        dst_conn = _get_jst_conn(target)
        with src_conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT note FROM fin_explore_filter_rule_set "
                "WHERE config_key=%s AND config_value=%s AND table_name=%s",
                (config_key, config_value, table_name)
            )
            sa = cur.fetchone()
            if not sa:
                return {"ok": False, "msg": f"源 [{src_label}] 没有该规则集"}
        sets = []
        vals = []
        if "note" in fields:
            sets.append("note=%s")
            vals.append(sa.get("note") or "")
        vals.extend([config_key, config_value, table_name])
        with dst_conn.cursor() as cur:
            cur.execute(
                f"UPDATE fin_explore_filter_rule_set SET {', '.join(sets)} "
                f"WHERE config_key=%s AND config_value=%s AND table_name=%s",
                vals
            )
            if cur.rowcount == 0:
                return {"ok": False, "msg": f"目标 [{dst_label}] 没有该规则集"}
        return {"ok": True, "fields": fields}
    except Exception as e:
        return {"ok": False, "msg": str(e)}
    finally:
        if src_conn:
            src_conn.close()
        if dst_conn:
            dst_conn.close()


def apply_filter_rule_set_full(source: str, target: str,
                               config_key: str, config_value: str, table_name: str) -> dict:
    """同步整个规则集（rule_set + 全部 rules）。
    - source 有、target 没有 → target 新建 rule_set 和所有 rules
    - source 有、target 也有 → 更新 target 的 note，并用 source 的 rules 替换 target 的 rules
    - source 没有、target 有 → target 删除 rule_set 和它的 rules
    - 都没有 → noop
    """
    src_label = WORKSPACE_LABELS.get(source, source)
    dst_label = WORKSPACE_LABELS.get(target, target)
    src_conn = dst_conn = None
    try:
        src_conn = _get_jst_conn(source)
        dst_conn = _get_jst_conn(target)

        with src_conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT id, note FROM fin_explore_filter_rule_set "
                "WHERE config_key=%s AND config_value=%s AND table_name=%s",
                (config_key, config_value, table_name)
            )
            ss = cur.fetchone()
            src_rules = []
            if ss:
                cur.execute(
                    "SELECT field, op, literal_value, "
                    "CAST(literal_values AS CHAR) AS literal_values, "
                    "value_source, order_idx, apply_bucket "
                    "FROM fin_explore_filter_rule WHERE rule_set_id=%s ORDER BY order_idx",
                    (ss["id"],)
                )
                src_rules = cur.fetchall()

        with dst_conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT id FROM fin_explore_filter_rule_set "
                "WHERE config_key=%s AND config_value=%s AND table_name=%s",
                (config_key, config_value, table_name)
            )
            ds = cur.fetchone()

        if not ss and not ds:
            return {"ok": True, "action": "noop"}

        if ss and not ds:
            # 新建 rule_set + rules
            with dst_conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO fin_explore_filter_rule_set "
                    "(config_key, config_value, table_name, note) "
                    "VALUES (%s, %s, %s, %s)",
                    (config_key, config_value, table_name, ss.get("note") or "")
                )
                new_sid = cur.lastrowid
                for r in src_rules:
                    cur.execute(
                        "INSERT INTO fin_explore_filter_rule "
                        "(rule_set_id, field, op, literal_value, literal_values, "
                        "value_source, order_idx, apply_bucket) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                        (new_sid, r["field"], r["op"],
                         r.get("literal_value"), r.get("literal_values"),
                         r.get("value_source"), r.get("order_idx") or 0,
                         r.get("apply_bucket") or "values")
                    )
            return {"ok": True, "action": "inserted", "rules_count": len(src_rules)}

        if not ss and ds:
            # 删除 rule_set + 它的 rules
            with dst_conn.cursor() as cur:
                cur.execute("DELETE FROM fin_explore_filter_rule WHERE rule_set_id=%s", (ds["id"],))
                deleted_rules = cur.rowcount
                cur.execute("DELETE FROM fin_explore_filter_rule_set WHERE id=%s", (ds["id"],))
            return {"ok": True, "action": "deleted", "deleted_rules": deleted_rules}

        # ss and ds: 同步 note + 用 src 的 rules 替换 dst 的 rules
        with dst_conn.cursor() as cur:
            cur.execute(
                "UPDATE fin_explore_filter_rule_set SET note=%s WHERE id=%s",
                (ss.get("note") or "", ds["id"])
            )
            cur.execute("DELETE FROM fin_explore_filter_rule WHERE rule_set_id=%s", (ds["id"],))
            old_rules = cur.rowcount
            for r in src_rules:
                cur.execute(
                    "INSERT INTO fin_explore_filter_rule "
                    "(rule_set_id, field, op, literal_value, literal_values, "
                    "value_source, order_idx, apply_bucket) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (ds["id"], r["field"], r["op"],
                     r.get("literal_value"), r.get("literal_values"),
                     r.get("value_source"), r.get("order_idx") or 0,
                     r.get("apply_bucket") or "values")
                )
        return {"ok": True, "action": "replaced",
                "rules_count": len(src_rules), "deleted_rules": old_rules}
    except Exception as e:
        return {"ok": False, "msg": str(e)}
    finally:
        if src_conn:
            src_conn.close()
        if dst_conn:
            dst_conn.close()


def apply_filter_rule(source: str, target: str,
                      config_key: str, config_value: str, table_name: str,
                      field: str, op: str) -> dict:
    """同步单条 rule（按 rule_set 三元组定位 + (field, op) 匹配 rule）。
    src 有则 upsert，src 无则 dst 删除。"""
    src_label = WORKSPACE_LABELS.get(source, source)
    dst_label = WORKSPACE_LABELS.get(target, target)
    src_conn = dst_conn = None
    try:
        src_conn = _get_jst_conn(source)
        dst_conn = _get_jst_conn(target)
        with src_conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT id FROM fin_explore_filter_rule_set "
                "WHERE config_key=%s AND config_value=%s AND table_name=%s",
                (config_key, config_value, table_name)
            )
            ss = cur.fetchone()
            src_rule = None
            if ss:
                cur.execute(
                    "SELECT field, op, literal_value, "
                    "CAST(literal_values AS CHAR) AS literal_values, "
                    "value_source, order_idx, apply_bucket FROM fin_explore_filter_rule "
                    "WHERE rule_set_id=%s AND field=%s AND op=%s",
                    (ss["id"], field, op)
                )
                src_rule = cur.fetchone()

        with dst_conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT id FROM fin_explore_filter_rule_set "
                "WHERE config_key=%s AND config_value=%s AND table_name=%s",
                (config_key, config_value, table_name)
            )
            ds = cur.fetchone()
            if not ds:
                return {"ok": False, "msg": f"目标 [{dst_label}] 没有该规则集"}
            cur.execute(
                "SELECT id FROM fin_explore_filter_rule "
                "WHERE rule_set_id=%s AND field=%s AND op=%s",
                (ds["id"], field, op)
            )
            dst_rule = cur.fetchone()

        with dst_conn.cursor() as cur:
            if src_rule and dst_rule:
                cur.execute(
                    "UPDATE fin_explore_filter_rule SET literal_value=%s, literal_values=%s, "
                    "value_source=%s, order_idx=%s, apply_bucket=%s WHERE id=%s",
                    (src_rule.get("literal_value"), src_rule.get("literal_values"),
                     src_rule.get("value_source"), src_rule.get("order_idx") or 0,
                     src_rule.get("apply_bucket") or "values",
                     dst_rule["id"])
                )
                action = "updated"
            elif src_rule and not dst_rule:
                cur.execute(
                    "INSERT INTO fin_explore_filter_rule "
                    "(rule_set_id, field, op, literal_value, literal_values, "
                    "value_source, order_idx, apply_bucket) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (ds["id"], field, op, src_rule.get("literal_value"),
                     src_rule.get("literal_values"), src_rule.get("value_source"),
                     src_rule.get("order_idx") or 0,
                     src_rule.get("apply_bucket") or "values")
                )
                action = "inserted"
            elif not src_rule and dst_rule:
                cur.execute("DELETE FROM fin_explore_filter_rule WHERE id=%s", (dst_rule["id"],))
                action = "deleted"
            else:
                return {"ok": True, "action": "noop"}
        return {"ok": True, "action": action}
    except Exception as e:
        return {"ok": False, "msg": str(e)}
    finally:
        if src_conn:
            src_conn.close()
        if dst_conn:
            dst_conn.close()


def migrate_system_config(source: str, target: str) -> dict:
    """迁移 jst.system_config 全表"""
    src_label = WORKSPACE_LABELS.get(source, source)
    dst_label = WORKSPACE_LABELS.get(target, target)
    try:
        src_conn = _get_jst_conn(source)
    except Exception as e:
        return {"ok": False, "msg": f"连接源 [{src_label}] jst 失败: {e}"}
    try:
        dst_conn = _get_jst_conn(target)
    except Exception as e:
        src_conn.close()
        return {"ok": False, "msg": f"连接目标 [{dst_label}] jst 失败: {e}"}
    try:
        with src_conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute("SELECT config_name, config_value FROM system_config")
            rows = cur.fetchall()

        updated = 0
        for r in rows:
            with dst_conn.cursor() as cur:
                cur.execute(
                    "UPDATE system_config SET config_value = %s WHERE config_name = %s",
                    (r['config_value'], r['config_name'])
                )
                if cur.rowcount > 0:
                    updated += 1

        return {"ok": True, "synced": updated, "total": len(rows)}
    except Exception as e:
        return {"ok": False, "msg": str(e)}
    finally:
        src_conn.close()
        dst_conn.close()
