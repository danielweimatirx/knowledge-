"""
NL2SQL 知识库迁移脚本：旧工作区 (catalog_service) → 新工作区 (moi-core)

用法:
  python migrate_nl2sql.py --dry-run          # 空跑，不写入
  python migrate_nl2sql.py --target local     # 写入本地 Docker MO（测试）
  python migrate_nl2sql.py --target remote    # 写入远程新工作区（正式迁移）
"""
import argparse
import json
import sys

import pymysql

# ==================== 连接配置 ====================

OLD_REMOTE = dict(
    host="freetier-01.cn-hangzhou.cluster.cn-dev.matrixone.tech",
    port=6001,
    user="019c97df-8bc3-7274-96bc-f3e3938489a8:admin:accountadmin",
    password="admin123",
    database="moi",
    charset="utf8mb4",
    autocommit=True,
)

NEW_REMOTE = dict(
    host="freetier-01.cn-hangzhou.cluster.cn-dev.matrixone.tech",
    port=6001,
    user="ws_bf2d347f:moi_core_system:accountadmin",
    password="moi_2d76c2c1a5eb95b160e10e0b1dc47109ded45fbc9ad7641d3adcbd07ce09da78",
    database="moi",
    charset="utf8mb4",
    autocommit=True,
)

NEW_LOCAL = dict(
    host="127.0.0.1",
    port=16001,
    user="dump",
    password="111",
    charset="utf8mb4",
    autocommit=True,
)

# ==================== 表 ID 映射 ====================

# 新工作区 catalog: db_name=jst_flat_table, database_id=1, catalog_id=10001
NEW_DB_NAME = "jst_flat_table"
NEW_DATABASE_ID = 1
NEW_CATALOG_ID = 10001

# 表名 → 新 catalog table_id
TABLE_ID_MAP = {
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

# 跳过的旧 knowledge_base id — 不再需要跳过，jst_flat_table 包含 inventory_aging_pc
SKIP_KB_IDS = set()

# ==================== DDL ====================

DDL_KNOWLEDGE_BASE = """
CREATE TABLE IF NOT EXISTS knowledge_base (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键',
    name VARCHAR(255) NOT NULL COMMENT '知识库名称',
    usage_notes TEXT NULL COMMENT '用途说明',
    `tables` JSON NULL COMMENT '关联表信息',
    files JSON NULL COMMENT '关联文件信息',
    created_by VARCHAR(64) NOT NULL COMMENT '创建人',
    updated_by VARCHAR(64) NOT NULL COMMENT '更新人',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_knowledge_base_name (name)
) COMMENT='知识库定义表'
"""

DDL_NL2SQL_KNOWLEDGE = """
CREATE TABLE IF NOT EXISTS nl2sql_knowledge (
    id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键',
    knowledge_base_id BIGINT NOT NULL COMMENT '知识库ID',
    knowledge_type VARCHAR(64) NOT NULL COMMENT '知识类型',
    knowledge_key TEXT NOT NULL COMMENT '知识Key',
    name VARCHAR(255) NULL COMMENT '知识名称',
    knowledge_value JSON NULL COMMENT '知识值',
    associate_tables JSON NULL COMMENT '关联表',
    explanation_type VARCHAR(64) NULL COMMENT '解释类型',
    created_by VARCHAR(64) NOT NULL COMMENT '创建人',
    updated_by VARCHAR(64) NOT NULL COMMENT '更新人',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_nl2sql_kb (knowledge_base_id),
    INDEX idx_nl2sql_type (knowledge_type)
) COMMENT='NL2SQL 知识条目表'
"""


# ==================== 读取旧库 ====================

def read_old_knowledge_bases(conn):
    """读取旧库 knowledge_base 全部记录"""
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            "SELECT id, name, usage_notes, "
            "CAST(`tables` AS CHAR) as tables_str, "
            "CAST(files AS CHAR) as files_str, "
            "created_by, updated_by, created_at, updated_at "
            "FROM moi.knowledge_base ORDER BY id"
        )
        rows = cur.fetchall()
    for r in rows:
        r["tables_parsed"] = json.loads(r["tables_str"]) if r["tables_str"] else []
        r["files_parsed"] = json.loads(r["files_str"]) if r["files_str"] else {}
    return rows


def read_old_nl2sql_knowledge(conn):
    """读取旧库 nl2sql_knowledge 全部记录"""
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            "SELECT id, knowledge_base_id, type, `key`, "
            "CAST(`value` AS CHAR) as value_str, "
            "CAST(meta AS CHAR) as meta_str, "
            "created_at, updated_at "
            "FROM moi.nl2sql_knowledge ORDER BY id"
        )
        rows = cur.fetchall()
    for r in rows:
        r["value_parsed"] = json.loads(r["value_str"]) if r["value_str"] else []
        r["meta_parsed"] = json.loads(r["meta_str"]) if r["meta_str"] else {}
    return rows


# ==================== 转换 ====================

def convert_tables_json(old_tables):
    """
    旧格式: [{"db_name": "jst_flat_table", "parent": [...], "table_ids": ["91"], "table_name": ["revenue_cost"]}]
    新格式: [{"db_name": "jst_flat_table_clone_moi_core", "table_ids": [13], "table_names": ["revenue_cost"], "parents": ["catalog-10001", "database-5"]}]
    """
    if not old_tables:
        return None

    new_entries = []
    for entry in old_tables:
        old_table_names = entry.get("table_name", [])
        new_table_ids = []
        valid_table_names = []

        for tname in old_table_names:
            new_id = TABLE_ID_MAP.get(tname)
            if new_id is not None:
                new_table_ids.append(new_id)
                valid_table_names.append(tname)
            else:
                print(f"  [WARN] 表 '{tname}' 不在新 catalog 中，跳过")

        if not valid_table_names:
            continue

        new_entries.append({
            "db_name": NEW_DB_NAME,
            "table_ids": new_table_ids,
            "table_names": valid_table_names,
            "parents": [f"catalog-{NEW_CATALOG_ID}", f"database-{NEW_DATABASE_ID}"],
        })

    return json.dumps(new_entries, ensure_ascii=False) if new_entries else None


# ==================== 写入新库 ====================

def setup_local_database(conn):
    """本地测试时创建数据库和表"""
    with conn.cursor() as cur:
        cur.execute("CREATE DATABASE IF NOT EXISTS moi")
        cur.execute("USE moi")
        cur.execute(DDL_KNOWLEDGE_BASE)
        cur.execute(DDL_NL2SQL_KNOWLEDGE)
    print("[OK] 本地数据库和表已创建")


def clear_target(conn):
    """清空新库目标表"""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM moi.nl2sql_knowledge")
        cur.execute("DELETE FROM moi.knowledge_base")
    print("[OK] 已清空目标表")


def insert_knowledge_bases(conn, kb_rows, dry_run=False):
    """
    写入 knowledge_base，返回 old_id → new_id 映射。
    """
    id_map = {}
    for kb in kb_rows:
        if kb["id"] in SKIP_KB_IDS:
            print(f"  [SKIP] kb_id={kb['id']} name='{kb['name']}' (关联表不在新 catalog)")
            continue

        new_tables_json = convert_tables_json(kb["tables_parsed"])
        files_json = json.dumps(kb["files_parsed"], ensure_ascii=False) if kb["files_parsed"] else None

        created_by = kb.get("created_by") or "admin"
        updated_by = kb.get("updated_by") or "admin"

        if dry_run:
            print(f"  [DRY] kb_id={kb['id']} name='{kb['name']}'")
            print(f"         tables: {new_tables_json}")
            id_map[kb["id"]] = kb["id"]  # dry-run 用原 ID 占位
            continue

        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO moi.knowledge_base "
                "(name, usage_notes, `tables`, files, created_by, updated_by, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    kb["name"],
                    kb.get("usage_notes"),
                    new_tables_json,
                    files_json,
                    created_by,
                    updated_by,
                    kb["created_at"],
                    kb["updated_at"],
                ),
            )
            new_id = cur.lastrowid
            id_map[kb["id"]] = new_id
            print(f"  [OK] old={kb['id']} → new={new_id} name='{kb['name']}'")

    return id_map


def insert_nl2sql_knowledge(conn, nk_rows, kb_id_map, dry_run=False):
    """写入 nl2sql_knowledge"""
    success = 0
    skipped = 0
    errors = []

    for nk in nk_rows:
        old_kb_id = nk["knowledge_base_id"]
        if old_kb_id not in kb_id_map:
            skipped += 1
            continue

        new_kb_id = kb_id_map[old_kb_id]
        knowledge_type = nk["type"]
        knowledge_key = nk["key"]
        knowledge_value = nk["value_str"]

        meta = nk["meta_parsed"]
        explanation_type = meta.get("explanation_type")
        associate_tables_raw = meta.get("associate_tables")
        name = meta.get("name")

        if isinstance(associate_tables_raw, list) and associate_tables_raw:
            associate_tables = json.dumps(associate_tables_raw, ensure_ascii=False)
        elif isinstance(associate_tables_raw, str) and associate_tables_raw:
            associate_tables = json.dumps([associate_tables_raw], ensure_ascii=False)
        else:
            associate_tables = None

        if dry_run:
            key_preview = knowledge_key[:80].replace("\n", " ")
            print(f"  [DRY] nk_id={nk['id']} kb={old_kb_id}→{new_kb_id} type={knowledge_type} key='{key_preview}...'")
            success += 1
            continue

        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO moi.nl2sql_knowledge "
                    "(knowledge_base_id, knowledge_type, knowledge_key, name, "
                    "knowledge_value, associate_tables, explanation_type, "
                    "created_by, updated_by, created_at, updated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        new_kb_id,
                        knowledge_type,
                        knowledge_key,
                        name,
                        knowledge_value,
                        associate_tables,
                        explanation_type,
                        "admin",
                        "admin",
                        nk["created_at"],
                        nk["updated_at"],
                    ),
                )
                success += 1
        except Exception as e:
            errors.append((nk["id"], str(e)))

    print(f"[结果] 成功={success}, 跳过={skipped}, 失败={len(errors)}")
    for eid, err in errors:
        print(f"  [FAIL] id={eid}: {err}")

    return success, skipped, errors


# ==================== 验证 ====================

def verify(conn):
    """验证迁移结果"""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM moi.knowledge_base")
        kb_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM moi.nl2sql_knowledge")
        nk_count = cur.fetchone()[0]

        print(f"\n{'='*50}")
        print(f"knowledge_base: {kb_count} 条")
        print(f"nl2sql_knowledge: {nk_count} 条")

        cur.execute(
            "SELECT kb.id, kb.name, COUNT(nk.id) as cnt "
            "FROM moi.knowledge_base kb "
            "LEFT JOIN moi.nl2sql_knowledge nk ON kb.id = nk.knowledge_base_id "
            "GROUP BY kb.id, kb.name ORDER BY kb.id"
        )
        print(f"\n各知识库明细:")
        for row in cur.fetchall():
            print(f"  id={row[0]}, name='{row[1]}', 知识条目={row[2]}")

        # 验证 tables JSON 能正确解析
        cur.execute("SELECT id, name, CAST(`tables` AS CHAR) as t FROM moi.knowledge_base ORDER BY id")
        print(f"\ntables JSON 验证:")
        for row in cur.fetchall():
            tables = json.loads(row[2]) if row[2] else []
            table_names = []
            for entry in tables:
                table_names.extend(entry.get("table_names", []))
            print(f"  id={row[0]} '{row[1]}' → 表: {table_names}")


# ==================== main ====================

def main():
    parser = argparse.ArgumentParser(description="NL2SQL 知识库迁移")
    parser.add_argument("--dry-run", action="store_true", help="空跑，不写入数据库")
    parser.add_argument("--target", choices=["local", "remote"], default="local",
                        help="目标数据库: local=本地Docker, remote=远程新工作区")
    args = parser.parse_args()

    # 1. 连接旧库读取数据
    print("=== 连接旧工作区 ===")
    old_conn = pymysql.connect(**OLD_REMOTE)
    print("[OK] 旧库已连接")

    print("\n=== 读取 knowledge_base ===")
    kb_rows = read_old_knowledge_bases(old_conn)
    print(f"读取到 {len(kb_rows)} 个 knowledge_base")

    print("\n=== 读取 nl2sql_knowledge ===")
    nk_rows = read_old_nl2sql_knowledge(old_conn)
    print(f"读取到 {len(nk_rows)} 条 nl2sql_knowledge")
    old_conn.close()

    # 2. 连接目标库
    if args.dry_run:
        print("\n=== DRY RUN 模式 ===")
        new_conn = None
    else:
        target_cfg = NEW_LOCAL if args.target == "local" else NEW_REMOTE
        print(f"\n=== 连接目标库 ({args.target}) ===")
        new_conn = pymysql.connect(**target_cfg)
        print("[OK] 目标库已连接")

        if args.target == "local":
            setup_local_database(new_conn)
            new_conn.select_db("moi")

        clear_target(new_conn)

    # 3. 迁移 knowledge_base
    print("\n=== 迁移 knowledge_base ===")
    kb_id_map = insert_knowledge_bases(new_conn, kb_rows, dry_run=args.dry_run)
    migrated_kb = len([k for k in kb_rows if k["id"] not in SKIP_KB_IDS])
    print(f"[OK] 迁移 {migrated_kb} 个 knowledge_base (跳过 {len(SKIP_KB_IDS)} 个)")

    # 4. 迁移 nl2sql_knowledge
    print("\n=== 迁移 nl2sql_knowledge ===")
    insert_nl2sql_knowledge(new_conn, nk_rows, kb_id_map, dry_run=args.dry_run)

    # 5. 验证
    if not args.dry_run:
        print("\n=== 验证 ===")
        verify(new_conn)
        new_conn.close()

    print("\n=== 完成 ===")


if __name__ == "__main__":
    main()
