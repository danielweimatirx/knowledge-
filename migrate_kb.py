"""
知识库跨工作区迁移脚本：问数Dev → AI Portal

用法:
  python migrate_kb.py --dry-run              # 空跑，不写入
  python migrate_kb.py                        # 正式迁移
  python migrate_kb.py --kb-ids 1,2,3         # 只迁移指定知识库
  python migrate_kb.py --reverse              # 反向：AI Portal → 问数Dev
"""
import argparse
import json
import sys

import pymysql

# ==================== 连接配置 ====================

WORKSPACES = {
    "dev": {
        "label": "问数Dev",
        "host": "freetier-01.cn-hangzhou.cluster.cn-dev.matrixone.tech",
        "port": 6001,
        "user": "ws_bf2d347f:moi_core_system:accountadmin",
        "password": "moi_2d76c2c1a5eb95b160e10e0b1dc47109ded45fbc9ad7641d3adcbd07ce09da78",
        "database": "moi",
        "charset": "utf8mb4",
        "autocommit": True,
    },
    "portal": {
        "label": "AI Portal",
        "host": "freetier-01.cn-hangzhou.cluster.cn-dev.matrixone.tech",
        "port": 6001,
        "user": "ws_bfb9ca8d:qa_manual_20260330185108_x9f3k2:accountadmin",
        "password": "moi_216a042120beaf5cdf357dfbc7a335a29c4b5d6641feeb598da2f3ccd824d342",
        "database": "moi",
        "charset": "utf8mb4",
        "autocommit": True,
    },
}


def get_conn(workspace_key):
    cfg = WORKSPACES[workspace_key]
    conn_params = {k: v for k, v in cfg.items() if k not in ("label",)}
    return pymysql.connect(**conn_params)


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


# ==================== 读取源库 ====================

def read_knowledge_bases(conn, kb_ids=None):
    """读取源库 knowledge_base"""
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
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
        return cur.fetchall()


def read_nl2sql_knowledge(conn, kb_ids=None):
    """读取源库 nl2sql_knowledge"""
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        if kb_ids:
            placeholders = ",".join(["%s"] * len(kb_ids))
            cur.execute(
                f"SELECT id, knowledge_base_id, knowledge_type, knowledge_key, "
                f"name, CAST(knowledge_value AS CHAR) AS knowledge_value, "
                f"CAST(associate_tables AS CHAR) AS associate_tables, "
                f"explanation_type, created_by, updated_by, created_at, updated_at "
                f"FROM moi.nl2sql_knowledge WHERE knowledge_base_id IN ({placeholders}) ORDER BY id",
                kb_ids,
            )
        else:
            cur.execute(
                "SELECT id, knowledge_base_id, knowledge_type, knowledge_key, "
                "name, CAST(knowledge_value AS CHAR) AS knowledge_value, "
                "CAST(associate_tables AS CHAR) AS associate_tables, "
                "explanation_type, created_by, updated_by, created_at, updated_at "
                "FROM moi.nl2sql_knowledge ORDER BY id"
            )
        return cur.fetchall()


# ==================== 写入目标库 ====================

def ensure_tables(conn):
    """确保目标库有 knowledge_base 和 nl2sql_knowledge 表"""
    with conn.cursor() as cur:
        cur.execute("CREATE DATABASE IF NOT EXISTS moi")
        cur.execute("USE moi")
        cur.execute(DDL_KNOWLEDGE_BASE)
        cur.execute(DDL_NL2SQL_KNOWLEDGE)
    print("[OK] 目标库表结构已就绪")


def migrate_knowledge_bases(conn, kb_rows, dry_run=False):
    """写入 knowledge_base，返回 old_id → new_id 映射"""
    id_map = {}
    for kb in kb_rows:
        if dry_run:
            print(f"  [DRY] kb_id={kb['id']} name='{kb['name']}'")
            print(f"         tables: {kb['tables_json']}")
            id_map[kb["id"]] = kb["id"]
            continue

        with conn.cursor() as cur:
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
            new_id = cur.lastrowid
            id_map[kb["id"]] = new_id
            print(f"  [OK] old={kb['id']} → new={new_id} name='{kb['name']}'")

    return id_map


def migrate_nl2sql_knowledge(conn, nk_rows, kb_id_map, dry_run=False):
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

        if dry_run:
            key_preview = (nk["knowledge_key"] or "")[:80].replace("\n", " ")
            print(f"  [DRY] nk_id={nk['id']} kb={old_kb_id}→{new_kb_id} type={nk['knowledge_type']} key='{key_preview}...'")
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


# ==================== main ====================

def main():
    parser = argparse.ArgumentParser(description="知识库跨工作区迁移")
    parser.add_argument("--dry-run", action="store_true", help="空跑，不写入数据库")
    parser.add_argument("--kb-ids", type=str, default=None,
                        help="只迁移指定知识库ID，逗号分隔，如 1,2,3")
    parser.add_argument("--reverse", action="store_true",
                        help="反向迁移：AI Portal → 问数Dev")
    args = parser.parse_args()

    if args.reverse:
        src_key, dst_key = "portal", "dev"
    else:
        src_key, dst_key = "dev", "portal"

    src_label = WORKSPACES[src_key]["label"]
    dst_label = WORKSPACES[dst_key]["label"]

    kb_ids = None
    if args.kb_ids:
        kb_ids = [int(x.strip()) for x in args.kb_ids.split(",")]

    # 1. 连接源库
    print(f"=== 连接源工作区: {src_label} ===")
    src_conn = get_conn(src_key)
    print("[OK] 源库已连接")

    # 2. 读取数据
    print(f"\n=== 读取 knowledge_base ===")
    kb_rows = read_knowledge_bases(src_conn, kb_ids)
    print(f"读取到 {len(kb_rows)} 个 knowledge_base")

    actual_kb_ids = [kb["id"] for kb in kb_rows]
    print(f"\n=== 读取 nl2sql_knowledge ===")
    nk_rows = read_nl2sql_knowledge(src_conn, actual_kb_ids if kb_ids else None)
    print(f"读取到 {len(nk_rows)} 条 nl2sql_knowledge")
    src_conn.close()

    if not kb_rows:
        print("\n没有需要迁移的知识库，退出")
        return

    # 3. 连接目标库
    if args.dry_run:
        print(f"\n=== DRY RUN 模式 ({src_label} → {dst_label}) ===")
        dst_conn = None
    else:
        print(f"\n=== 连接目标工作区: {dst_label} ===")
        dst_conn = get_conn(dst_key)
        print("[OK] 目标库已连接")
        ensure_tables(dst_conn)

    # 4. 迁移 knowledge_base
    print(f"\n=== 迁移 knowledge_base ({src_label} → {dst_label}) ===")
    kb_id_map = migrate_knowledge_bases(dst_conn, kb_rows, dry_run=args.dry_run)
    print(f"[OK] 迁移 {len(kb_id_map)} 个 knowledge_base")

    # 5. 迁移 nl2sql_knowledge
    print(f"\n=== 迁移 nl2sql_knowledge ===")
    migrate_nl2sql_knowledge(dst_conn, nk_rows, kb_id_map, dry_run=args.dry_run)

    # 6. 验证
    if not args.dry_run:
        print(f"\n=== 验证 ({dst_label}) ===")
        verify(dst_conn)
        dst_conn.close()

    print("\n=== 完成 ===")


if __name__ == "__main__":
    main()
