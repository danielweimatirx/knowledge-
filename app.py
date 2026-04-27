"""
NL2SQL 迁移脚本 Web 控制台
启动: python app.py
访问: http://localhost:9090
"""
import subprocess
import sys
import threading
from datetime import datetime

from flask import Flask, jsonify, render_template, request

import db_service

app = Flask(__name__)

# --- 全局任务状态 ---
task_state = {
    "status": "idle",       # idle / running / success / error
    "logs": [],
    "started_at": None,
    "finished_at": None,
    "return_code": None,
}
task_lock = threading.Lock()


def _run_script(target, dry_run):
    """在子线程中执行迁移脚本，逐行捕获输出"""
    cmd = [sys.executable, "migrate_nl2sql.py", "--target", target]
    if dry_run:
        cmd.append("--dry-run")

    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1,
    )
    for line in proc.stdout:
        with task_lock:
            task_state["logs"].append(line.rstrip("\n"))
    proc.wait()

    with task_lock:
        task_state["return_code"] = proc.returncode
        task_state["status"] = "success" if proc.returncode == 0 else "error"
        task_state["finished_at"] = datetime.now().isoformat(timespec="seconds")


# ==================== 页面路由 ====================

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/kb/<int:kb_id>")
def kb_detail_page(kb_id):
    return render_template("kb_detail.html", kb_id=kb_id)

@app.route("/v2")
def v2_index():
    return render_template("v2_index.html")

@app.route("/v2/model/<int:model_id>")
def v2_model_detail_page(model_id):
    return render_template("v2_model_detail.html", model_id=model_id)


# ==================== 迁移 API ====================

@app.route("/api/run", methods=["POST"])
def api_run():
    with task_lock:
        if task_state["status"] == "running":
            return jsonify({"ok": False, "msg": "脚本正在运行中，请等待完成"}), 409

    body = request.get_json(force=True) or {}
    target = body.get("target", "local")
    dry_run = body.get("dry_run", False)

    with task_lock:
        task_state.update(
            status="running", logs=[], return_code=None,
            started_at=datetime.now().isoformat(timespec="seconds"),
            finished_at=None,
        )

    t = threading.Thread(target=_run_script, args=(target, dry_run), daemon=True)
    t.start()
    return jsonify({"ok": True, "msg": "已启动"})


@app.route("/api/status")
def api_status():
    since = int(request.args.get("since", 0))
    with task_lock:
        return jsonify({
            "status": task_state["status"],
            "started_at": task_state["started_at"],
            "finished_at": task_state["finished_at"],
            "return_code": task_state["return_code"],
            "logs": task_state["logs"][since:],
            "total_lines": len(task_state["logs"]),
        })


# ==================== 数据 API（调 db_service） ====================

@app.route("/api/data")
def api_data():
    target = request.args.get("target", "local")
    try:
        return jsonify(db_service.get_raw_data(target))
    except ValueError as e:
        return jsonify({"ok": False, "msg": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "msg": f"连接失败: {e}"}), 500


@app.route("/api/kb")
def api_kb_list():
    target = request.args.get("target", "remote")
    try:
        return jsonify(db_service.get_knowledge_base_list(target))
    except ValueError as e:
        return jsonify({"ok": False, "msg": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "msg": f"查询失败: {e}"}), 500


@app.route("/api/kb/<int:kb_id>")
def api_kb_detail(kb_id):
    target = request.args.get("target", "remote")
    try:
        result = db_service.get_knowledge_base_detail(target, kb_id)
        status = 200 if result["ok"] else 404
        return jsonify(result), status
    except ValueError as e:
        return jsonify({"ok": False, "msg": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "msg": f"查询失败: {e}"}), 500


@app.route("/api/kb", methods=["POST"])
def api_kb_create():
    target = request.args.get("target", "remote")
    body = request.get_json(force=True)
    if not body or not body.get("name"):
        return jsonify({"ok": False, "msg": "name 必填"}), 400
    try:
        return jsonify(db_service.create_knowledge_base(target, body)), 201
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


@app.route("/api/kb/<int:kb_id>", methods=["PUT"])
def api_kb_update(kb_id):
    target = request.args.get("target", "remote")
    body = request.get_json(force=True)
    if not body or not body.get("name"):
        return jsonify({"ok": False, "msg": "name 必填"}), 400
    try:
        result = db_service.update_knowledge_base(target, kb_id, body)
        status = 200 if result["ok"] else 404
        return jsonify(result), status
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


@app.route("/api/kb/<int:kb_id>/items", methods=["POST"])
def api_item_create(kb_id):
    target = request.args.get("target", "remote")
    body = request.get_json(force=True) or {}
    body["knowledge_base_id"] = kb_id
    if not body.get("knowledge_type") or not body.get("knowledge_key"):
        return jsonify({"ok": False, "msg": "knowledge_type 和 knowledge_key 必填"}), 400
    try:
        return jsonify(db_service.create_knowledge_item(target, body)), 201
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


@app.route("/api/items/<int:item_id>", methods=["PUT"])
def api_item_update(item_id):
    target = request.args.get("target", "remote")
    body = request.get_json(force=True)
    if not body or not body.get("knowledge_type") or not body.get("knowledge_key"):
        return jsonify({"ok": False, "msg": "knowledge_type 和 knowledge_key 必填"}), 400
    try:
        result = db_service.update_knowledge_item(target, item_id, body)
        status = 200 if result["ok"] else 404
        return jsonify(result), status
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


@app.route("/api/items/<int:item_id>", methods=["DELETE"])
def api_item_delete(item_id):
    target = request.args.get("target", "remote")
    try:
        result = db_service.delete_knowledge_item(target, item_id)
        status = 200 if result["ok"] else 404
        return jsonify(result), status
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


@app.route("/api/kb/<int:kb_id>", methods=["DELETE"])
def api_kb_delete(kb_id):
    target = request.args.get("target", "remote")
    try:
        result = db_service.delete_knowledge_base(target, kb_id)
        status = 200 if result["ok"] else 404
        return jsonify(result), status
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


@app.route("/api/tables")
def api_tables():
    target = request.args.get("target", "remote")
    db_name = request.args.get("db", None)
    try:
        return jsonify(db_service.get_available_tables(target, db_name))
    except ValueError as e:
        return jsonify({"ok": False, "msg": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "msg": f"查询失败: {e}"}), 500


@app.route("/api/compare-kb", methods=["POST"])
def api_compare_kb():
    body = request.get_json(force=True) or {}
    ws_a = body.get("a", "remote")
    ws_b = body.get("b", "portal")
    if ws_a == ws_b:
        return jsonify({"ok": False, "msg": "两个工作区不能相同"}), 400
    try:
        return jsonify(db_service.compare_knowledge_bases(ws_a, ws_b))
    except ValueError as e:
        return jsonify({"ok": False, "msg": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "msg": f"对比失败: {e}"}), 500


@app.route("/api/sync-kb", methods=["POST"])
def api_sync_kb():
    body = request.get_json(force=True) or {}
    source = body.get("source")
    target = body.get("target")
    kb_name = body.get("kb_name")
    if not source or not target or not kb_name:
        return jsonify({"ok": False, "msg": "source, target, kb_name 必填"}), 400
    if source == target:
        return jsonify({"ok": False, "msg": "源和目标不能相同"}), 400
    try:
        return jsonify(db_service.sync_knowledge_base(source, target, kb_name))
    except ValueError as e:
        return jsonify({"ok": False, "msg": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "msg": f"同步失败: {e}"}), 500


@app.route("/api/migrate-kb", methods=["POST"])
def api_migrate_kb():
    body = request.get_json(force=True) or {}
    source = body.get("source", "remote")
    target = body.get("target", "portal")
    kb_ids = body.get("kb_ids")  # list of int or None for all
    dry_run = body.get("dry_run", False)
    if source == target:
        return jsonify({"ok": False, "msg": "源和目标不能相同"}), 400
    try:
        result = db_service.migrate_knowledge_bases(source, target, kb_ids, dry_run=dry_run, overwrite=body.get("overwrite", False))
        return jsonify(result)
    except ValueError as e:
        return jsonify({"ok": False, "msg": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "msg": f"迁移失败: {e}"}), 500


@app.route("/api/databases")
def api_databases():
    target = request.args.get("target", "remote")
    try:
        return jsonify(db_service.get_available_databases(target))
    except ValueError as e:
        return jsonify({"ok": False, "msg": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "msg": f"查询失败: {e}"}), 500


# ==================== V2 语义模型 API ====================

@app.route("/api/v2/models")
def api_v2_model_list():
    target = request.args.get("target", "remote")
    try:
        return jsonify(db_service.get_semantic_model_list(target))
    except ValueError as e:
        return jsonify({"ok": False, "msg": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "msg": f"查询失败: {e}"}), 500


@app.route("/api/v2/export")
def api_v2_export():
    target = request.args.get("target", "remote")
    try:
        return jsonify(db_service.get_semantic_model_export(target))
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


@app.route("/api/v2/models/<int:model_id>")
def api_v2_model_detail(model_id):
    target = request.args.get("target", "remote")
    try:
        result = db_service.get_semantic_model_detail(target, model_id)
        status = 200 if result["ok"] else 404
        return jsonify(result), status
    except ValueError as e:
        return jsonify({"ok": False, "msg": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "msg": f"查询失败: {e}"}), 500


@app.route("/api/v2/models", methods=["POST"])
def api_v2_model_create():
    target = request.args.get("target", "remote")
    body = request.get_json(force=True)
    if not body or not body.get("name"):
        return jsonify({"ok": False, "msg": "name 必填"}), 400
    try:
        return jsonify(db_service.create_semantic_model(target, body)), 201
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


@app.route("/api/v2/models/<int:model_id>", methods=["PUT"])
def api_v2_model_update(model_id):
    target = request.args.get("target", "remote")
    body = request.get_json(force=True)
    if not body or not body.get("name"):
        return jsonify({"ok": False, "msg": "name 必填"}), 400
    try:
        result = db_service.update_semantic_model(target, model_id, body)
        status = 200 if result["ok"] else 404
        return jsonify(result), status
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


@app.route("/api/v2/models/<int:model_id>", methods=["DELETE"])
def api_v2_model_delete(model_id):
    target = request.args.get("target", "remote")
    try:
        result = db_service.delete_semantic_model(target, model_id)
        status = 200 if result["ok"] else 404
        return jsonify(result), status
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


@app.route("/api/v2/models/<int:model_id>/entries", methods=["POST"])
def api_v2_entry_create(model_id):
    target = request.args.get("target", "remote")
    body = request.get_json(force=True) or {}
    body["model_id"] = model_id
    if not body.get("kind") or not body.get("key_name"):
        return jsonify({"ok": False, "msg": "kind 和 key_name 必填"}), 400
    try:
        return jsonify(db_service.create_semantic_entry(target, body)), 201
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


@app.route("/api/v2/entries/<int:entry_id>", methods=["PUT"])
def api_v2_entry_update(entry_id):
    target = request.args.get("target", "remote")
    body = request.get_json(force=True)
    if not body or not body.get("kind") or not body.get("key_name"):
        return jsonify({"ok": False, "msg": "kind 和 key_name 必填"}), 400
    try:
        result = db_service.update_semantic_entry(target, entry_id, body)
        status = 200 if result["ok"] else 404
        return jsonify(result), status
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


@app.route("/api/v2/entries/<int:entry_id>", methods=["DELETE"])
def api_v2_entry_delete(entry_id):
    target = request.args.get("target", "remote")
    try:
        result = db_service.delete_semantic_entry(target, entry_id)
        status = 200 if result["ok"] else 404
        return jsonify(result), status
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


# ==================== 问数过滤条件配置 ====================

@app.route("/api/filter-rules")
def api_filter_rules():
    target = request.args.get("target", "remote")
    try:
        result = db_service.get_filter_rules(target)
        status = 200 if result["ok"] else 500
        return jsonify(result), status
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


@app.route("/api/filter-rules/tables")
def api_filter_rule_tables():
    target = request.args.get("target", "remote")
    try:
        return jsonify(db_service.get_jst_flat_tables(target))
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


@app.route("/api/filter-rules/columns")
def api_filter_rule_columns():
    target = request.args.get("target", "remote")
    table = request.args.get("table", "")
    if not table:
        return jsonify({"ok": False, "msg": "缺少 table 参数"}), 400
    try:
        return jsonify(db_service.get_table_columns(target, table))
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


@app.route("/api/filter-rules", methods=["POST"])
def api_filter_rule_save():
    target = request.args.get("target", "remote")
    try:
        data = request.get_json(force=True)
        result = db_service.save_filter_rule(target, data)
        status = 200 if result["ok"] else 400
        return jsonify(result), status
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


@app.route("/api/filter-rules/<int:rule_set_id>", methods=["DELETE"])
def api_filter_rule_delete(rule_set_id):
    target = request.args.get("target", "remote")
    try:
        result = db_service.delete_filter_rule(target, rule_set_id)
        status = 200 if result["ok"] else 404
        return jsonify(result), status
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


@app.route("/api/system-config/<config_name>")
def api_system_config_get(config_name):
    target = request.args.get("target", "remote")
    try:
        return jsonify(db_service.get_system_config(target, config_name))
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


@app.route("/api/system-config/<config_name>", methods=["PUT"])
def api_system_config_set(config_name):
    target = request.args.get("target", "remote")
    body = request.get_json(force=True) or {}
    value = body.get("value")
    if value is None:
        return jsonify({"ok": False, "msg": "缺少 value"}), 400
    try:
        return jsonify(db_service.set_system_config(target, config_name, value))
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, port=9090)
