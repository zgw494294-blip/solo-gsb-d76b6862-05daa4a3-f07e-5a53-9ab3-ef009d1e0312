import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory


DATA_DIR = Path(os.environ.get("EXPERIMENT_DATA_DIR", "/tmp/sqlite-isolation"))
MAX_LOGS = 80

app = Flask(__name__, static_folder="static", static_url_path="")


def sqlite_error_info(error):
    """Return (code, symbolic name) for Python sqlite3 errors across versions."""
    code = getattr(error, "sqlite_errorcode", None)
    if code is None:
        return None, None

    name = getattr(error, "sqlite_errorname", None)
    if name:
        return code, name

    matches = sorted(
        constant_name
        for constant_name in dir(sqlite3)
        if constant_name.startswith("SQLITE_")
        and getattr(sqlite3, constant_name, None) == code
    )
    if matches:
        return code, matches[0]

    primary = code & 0xFF
    primary_matches = sorted(
        constant_name
        for constant_name in dir(sqlite3)
        if constant_name.startswith("SQLITE_")
        and getattr(sqlite3, constant_name, None) == primary
    )
    return code, primary_matches[0] if primary_matches else "UNKNOWN_SQLITE_ERROR"


def sqlite_error_response(error, http_status=None):
    code, name = sqlite_error_info(error)
    if http_status is None:
        primary_error_code = code & 0xFF
        if primary_error_code in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
            http_status = 409
        elif primary_error_code == sqlite3.SQLITE_ERROR:
            http_status = 400
        else:
            http_status = 500

    return (
        jsonify(
            {
                "error": str(error),
                "sqlite_error_code": code,
                "sqlite_error_name": name,
            }
        ),
        http_status,
    )


class CreationGate:
    """Allow concurrent pane requests, while making experiment replacement exclusive."""

    def __init__(self):
        self.condition = threading.Condition()
        self.active_operations = 0
        self.replacing = False

    @contextmanager
    def operation(self):
        with self.condition:
            while self.replacing:
                self.condition.wait()
            self.active_operations += 1

        try:
            yield
        finally:
            with self.condition:
                self.active_operations -= 1
                self.condition.notify_all()

    @contextmanager
    def replacing(self):
        with self.condition:
            self.replacing = True
            self.condition.wait_for(lambda: self.active_operations == 0)
            try:
                yield
            finally:
                self.replacing = False
                self.condition.notify_all()


creation_gate = CreationGate()
current_experiment = None


def open_long_connection(database_path):
    # isolation_level=None makes Python issue exactly the SQL statements we send.
    # timeout=0 / busy_timeout=0 makes lock conflicts immediate instead of waiting.
    connection = sqlite3.connect(
        database_path,
        timeout=0,
        isolation_level=None,
        check_same_thread=False,
    )
    connection.execute("PRAGMA busy_timeout=0")
    return connection


class PaneConnection:
    def __init__(self, key, label, connection):
        self.key = key
        self.label = label
        self.connection = connection
        self.lock = threading.RLock()
        self.last_read = None
        self.logs = []

    def log(self, message, ok):
        self.logs.append(
            {
                "id": len(self.logs) + 1,
                "time": time.strftime("%H:%M:%S"),
                "message": message,
                "ok": ok,
            }
        )
        if len(self.logs) > MAX_LOGS:
            del self.logs[: len(self.logs) - MAX_LOGS]

    def snapshot(self):
        acquired = self.lock.acquire(blocking=False)
        if not acquired:
            return {
                "key": self.key,
                "label": self.label,
                "busy": True,
                "in_transaction": None,
                "last_read": None,
                "logs": [],
            }

        try:
            return {
                "key": self.key,
                "label": self.label,
                "busy": False,
                "in_transaction": bool(self.connection.in_transaction),
                "last_read": self.last_read,
                "logs": list(self.logs),
            }
        finally:
            self.lock.release()

    def close(self):
        with self.lock:
            self.connection.close()


class MonitorConnection:
    def __init__(self, connection):
        self.connection = connection
        self.lock = threading.RLock()
        self.last_read = None
        self.logs = []

    def log(self, message, ok):
        self.logs.append(
            {
                "id": len(self.logs) + 1,
                "time": time.strftime("%H:%M:%S"),
                "message": message,
                "ok": ok,
            }
        )
        if len(self.logs) > MAX_LOGS:
            del self.logs[: len(self.logs) - MAX_LOGS]

    def read_committed(self):
        with self.lock:
            row = self.connection.execute(
                "SELECT value FROM account WHERE id = 1"
            ).fetchone()
            self.last_read = row[0]
            self.log(f"第三条连接显式读取已提交值：{self.last_read}", True)
            return self.last_read

    def snapshot(self):
        acquired = self.lock.acquire(blocking=False)
        if not acquired:
            return {"busy": True, "in_transaction": None, "last_read": None, "logs": []}

        try:
            return {
                "busy": False,
                # An autocommit SELECT must not leave a transaction open.
                "in_transaction": bool(self.connection.in_transaction),
                "last_read": self.last_read,
                "logs": list(self.logs),
            }
        finally:
            self.lock.release()

    def close(self):
        with self.lock:
            self.connection.close()


class Experiment:
    def __init__(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.experiment_id = uuid.uuid4().hex
        self.database_path = DATA_DIR / f"experiment-{self.experiment_id}.db"

        initial_connection = sqlite3.connect(
            self.database_path, timeout=0, isolation_level=None
        )
        try:
            journal_mode = initial_connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            initial_connection.execute(
                "CREATE TABLE account (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)"
            )
            initial_connection.execute("INSERT INTO account (id, value) VALUES (1, 100)")
        finally:
            initial_connection.close()

        connection_a = open_long_connection(self.database_path)
        connection_b = open_long_connection(self.database_path)
        monitor = open_long_connection(self.database_path)

        self.panes = {
            "a": PaneConnection("a", "甲栏 / 连接 A", connection_a),
            "b": PaneConnection("b", "乙栏 / 连接 B", connection_b),
        }
        self.monitor = MonitorConnection(monitor)
        self.journal_mode = journal_mode

    def state(self):
        return {
            "experiment_id": self.experiment_id,
            "journal_mode": self.journal_mode,
            "database_path": str(self.database_path),
            "panes": [self.panes["a"].snapshot(), self.panes["b"].snapshot()],
            "committed": self.monitor.snapshot(),
        }

    def close(self):
        for pane in self.panes.values():
            pane.close()
        self.monitor.close()

        for suffix in ("", "-wal", "-shm"):
            try:
                self.database_path.with_name(self.database_path.name + suffix).unlink()
            except FileNotFoundError:
                pass


def get_current_experiment():
    experiment = current_experiment
    if experiment is None:
        return None
    return experiment


def require_experiment(experiment_id):
    experiment = get_current_experiment()
    if experiment is None or experiment.experiment_id != experiment_id:
        return None
    return experiment


@app.get("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.post("/api/experiments")
def create_experiment():
    global current_experiment

    with creation_gate.replacing():
        old_experiment = current_experiment
        experiment = Experiment()
        current_experiment = experiment
        if old_experiment is not None:
            old_experiment.close()
        state = experiment.state()

    return jsonify({"experiment_id": experiment.experiment_id, "state": state}), 201


@app.get("/api/experiments/<experiment_id>/state")
def experiment_state(experiment_id):
    experiment = require_experiment(experiment_id)
    if experiment is None:
        return jsonify({"error": "实验不存在；请新建实验。"}), 404

    # This deliberately performs no SELECT and creates no SQLite snapshot.
    return jsonify({"state": experiment.state()})


@app.post("/api/experiments/<experiment_id>/committed/read")
def read_committed(experiment_id):
    with creation_gate.operation():
        experiment = require_experiment(experiment_id)
        if experiment is None:
            return jsonify({"error": "实验不存在；请新建实验。"}), 404

        try:
            experiment.monitor.read_committed()
        except sqlite3.Error as error:
            return sqlite_error_response(error)

        return jsonify({"state": experiment.state()})


@app.post("/api/experiments/<experiment_id>/panes/<pane_key>/begin")
def begin_transaction(experiment_id, pane_key):
    with creation_gate.operation():
        experiment = require_experiment(experiment_id)
        if experiment is None:
            return jsonify({"error": "实验不存在；请新建实验。"}), 404

        pane = experiment.panes.get(pane_key)
        if pane is None:
            return jsonify({"error": "未知连接栏。"}), 404

        with pane.lock:
            if pane.connection.in_transaction:
                pane.log("拒绝 BEGIN：事务已经开启，不能重复开启。", False)
                response = jsonify({"error": "事务已经开启。"})
                response.status_code = 409
                return response

            try:
                pane.connection.execute("BEGIN DEFERRED")
                pane.log("执行 BEGIN DEFERRED；读取前不会取得写锁。", True)
                return jsonify({"state": experiment.state()})
            except sqlite3.Error as error:
                code, name = sqlite_error_info(error)
                pane.log(f"{name}({code})：{error}", False)
                return sqlite_error_response(error)


@app.post("/api/experiments/<experiment_id>/panes/<pane_key>/read")
def read_value(experiment_id, pane_key):
    with creation_gate.operation():
        experiment = require_experiment(experiment_id)
        if experiment is None:
            return jsonify({"error": "实验不存在；请新建实验。"}), 404

        pane = experiment.panes.get(pane_key)
        if pane is None:
            return jsonify({"error": "未知连接栏。"}), 404

        with pane.lock:
            if not pane.connection.in_transaction:
                pane.log("拒绝读取：必须先 BEGIN DEFERRED，禁止在事务外建立读状态。", False)
                return jsonify({"error": "请先开启事务。"}), 400

            try:
                row = pane.connection.execute(
                    "SELECT value FROM account WHERE id = 1"
                ).fetchone()
                pane.last_read = row[0]
                pane.log(f"显式读取当前快照：value = {pane.last_read}", True)
                return jsonify({"state": experiment.state()})
            except sqlite3.Error as error:
                code, name = sqlite_error_info(error)
                pane.log(f"{name}({code})：{error}", False)
                return sqlite_error_response(error)


@app.post("/api/experiments/<experiment_id>/panes/<pane_key>/add")
def add_value(experiment_id, pane_key):
    with creation_gate.operation():
        experiment = require_experiment(experiment_id)
        if experiment is None:
            return jsonify({"error": "实验不存在；请新建实验。"}), 404

        pane = experiment.panes.get(pane_key)
        if pane is None:
            return jsonify({"error": "未知连接栏。"}), 404

        payload = request.get_json(silent=True) or {}
        amount = payload.get("amount")

        with pane.lock:
            if not pane.connection.in_transaction:
                pane.log("拒绝写入：必须在已开启的事务内修改，禁止事务外修改。", False)
                return jsonify({"error": "请先开启事务。"}), 400

            if isinstance(amount, bool) or not isinstance(amount, int):
                pane.log("拒绝写入：请输入整数。", False)
                return jsonify({"error": "请输入整数。"}), 400

            try:
                pane.connection.execute(
                    "UPDATE account SET value = value + ? WHERE id = 1", (amount,)
                )
                pane.log(f"执行写入：UPDATE account SET value = value + {amount}", True)
                return jsonify({"state": experiment.state()})
            except sqlite3.Error as error:
                code, name = sqlite_error_info(error)
                pane.log(
                    f"写入失败：{name}（错误码 {code}）：{error}；失败事务未自动结束。",
                    False,
                )
                return sqlite_error_response(error)


@app.post("/api/experiments/<experiment_id>/panes/<pane_key>/commit")
def commit_transaction(experiment_id, pane_key):
    with creation_gate.operation():
        experiment = require_experiment(experiment_id)
        if experiment is None:
            return jsonify({"error": "实验不存在；请新建实验。"}), 404

        pane = experiment.panes.get(pane_key)
        if pane is None:
            return jsonify({"error": "未知连接栏。"}), 404

        with pane.lock:
            if not pane.connection.in_transaction:
                pane.log("拒绝提交：当前没有开启的事务。", False)
                return jsonify({"error": "当前没有事务。"}), 400

            try:
                pane.connection.execute("COMMIT")
                pane.log("执行 COMMIT，修改已提交。", True)
                return jsonify({"state": experiment.state()})
            except sqlite3.Error as error:
                code, name = sqlite_error_info(error)
                pane.log(
                    f"提交失败：{name}（错误码 {code}）：{error}；事务未自动结束。",
                    False,
                )
                return sqlite_error_response(error)


@app.post("/api/experiments/<experiment_id>/panes/<pane_key>/rollback")
def rollback_transaction(experiment_id, pane_key):
    with creation_gate.operation():
        experiment = require_experiment(experiment_id)
        if experiment is None:
            return jsonify({"error": "实验不存在；请新建实验。"}), 404

        pane = experiment.panes.get(pane_key)
        if pane is None:
            return jsonify({"error": "未知连接栏。"}), 404

        with pane.lock:
            if not pane.connection.in_transaction:
                pane.log("拒绝回滚：当前没有开启的事务。", False)
                return jsonify({"error": "当前没有事务。"}), 400

            try:
                pane.connection.execute("ROLLBACK")
                pane.log("执行 ROLLBACK，当前事务结束；可重新 BEGIN DEFERRED。", True)
                return jsonify({"state": experiment.state()})
            except sqlite3.Error as error:
                code, name = sqlite_error_info(error)
                pane.log(f"{name}({code})：{error}", False)
                return sqlite_error_response(error)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=True)
