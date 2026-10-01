# SQLite WAL 事务隔离实验

这是一个使用 **Python + Flask + 原生 HTML/CSS/JavaScript** 的 SQLite 并发事务实验网页。

每次点击“新建实验”，后端都会创建一个独立的 WAL SQLite 数据库。数据库只有一张表、一行数据：

```sql
PRAGMA journal_mode=WAL;
CREATE TABLE account (id INTEGER PRIMARY KEY, value INTEGER NOT NULL);
INSERT INTO account (id, value) VALUES (1, 100);
```

页面中的甲栏和乙栏分别绑定两个进程内长期、独立的 SQLite 连接；右侧/顶部的“第三条独立连接”用于显式查看当前已提交值。所有数据放在容器的内存盘 `/data` 中，容器删除后不需要保留。

## 启动

需要 Docker Compose v2：

```bash
docker compose up --build
```

然后访问：

```text
http://localhost:8000
```

停止：

```bash
docker compose down
```

> 必须使用仓库中的单进程 Flask 服务。不要改成多个 worker，因为甲、乙栏和第三条连接都必须是同一个实验进程内的长期连接。

## 操作按钮

甲、乙两栏都有：

- `BEGIN DEFERRED`：真实执行 `BEGIN DEFERRED`。
- `读值`：真实执行 `SELECT value FROM account WHERE id = 1`，只允许在事务内执行。
- `加上输入整数`：真实执行 `UPDATE account SET value = value + ? WHERE id = 1`，只允许在事务内执行，不会自动提交。
- `提交 COMMIT`：真实执行 `COMMIT`。
- `回滚 ROLLBACK`：真实执行 `ROLLBACK`。

每栏显示：

- 最近一次“显式读值”读到的数字；
- 当前连接的事务是否开启；
- 操作记录和失败信息；
- 当前请求未完成时，本栏所有按钮和输入框会被禁用，但另一栏仍可操作。

第三条连接区域只有点击“读取已提交值”时才会执行一次 autocommit SELECT。刷新两栏状态的接口只读取进程保存的状态和连接的 `in_transaction` 标记，不执行 SELECT，因此不会提前替任何栏建立 SQLite 快照。

锁等待时间设置为 `busy_timeout=0`，所以写锁冲突会立即返回错误；失败的事务不会被后端自动提交或回滚。

## 示例一：WAL 快照冲突

按以下顺序操作：

1. 点击“新建实验”。
2. 甲栏点击 `BEGIN DEFERRED`，再点击“读值”，看到最近读值为 `100`。
3. 乙栏点击 `BEGIN DEFERRED`。
4. 乙栏输入 `10`，点击“加上输入整数”。
5. 乙栏点击“提交 COMMIT”。
6. 回到甲栏再次点击“读值”，甲栏仍读到 `100`。这是因为甲的事务仍持有自己开始读时的快照。
7. 甲栏输入任意整数并点击“加上输入整数”，会立即失败并显示：

   ```text
   SQLITE_BUSY_SNAPSHOT
   错误码 517
   database is locked
   ```

   甲栏的事务仍保持开启，后端不会自动回滚。

8. 甲栏点击“回滚 ROLLBACK”。
9. 甲栏重新点击 `BEGIN DEFERRED`，再点击“读值”，此时读到乙已经提交的 `110`。
10. 点击第三条连接的“读取已提交值”，同样可以看到已提交值 `110`。

## 示例二：写锁互斥

可以再次点击“新建实验”取得一行新的 `100`，然后：

1. 甲栏点击 `BEGIN DEFERRED`。
2. 甲栏输入 `10`，点击“加上输入整数”。此时甲已经取得写锁，但不要提交。
3. 乙栏点击 `BEGIN DEFERRED`。
4. 乙栏输入 `5`，点击“加上输入整数”，会立即失败并显示：

   ```text
   SQLITE_BUSY
   错误码 5
   database is locked
   ```

   乙栏事务不会被自动结束；甲栏的按钮也没有被阻塞。

5. 甲栏点击“回滚 ROLLBACK”释放写锁。
6. 乙栏再次点击“加上输入整数”，可以重试成功。
7. 乙栏点击“提交 COMMIT`。
8. 第三条连接点击“读取已提交值”，看到 `105`。

## API 摘要

所有状态刷新都不执行额外 SELECT：

- `POST /api/experiments`：新建独立 WAL 实验。
- `GET /api/experiments/<id>/state`：返回保存状态和连接事务标记；不执行 SELECT，也不会建立 SQLite 快照。
- `POST /api/experiments/<id>/committed/read`：第三条连接显式读取已提交值。
- `POST /api/experiments/<id>/panes/<a|b>/begin`
- `POST /api/experiments/<id>/panes/<a|b>/read`
- `POST /api/experiments/<id>/panes/<a|b>/add`
- `POST /api/experiments/<id>/panes/<a|b>/commit`
- `POST /api/experiments/<id>/panes/<a|b>/rollback`
