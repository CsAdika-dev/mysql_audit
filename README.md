# MySQL Audit Procedure

This repository contains two stored procedures that help you **audit** data changes
in a MySQL database:

* `create_audit` – creates an audit table (if it does not exist) and generates
  **INSERT**, **UPDATE** and **DELETE** triggers for every base table in a given
  schema. The triggers write a JSON representation of the changed rows into the
  audit table.
* `delete_audit_triggers` – drops all triggers that were created by
  `create_audit` (identified by a suffix you provide).

Both procedures do **not** execute the generated DDL statements directly. They
store the statements in a temporary table and finally return a single string
named `run_this_ddl`.  You can copy this string and execute it with a normal
`CALL` or in a MySQL client.

---

## Prerequisites

* MySQL 5.7 or newer (JSON datatype is required).
* The user that runs the procedures must have the privileges to:
  - `CREATE TABLE`
  - `CREATE TRIGGER`
  - `DROP TRIGGER`
  - `SELECT` on `INFORMATION_SCHEMA` tables.
* The schema you want to audit must already exist.

---

## Files

* `create_audit.sql` – definition of the `create_audit` procedure.
* `delete_audit_triggers.sql` – definition of the `delete_audit_triggers`
  procedure.

---

## How to install the procedures

```bash
# Load the procedures into the server (replace <path> with the actual path)
mysql -u <user> -p <database> < <path>/create_audit.sql
mysql -u <user> -p <database> < <path>/delete_audit_triggers.sql
```

If you are using a MySQL client that supports `source`, you can also run:

```sql
SOURCE /full/path/to/create_audit.sql;
SOURCE /full/path/to/delete_audit_triggers.sql;
```

---

## Usage

### 1. Create the audit infrastructure

```sql
-- Parameters
SET @schema_name   = 'my_schema';          -- the schema you want to audit
SET @audit_table   = 'audit_log';          -- name of the audit table
SET @trigger_suffix = 'audit';             -- any suffix you like (used to identify the triggers)

CALL create_audit(@schema_name, @audit_table, @trigger_suffix);

-- The procedure returns a single row with a column named `run_this_ddl`.
-- Copy the content of that column and execute it, e.g.:
-- (in the mysql client you can use the \G output format to see the full text)
SELECT run_this_ddl FROM (CALL create_audit(@schema_name, @audit_table, @trigger_suffix)) AS t \G

-- Execute the returned DDL (you can paste it directly into the client):
/*
   <paste the output here>
*/
```

The generated triggers will write rows into `audit_log` with the following
columns:

| Column            | Description                                          |
|-------------------|------------------------------------------------------|
| `id`              | Auto‑increment primary key.                           |
| `table_name`      | Name of the table where the change occurred.         |
| `old_row_data`   | JSON object with the **previous** row values (NULL for INSERT). |
| `new_row_data`   | JSON object with the **new** row values (NULL for DELETE).    |
| `dml_type`        | `'INSERT'`, `'UPDATE'` or `'DELETE'`.                |
| `dml_timestamp`   | Time of the change (defaults to `CURRENT_TIMESTAMP`). |
| `dml_created_by`  | MySQL user that performed the change (`USER()`).      |

### 2. Remove the audit triggers

When you no longer need the audit, drop the generated triggers with:

```sql
SET @schema_name   = 'my_schema';
SET @trigger_suffix = 'audit';   -- must match the suffix used when creating

CALL delete_audit_triggers(@schema_name, @trigger_suffix);

-- As with `create_audit`, the procedure returns a `run_this_ddl` column.
SELECT run_this_ddl FROM (CALL delete_audit_triggers(@schema_name, @trigger_suffix)) AS t \G

/*
   Paste the output and run it to actually drop the triggers.
*/
```

The audit table (`audit_log` in the example) is **not** dropped automatically –
you may keep it for historical analysis or drop it manually if you wish.

---

## Example Session

```sql
-- 1. Load the procedures (once)
SOURCE ~/mysql_audit/create_audit.sql;
SOURCE ~/mysql_audit/delete_audit_triggers.sql;

-- 2. Create audit infrastructure for the `sales` schema
CALL create_audit('sales', 'audit_log', 'audit');

-- 3. Retrieve the generated DDL and run it (copy‑paste the result)
SELECT run_this_ddl FROM (CALL create_audit('sales', 'audit_log', 'audit')) AS t \G
/* paste result here */

-- 4. Test – insert a row into a table in the `sales` schema and verify audit entry
INSERT INTO sales.orders (customer_id, amount) VALUES (1, 100);
SELECT * FROM sales.audit_log ORDER BY id DESC LIMIT 1;

-- 5. When you are done, clean up the triggers
CALL delete_audit_triggers('sales', 'audit');
SELECT run_this_ddl FROM (CALL delete_audit_triggers('sales', 'audit')) AS t \G
/* paste result to drop triggers */
```

---

## Limitations & Things to Consider

* The procedure builds a **single** large DDL string. For schemas with a very
  high number of tables or columns you may need to increase the MySQL session
  variable `group_concat_max_len` (the script already sets it to 10 MB, which
  is sufficient for most cases).
* Triggers are created **AFTER** the DML operation, therefore the audit entry
  reflects the final state of the row.
* The audit table stores JSON, so you can query it using MySQL JSON functions
  (`JSON_EXTRACT`, `JSON_TABLE`, …) for reporting.
* The scripts do not automatically purge old audit rows. Implement a separate
  cleanup job if you need data retention policies.

---

## License

The code in this repository is released under the MIT License. Feel free to
adapt it to your own projects.

---

*Created by the `sql/mysql_audit` project.*
