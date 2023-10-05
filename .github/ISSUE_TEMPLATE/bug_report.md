---
name: Bug report
about: Report a bug to help improve sqllineage
title: ''
labels: bug
assignees: ''

---

**Describe the bug**
* A clear and concise description of what the bug is.

**SQL**
Paste the SQL text here. For example:
```sql
insert into analyze select * from foo;
```

**To Reproduce**
*Note here we refer to SQL provided in prior step as stored in a file named `test.sql`*

- `if` CLI (Command Line Interface): provide the command you're calling and the output. 
For example:
```shell
sqllineage -f test.sql --dialect=ansi
```
```
Statements(#): 1
Source Tables:
    <default>.foo
Target Tables:
    <default>.analyze
```

- `elif` API (Application Programming Interface): provide the python code you're using and the output. 
For example:
```python
from sqllineage.runner import LineageRunner
with open("test.sql") as f:
    sql = f.read()
result = LineageRunner(sql, dialect="ansi")
print(result.target_tables)
```
```
[Table: <default>.analyze]
```

- `elif` Web UI (Web User Interface): provide the lineage graph which could be downloaded from the page, or screenshots if there're components other than the lineage graph that's related to this bug.

- `else`: whatever other ways to reproduce this bug.

**Expected behavior**
A clear and concise description of what you expected to happen, and the output in accordance with the `To Reproduce` section.

**Python version (available via `python --version`)**
 - 3.8.17
 - 3.9.18
 - 3.10.13
 - 3.11.5
 - etc.

**SQLLineage version (available via `sqllineage --version`):**
 - 1.3.8
 - 1.4.7
 - etc.

**Additional context**
Add any other context about the problem here.
