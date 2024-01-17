import string
from networkx import DiGraph
from sqllineage.core.models import Table, Schema
from secrets import token_hex, choice


class NewTable(Table):
    def __init__(self, name: str, schema: Schema = Schema(), **kwargs):
        super().__init__(name, schema, **kwargs)
        self._hash_value = int("".join(choice(string.digits) for i in range(8)))
        # print(self._hash_value)

    def __hash__(self):
        return self._hash_value


g = DiGraph()
a = NewTable("a")
b = NewTable("b")
print(
    hash(a.schema),
)

g.add_edge(a, b)


print("before change schema")
for node in g.nodes():
    if node in g.nodes():
        print(
            f"find node {node._raw_name}",
        )
    else:
        print("can't find node {node.raw_name}")

a.schema._raw_name = "ods"
print("after change schema")
for node in g.nodes():
    print(node)
    if node in g.nodes():
        print("ok")
    else:
        print("false")

for _ in range(10):
    x = y = 0
    for _ in range(10):
        for node in g.nodes():
            if node in g.nodes():
                x = x + 1
            else:
                y = y + 1
    print(x, y)
