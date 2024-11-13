from blazel.tasks import Data
from blazel.tasks import ExtractTask
from blazel.tasks import ExtractLoadTable


def extract_column0(table: ExtractLoadTable, task: ExtractTask) -> Data:
    print(table.table_uri)
    print(task.limit)
    for _ in range(10):
        yield {'column0': 'value0', 'column1': '2024-01-01'}
