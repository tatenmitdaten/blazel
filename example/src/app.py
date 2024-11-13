from blazel.tables import SnowflakeWarehouse
from blazel.tasks import TaskFactory



def lambda_handler(event: dict, context):
    task = TaskFactory.from_dict(event)
    warehouse = SnowflakeWarehouse.from_yaml_file()
    task(warehouse)
