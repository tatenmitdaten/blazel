from tasks import TaskFactory


def lambda_handler(event: dict, context):
    task = TaskFactory.from_dict(event)
    task()
