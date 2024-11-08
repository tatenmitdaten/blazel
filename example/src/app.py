import dbt_lambda.app


def lambda_handler(event, context):
    return dbt_lambda.app.lambda_handler(event, context)