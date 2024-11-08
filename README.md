# Project

The package provides helper functions to run dbt on AWS Lambda. The dbt project is fetched from a Git repository hosted on GitHub or AWS CodeCommit.

# How to configure

- `DBT_DOCS_BUCKET` - The S3 bucket where the dbt documentation is stored if `dbt docs generate` is run. You can provide a simple API Gateway to access the documentation with a secure connection.
- `DBT_REPOSITORY_NAME` - The name of the dbt repository.
- `GITHUB_ACCESS_TOKEN` - The GitHub access token to access the repository if the repository is hosted on GitHub.
- `GITHUB_SECRET_ARN` - The ARN of the secret that stores the GitHub access token on AWS Secret Manager. If the `GITHUB_SECRET_ARN` is provided, the `GITHUB_ACCESS_TOKEN` is overwritten with the value stored in the secret.
- `CODECOMMIT_ROLE_ARN` - The ARN of the role that has access to the AWS CodeCommit repository if the repository is hosted on AWS CodeCommit. The role is only required if the CodeCommit repository is in a separate AWS account.
- `SNOWFLAKE_SECRET_ARN` - The ARN of the secret that stores the Snowflake credentials on AWS Secret Manager.

Best practice is to store all parameters in the samconfig.yaml file and ship it with the project. Set the `SAM_CONFIG_FILE` environment variable to the path of the samconfig file. The app reads the parameters from the samconfig file and sets the environment variables listed above. See the example in the `example` directory.

The advantage of reading the parameters directly from the samconfig.yaml is that we need define them only in one place. We can also use the same samconfig file to set the parameters in the `template.yaml` to deploy the app.
