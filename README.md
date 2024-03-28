# api_caller_to_snowflake_etl
project that calls a series of APIs (swagger) and finally executes a snowflake process

deploy.sh ensure that the deployment to the server is encloused in a local env
run-snowflake-import-process.sh is to be used if a orchestrator is requiered
  this approach asumes there is a file called ".envdata" with a local os env data to connect to the client api and snowflake
