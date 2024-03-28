import utils # This ensures that the logging configuration in utils.py is applied
import typer

from config import settings_factory, Environment, Mode
from api_client import APIClient, JobExecutor
from db_client import Snowflake

def main(
    mode: Mode = None,
    name: str = None,
    env: Environment = None,
    max_probes: int = 360, # This gives a total of 30 minutes wait.
    probe_interval: int = 5
):
    if not name:
        utils.log_error("Name must be provided")
    
    settings = settings_factory(env)
    utils.log_info(f"Environment is {env.value}")

    run_job = mode in ('job', 'fulljob')
    run_repository = mode in ('repository', 'fullrepository')
    run_snowflake = mode in ('fulljob', 'fullrepository', 'snowflake')
    run_atlas = run_job or run_repository

    if run_atlas:
        client = APIClient.from_settings(settings)
        executor = JobExecutor(client, max_probes, probe_interval)

        if run_job:
            res = executor.run_job(name)
        elif run_repository:
            repository_id = executor.get_repository_id(name)
            res = executor.run_bulk_jobs(repository_id)
    
        if not res:
            utils.log_error("Job(s) did not complete properly")
    
    
    if run_snowflake:
        sf = Snowflake(settings)

        # Convert job_name to empty string if it is None
        if run_repository:
            job_name = ""
        else:
            job_name = name       

        sf.call_stored_procedure(job_name)

    utils.log_info("Logging system ended.")

if __name__ == "__main__":
    typer.run(main)