import utils # This ensures that the logging configuration in utils.py is applied
import time
from typing import Tuple, Optional
from datetime import datetime
import requests

from config import RunSettings

class APIClient:
    def __init__(
        self,
        base_url: str,
        client_id: str,
        client_secret: str,
        application_name: str = "ClientApp",
    ):
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.application_name = application_name

    def _headers(self, headers):
        new_headers = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "application_name": self.application_name,
        }
        return dict(**headers, **new_headers)

    def _send(self, method, url: str, *args, **kwargs):
        headers = self._headers(kwargs.pop("headers", {}))
        send_url = f'{self.base_url}{url.lstrip("/")}'
        return self._parse_response(
            requests.request(method, send_url, headers=headers, *args, **kwargs)
        )

    def get(self, *args, **kwargs):
        return self._send("GET", *args, **kwargs)

    def post(self, *args, **kwargs):
        return self._send("POST", *args, **kwargs)

    def _parse_response(self, resp: requests.Response):
        data = resp.json()

        if data is None:
            utils.log_error("Expected response from ClientApp is missing or None")
        elif "status_code" not in data and "repository_id" not in data:
            utils.log_error("Unexpected response format")
        elif "status_code" in data and data["status_code"] != 200:
            utils.log_error(f"Error from ClientApp with message: {data['msg']}")
           
        return data

    @classmethod
    def from_settings(cls, settings: RunSettings):
        return cls(
            settings.base_url,
            settings.clientapi_client_id,
            settings.clientapi_client_secret,
            settings.clientapi_application_name
        )


class JobExecutor:
    def __init__(
        self, client: APIClient, max_probes: int = 360, probe_interval: int = 5
    ):
        self.client = client
        self.max_probes = max_probes
        self.probe_interval = probe_interval

    @staticmethod
    def _parse_datetime (datetime_str: str) -> datetime:

        if datetime_str is None:
            return None
        
        # Define the format based on example: "actual_start_time": "Fri, 29 Dec 2023 18:47:16 GMT"
        datetime_format = "%a, %d %b %Y %H:%M:%S GMT"

        # Parse the string to datetime object
        return datetime.strptime(datetime_str, datetime_format)

    def _launch_job(self, job_name: str) -> dict:
        utils.log_info("Sending request for job to be launch")
        full_response = self.client.post(f"job/joblauncher/{job_name}")

        if not full_response:
            utils.log_error("No response from Job launch","Invalid response structure")
        
        utils.log_info("Job was launched successfully")
        return full_response
       
    def _launch_bulk_jobs(self, repository_id: str) -> list:
        utils.log_info(f"Sending request to launch all jobs for repository | Debug Data (repository_id: {repository_id})")
        full_response = self.client.post(f"job/joblauncher/bulk?repository_id={repository_id}",data='')

        if not full_response:
            utils.log_error("No response from bulk job launch","Invalid response structure")
        
        utils.log_info("Bulk jobs launched successfully")
        return full_response
    
    def _get_status(self, run_id: int, batch_job_id: str) -> Tuple[str, str, datetime, Optional[datetime]]:
        try:
            payload = {
                "batch_job_id":batch_job_id,
                "runid": run_id              
            }
            full_response = self.client.post("/job/jobstatus", data=payload)

            if "data" in full_response:
                response = full_response["data"]
            else:
                response = None

            if response is None:
                utils.log_info("Received no response from the server")
                return False, None, None, None
            
            job_status = response.get('status')
            job_name = response.get('job_name','Unknown Job')
            actual_start_time = response.get('actual_start_time')
            completed_time = response.get('completed_time')
            job_start_time = JobExecutor._parse_datetime(actual_start_time)
            job_completed_time = JobExecutor._parse_datetime(completed_time)

            return job_status, job_name, job_start_time, job_completed_time
            
        except Exception as e:
            utils.log_error(f"An error occurred: {e}")
            return False, None, None, None
   
    def _poll_for_completion(self, job_data):
        incomplete_jobs = job_data.copy()
        completed_jobs = []
        probe_count = 0

        while incomplete_jobs:
            for job in incomplete_jobs:
                run_id = job["runid"]
                batch_job_id = job["batch_job"]
                job_status, job_name, job_start_time, job_completed_time = self._get_status(run_id, batch_job_id)
                job_start_time = job_start_time or datetime.utcnow()
                job_status = job_status or "started"
                if job_status in ("started", "submitted"):
                    elapsed_time = int((datetime.utcnow() - job_start_time).total_seconds())
                    utils.log_info(f"Job {job_name} is running (elapsed time: {elapsed_time} seconds) | Debug Data (run_id: {run_id}, batch_job_id: {batch_job_id})")
                elif job_status == "completed":
                    elapsed_time = int((job_completed_time - job_start_time).total_seconds())
                    utils.log_info(f"Job {job_name} completed in {elapsed_time} seconds")
                    completed_jobs.append(job)
                else:
                    utils.log_error(f"Job {job_name} encountered an unexpected status: {job_status}")
                    return False
            incomplete_jobs = [job for job in incomplete_jobs if job not in completed_jobs]

            job_status, job_name = None, None
            if incomplete_jobs:
                if probe_count >= self.max_probes:
                    utils.log_error("Max probe limit reached. Exiting due to timeout")
                    return False
                time.sleep(self.probe_interval)
                probe_count += 1

        return True

    def get_repository_id(self, repository_name: str) -> str:
        utils.log_info("Sending request to get repository id")
        full_response = self.client.get(f"catalog/repository/{repository_name}")

        if 'repository_id' in full_response:
            return full_response["repository_id"]
        else:
            utils.log_error("Requiered key (repository_id) not found in response","Invalid response structure")

    def run_job(self, job_name: str):
        job_info = self._launch_job(job_name)

        if not "data" in job_info:
            utils.log_error("Requiered Keys not found in response","Invalid response structure")
        
        # Convert the job_info to a format similar to bulk jobs
        job_data = [{
            "batch_job": job_info["data"]["batch_job"],
            "runid": job_info["data"]["runid"],
            "job_name": job_name
        }]
                
        return self._poll_for_completion(job_data)
    
    def run_bulk_jobs(self, repository_id: str):
        job_info = self._launch_bulk_jobs(repository_id)
        
        if not "data" in job_info:
            utils.log_error("Requiered Keys not found in response","Invalid response structure")
        
        job_data = job_info["data"]

        return self._poll_for_completion(job_data)