"""This trial code demonstrates how to use Bodo SDK to run a SQL query on a Bodo Engine cluster with Snowflake as
data source.

To get started, install Bodo SDK and run the Python script:

    pip install bodosdk
    python bodo_trial.py

See Bodo SDK docs for more SDK examples: https://pypi.org/project/bodosdk
See Bodo docs for more info: https://docs.bodo.ai/
"""
import re
from bodosdk.models import WorkspaceKeys, CreateSQLJobRun
from bodosdk.client import get_bodo_client


# Construct the SDK Client
creds = WorkspaceKeys(
    client_id="...",
    secret_key="...",
)
client = get_bodo_client(creds)

# read the sql file
with open("tpch.sql", "r") as f:
    query = f.read()

# Submit query to execute on the cluster

print("\nExecuting Query:", query)
job_run = client.job.submit_sql_job_run(
    CreateSQLJobRun(
        clusterUUID='...',
        sql_query_text=query,
        retryStrategy=None,
        # Input data is in Snowflake storage
        catalog="SNOWFLAKE",
        args={"--print_output": ""},
    )
)

print("\nWaiting for job to finish executing on the cluster...")
# Wait for the job to finish executing
waiter = client.job.get_job_run_waiter()
res = waiter.wait(job_run.uuid)

# Get job logs for more details, including execution time
print("\nDownloading job logs...")
logs = client.job.get_job_logs(res.uuid)
with open(f"stdout_{res.uuid}.txt", "r") as f:
    stdout_log_txt = f.read()

exec_time = re.search(
    r"Finished executing the query. It took (\d+\.\d+) seconds.", stdout_log_txt
).group(1)
exec_time = float(exec_time)

output = re.search(r"Output:\n(.*)\n", stdout_log_txt, re.DOTALL).group(1)
output = output[: output.find("0: Total (compilation + execution) time:")]
output = "Output of workers:\n" + output
print(output)

print(f"Job Execution Time: {exec_time} seconds\n")
print(
    f"See job log file for query output and Bodo execution details: stdout_{res.uuid}.txt"
)
