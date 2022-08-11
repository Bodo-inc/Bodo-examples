import os
def load_aws_creds():
    with open(f"{os.getcwd()}/secrets/secrets.txt") as f:
        for line in f:
            key,value=line.strip().split("=")
            os.environ[key]=value
            
def load_sf_creds():
    with open(f"{os.getcwd()}/secrets/sf-secrets.txt") as f:
        for line in f:
            key,value=line.strip().split("=")
            os.environ[key]=value
            