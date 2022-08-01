# Create a Bodo VMI

1. We will be using [Packer](https://www.packer.io) to build our VMI. [Install Packer](https://www.packer.io/downloads).

1. [Install Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli) if it's not already installed. 

1. Login using Azure CLI:

        az login

1. Get your Azure subscription's ID and set the value in an environment variable:

        export AZ_SUBSCRIPTION_ID="<SUBSCRIPTION_ID>"

1. Create a resource group to store the VMI:

        az group create --location eastus --name BodoExampleImageRG --subscription $AZ_SUBSCRIPTION_ID

1. A simple VMI template is defined in [``bodo_img.json``](./bodo_img.json). 
    
    For this example, we're using a publicly available Ubuntu Image as our base image. The image will be created in the ``eastus`` region.

1. Build image using Packer:

        packer build [-force] [-var 'bodo_version=<BODO_VERSION>'] -var "azure_subscription_id=$AZ_SUBSCRIPTION_ID" bodo_img.json

    NOTE: You might be asked to authenticate by going to ``https://microsoft.com/devicelogin`` and entering a provided code.
    
    NOTE: ``-force`` replaces existing images with same name.
    
    NOTE: The template installs ``bodo==2021.10`` by default. You can install a different version by specifying ``-var 'bodo_version=<BODO_VERSION>'``, for instance, ``-var 'bodo_version=2021.8'``

1. If successful, packer should output the VMI-ID of the created image. It should also output a manifest file ``vmi_build_manifest.json`` which shoud look like:

        {
            "builds": [
                {
                    "name": "Azure Arm Builder Bodo",
                    "builder_type": "azure-arm",
                    "build_time": 1636162634,
                    "files": null,
                    "artifact_id": "<VMI_ID>",
                    "packer_run_uuid": "<SOME_UUID>",
                    "custom_data": {
                        "bodo_version": "2021.10"
                    }
                }
            ],
            "last_run_uuid": "9430ab69-7fa6-791c-c58a-446f87727c81"
        }
    
    (The VMI-ID is in ``artifact_id``).
