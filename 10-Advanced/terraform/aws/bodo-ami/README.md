# Create a Bodo AMI

1. We will be using [Packer](https://www.packer.io) to build our AMI. [Install Packer](https://www.packer.io/downloads).

1. Set up your AWS credentials. We recommend doing this through [environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html), but other methods should also work.

1. A simple AMI template is defined in [``bodo_img.json``](./bodo_img.json). 
    
    For this example, we're using a publicly available Ubuntu Image as our base image. The source AMI-ID (``ami-0885b1f6bd170450c``) is specific to the ``us-east-1`` region. If you want to build the image in another region, you can find the corresponding base AMI-ID in that region in your AWS EC2 Console under Public Images. The name should be similar to ``ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-20201026``. For instance, the AMI-ID for the corresponding image in us-east-2 is ``ami-0a91cd140a1fc148a``.

    Note: In the bodo_img.json file, Make sure instance type is  "t3a.medium" or higher to avoid failures.

1. Build image using Packer:

        packer build [-force] [-var 'bodo_version=<BODO_VERSION>'] bodo_img.json

    NOTE: ``-force`` replaces existing images with same name.
    
    NOTE: The template installs ``bodo==2021.10`` by default. You can install a different version by specifying ``-var 'bodo_version=<BODO_VERSION>'``, for instance, ``-var 'bodo_version=2021.8'``

1. If successful, packer should output the AMI-ID of the created image. It should also output a manifest file ``ami_build_manifest.json`` which shoud look like:

        {
            "builds": [
                {
                    "name": "AWS AMI Builder Bodo",
                    "builder_type": "amazon-ebs",
                    "build_time": 1635355885,
                    "files": null,
                    "artifact_id": "us-east-1:<AMI_ID>",
                    "packer_run_uuid": "<SOME_UUID>",
                    "custom_data": {
                        "bodo_version": "2021.10"
                    }
                }
            ],
            "last_run_uuid": "<SOME_UUID>"
        }
    
    (The AMI-ID is in ``artifact_id``).
