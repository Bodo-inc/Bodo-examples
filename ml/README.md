This folder contains some examples using scikit-learn for machine learning project. 
To run the examples:

1- Make a new file called credentials.json. This file is gitignored so it will not be saved in git.
2- Put your aws access and secret keys in the following format and save:

```shell
cd Bodo-examples/ml
vim credentials.json
```
and paste the json text below and populate with your credentials.

```json
{
  "aws": {
    "aws_access_key_id": "--",
    "aws_secret_access_key": "--"
  }
}
```

Open any of the notebooks and run it.

watch your htop in terminal and enjoy your CPUs running at maximum efficiency.

This is an image of running credit-card-fraud.ipynb with 8 cores.
![img.png](img.png)
Happy bodoing!!!
