# PGSQL_Docker
> A practice of using docker to create a Postgresql container and import the content of a csv file into the database by using Python. Finally, test whether `db_query_test.py` which is used to query the data and input the data into GPT-4 model to generate the correct answer can fetch the data from the database normally. 

- [PGSQL\_Docker](#pgsql_docker)
  - [Docker Image](#docker-image)
  - [Docker Container](#docker-container)
    - [Create and run the container](#create-and-run-the-container)
    - [Create a database](#create-a-database)
  - [Python Script](#python-script)
    - [Install the required packages](#install-the-required-packages)
    - [Run the script](#run-the-script)
    - [Test the script](#test-the-script)


## Docker Image
I personally install the docker into Ubuntu 20.04 which is running on wsl2. 

> [!WARNING]  
> I originally ran the docker by using the docker desktop on Windows 10. However, I found that the docker desktop somehow has some conflicts with Ubuntu 20.04 on wskl2, which makes the Python script cannot connect to the Postgresql container normally. Therefore, I suggest you to install the docker on Ubuntu 20.04 directly.

The following command is used to pull the postgres image from the docker hub.
```bash
docker pull postgres
```

## Docker Container

### Create and run the container
Run the following command to create a Postgresql container.
```bash
docker run --name <your container name> -e POSTGRES_PASSWORD=<password> -d -p <port>:5432(default= 5432:5432) postgres postgres
```
- `your container name`: Set the name of the container you want to create.
- `password`: Set the password of the superuser of the Postgresql database.
- `port`: Set the port of the local machine. 

### Create a database
Run the following command to create a database.
```bash
docker exec -it my-postgres psql -U postgres -c "create database <database-name> owner <user-name>"
```
- `database-name`: Set the name of the database you want to create.
- `user-name`: Set the name of the user who owns the database.

## Python Script

### Install the required packages
Using anaconda to install the required packages is recommended.

- Run the following command to create a new environment.
    ```bash
    conda create -n <env name> python=3.9
    ```
    `env name`: the name of the environment you want to create.

- Run the following command to activate the environment.
    ```bash
    conda activate <env name>
    ```
    `env name`: the name of the environment you want to create.

- Run the following command to install the required packages.
    ```bash
    pip install -r requirements.txt
    ```

### Run the script
Before running the script, you need to modify the following variables in `data_process.py`.
- `host`: the host of the Postgresql container.
- `dbname`: the name of the database.
- `user`: the user of the database.
- `password`: the password of the user.
- `port`(optional): the port of the Postgresql container. If you use the default port, you can ignore this variable.

Run the following command to start importing the content of the csv file into the database.
```bash
python data_process.py
```

### Test the script
Run the following command to test whether the script can fetch the data from the database normally.
```bash
python db_query_test.py
```