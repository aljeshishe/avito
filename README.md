# Template project

Crawler of igoods.ru

## Quickstart   
Create conda environment and activate it

    conda env create -f requirements.yaml -p ./cenv
    . activate ./cenv

Run 2 docker containers: airflow with crawler DAG and juputer notebook

    docker-compose up

Open airflow

    http://127.0.0.1:8080

Open jupyter notebook

    http://127.0.0.1:8888
    
Stop containers when you are done

    docker-compose down