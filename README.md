# BigDataSpark

## Запуск

Запустить docker
```
sudo docker compose up -d
```

Подключиться к сервису spark-driver:
```
sudo docker exec -it spark_driver bash
```

Выполнить команду:
```
spark-submit --jars /home/jovyan/work/jars/postgresql-42.6.0.jar,/home/jovyan/work/jars/clickhouse-jdbc-0.9.8-all.jar etl.py
```
(Возможна ошибка с подключением на WSL2. Перезапустить WSL2 в таком случае через ```wsl --shutdown``` в cmd)

Подключиться через DBeaver:
Порт 5432, пользователь user, пароль password, название базы данных: bigdata_lab.

## Результат
![Результат обработки](https://github.com/Supertos/BigDataSpark/blob/5d315ca221a92203ccd134b09c66a83f9b58b54f/result.PNG)
