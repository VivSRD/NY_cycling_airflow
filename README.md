#### Пайплайн выгрузки ежедневных отчётов по количеству поездок на велосипедах в городе Нью-Йорк, который:

1. Отслеживает появление новых файлов в бакете на AWS S3. 
   (Представим, что пользователь или провайдер данных будет загружать 
   новые исторические данные по поездкам в наш бакет);
2. При появлении нового файла запускается оператор импорта данных в 
   созданную таблицу базы данных Clickhouse;
3. Далее сформируются таблицы с ежедневными отчётами по следующим 
   критериям:
   – количество поездок в день
   – средняя продолжительность поездок в день
   – распределение поездок пользователей, разбитых по категории «gender»
4. Данные статистики загружаются на специальный S3 бакет (reports) с 
   хранящимися отчётами по загруженным файлам.

![image](https://github.com/VivSRD/NY_cycling_airflow/blob/main/screens/schema.png)


#### В AWS создаем и конфигурируем:

1. Публичный бакет Amazon S3, куда будут загружаться файлы от пользователей (tripdata) и отчеты с обработанными данными (reports);
2. Amazon MWAA, куда загружаем наш даг
3. ClickHouse Cluster on AWS для обработки данных, в котором создаем таблицу report

#### Даг состоит из 6 Задач:

![image](https://github.com/VivSRD/NY_cycling_airflow/blob/main/screens/graph.png)

![image](https://github.com/VivSRD/NY_cycling_airflow/blob/main/screens/dag_run.png)


1. *new_file_onto_S3_trigger*:
Сенсор, который проверяет содержимое бакета tripdata на появление файла 
"{{ ds.format('%Y%m') }}-citibike-tripdata.csv.zip", где ds.format('%Y%m') - дата запуска 
2. *unziped_files_to_s3*:
Извлекает csv файлы из zip архива из п.1, результат вызова функции используется в задаче load_data_to_Clickhouse.
3. *load_data_to_Clickhouse*:
Данные, извлеченные из архива, передаются в таблицу Clickhouse

4-6. *{name}_report*:
Агрегирующие запросы к таблице Clickhouse из п.3, формирующие отчеты; при целевом запуске на облаке движок таблиц будет отличаться; в текущем варианте результат очереди записывается в csv файл и сохраняется локально

#####  – количество поездок в день (daytrips.csv)

![image](https://github.com/VivSRD/NY_cycling_airflow/blob/main/screens/dt_XCom.png)
   
##### – средняя продолжительность поездок в день (avg_duration.csv)

![image](https://github.com/VivSRD/NY_cycling_airflow/blob/main/screens/avg_XCom.png)
   
##### – распределение поездок пользователей, разбитых по категории «gender» (daytrips_per_gender.csv)

![image](https://github.com/VivSRD/NY_cycling_airflow/blob/main/screens/gender_XCom.png)



Параметры BUCKET_KEY, BUCKET_NAME, FILE_NAME, URL настраиваются в файле окружения .env
