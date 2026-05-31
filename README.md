# BigDataFlink
Анализ больших данных - лабораторная работа №3 - Streaming processing с помощью Flink

Одним из самых популярных фреймворков для работы со streaming processing является Apache Flink. Apache Flink - мощный фреймворк, который предлагает широкий набор функциональности для простого написания streaming processing.

Что необходимо сделать? 

Необходимо реализовать потоковую обработку данных с помощью Flink, который читает топик Kafka, трансформирует данные в режиме streaming в модель звезда и пишет результат в PostgreSQL. Данные в Kafka-топиках хранятся в формате json. Данные в топик kafka нужно отправлять самостоятельно, эмулируя источник данных.

Какие данные отправляются в Kafka?
 - Каждое сообщение в Kafka-топике - это строчка из csv файлов, преобразованная в формат json.

Какие данные отправляются в PostgreSQL?
 - Трансформированные данные в модель данных звезда.

![Лабораторная работа №3](https://github.com/user-attachments/assets/d3c1544d-3fe6-4c15-b673-9aa5d27dbd76)


Алгоритм:

1. Клонируете к себе этот репозиторий.
2. Устанавливаете инструмент для работы с запросами SQL (рекомендую DBeaver).
3. Устанавливаете базу данных PostgreSQL (рекомендую установку через docker).
4. Устанавливаете Apache Flink (рекомендую установку через Docker).
5. Устанавливаете Apache Kafka (рекомендую установку через Docker).
6. Скачиваете файлы с исходными данными mock_data( * ).csv, где ( * ) номера файлов. Всего 10 файлов, каждый по 1000 строк.
7. Реализуете приложение, которое каждую строчку из исходных csv-файлов преобразует в json и отправляет в виде сообщения в Kafka-топик.
8. Реализуете приложение на Flink, которое читает Kafka-топик, преобразует данные в модель звезда и сохраняет в PostgreSQL в режиме streaming.
9. Проверяете конечные данные в PostgreSQL.
10. Отправляете работу на проверку лаборантам.

Что должно быть результатом работы?

1. Репозиторий, в котором есть исходные данные mock_data().csv, где () номера файлов. Всего 10 файлов, каждый по 1000 строк.
2. Файл docker-compose.yml с установкой PostgreSQL, Flink, Kafka и запуском приложения, которое из файлов mock_data(*).csv создает сообщения json в Kafka.
3. Инструкция, как запускать Flink-джобу и приложение для отправки данных в Kafka для проверки лабораторной работы.
4. Код Apache Flink для трансформации данных в режиме streaming.

## Реализация в этом репозитории

Решение состоит из:

- `docker-compose.yml` - PostgreSQL, Kafka, Flink JobManager/TaskManager и producer CSV -> JSON -> Kafka.
- `sql/init.sql` - создание схемы `star` и таблиц модели звезда.
- `src/main/java/ru/bigdataflink/CsvKafkaProducer.java` - читает все CSV-файлы из `исходные данные`, преобразует каждую CSV-запись в JSON и отправляет в Kafka topic `pet-sales`.
- `src/main/java/ru/bigdataflink/StreamingStarSchemaJob.java` - Flink streaming job, читает JSON из Kafka и пишет результат в PostgreSQL.
- `src/main/java/ru/bigdataflink/StarSchemaJdbcSink.java` - upsert в таблицы измерений и фактов.

CSV читается через полноценный CSV-парсер, потому что поле `product_description` содержит переносы строк внутри кавычек.

## Модель данных

PostgreSQL создается с базой `petshop`, пользователем `flink` и паролем `flink`.

Схема `star` содержит таблицы:

- `star.dim_customer`
- `star.dim_seller`
- `star.dim_supplier`
- `star.dim_product`
- `star.dim_store`
- `star.dim_date`
- `star.fact_sales`

В разных CSV-файлах исходные `id`, `sale_customer_id`, `sale_seller_id` и `sale_product_id` повторяются. Чтобы не терять факты при загрузке 10 файлов, producer добавляет к каждому Kafka-сообщению служебные поля:

- `source_file`
- `source_record_number`
- `event_id`

Flink использует `event_id` как ключ факта, а ключи измерений строит с учетом `source_file`.

## Запуск

Требуется Docker с Docker Compose.

```powershell
docker compose up --build
```

Команда:

1. Соберет Java jar через Maven внутри Docker.
2. Запустит PostgreSQL и создаст таблицы.
3. Запустит Kafka и создаст topic `pet-sales`.
4. Запустит Flink job.
5. Запустит producer, который отправит все CSV-записи в Kafka.

Flink UI будет доступен по адресу:

```text
http://localhost:8081
```

PostgreSQL доступен на `localhost:5432`:

```text
database: petshop
user: flink
password: flink
```

## Проверка результата

После завершения producer можно проверить количество загруженных фактов:

```powershell
docker compose exec postgres psql -U flink -d petshop -c "select count(*) from star.fact_sales;"
```

Пример просмотра фактов:

```powershell
docker compose exec postgres psql -U flink -d petshop -c "select sale_event_id, sale_date, quantity, total_price from star.fact_sales order by sale_event_id limit 10;"
```

Проверка измерений:

```powershell
docker compose exec postgres psql -U flink -d petshop -c "select count(*) from star.dim_customer;"
docker compose exec postgres psql -U flink -d petshop -c "select count(*) from star.dim_product;"
docker compose exec postgres psql -U flink -d petshop -c "select count(*) from star.dim_store;"
```

## Повторный запуск с чистой базой

```powershell
docker compose down -v
docker compose up --build
```

## Ручной запуск producer

Если инфраструктура уже запущена, producer можно повторно запустить отдельно:

```powershell
docker compose run --rm producer
```

Запись в PostgreSQL сделана через `ON CONFLICT DO UPDATE`, поэтому повторный запуск producer не создает дубликаты фактов.
