# Задание 2: Работа с Big Data: обработка событий в реальном времени
В системе поступает существенное количество событий в секунду (клики, просмотры, покупки). Вам нужно спроектировать масштабируемую систему для обработки и хранения этих данных

## 1. Выбор технологий для обработки в реальном времени: 
- Kafka, Apache Flink, Spark Streaming, ClickHouse – какие инструменты использовать и почему?

Выбор инструментов будет зависить от поставленных задач.

Минимальная архитектура может состоять из:

[Продьюсер] --> [Kafka] --> [ClickHouse]

ClickHouse получает данные из Kafka через движок таблиц Kafka. Далее может быть реализована требуемая логика обработки через механизм материализованных представлений ClickHouse.

Если дополнительно есть требования по обработке данных в реальном времени (обогащение, скользящие статистики и т.п.) можно добавить в схему Spark Streaming (если допустимы задеркжи до нескольких секунд) или Apache Flink (если минимальные задержки критичны).

[Продьюсер] --> [Kafka] --> [Fink] --> [ClickHouse]

Эта архитектура ближе всего к Kappa 

## Хранилище агрегированных данных
### Как организовать оптимальное хранилище (OLAP vs OLTP)?
Классическим решением для хранения потоковых данных - OLAP системы.

Плюсь OLAP систем для хранения потоковых данных:
* Возможна запись на порядок больше строк в секунду, чем у OLTP систем.
* Горизонтальное масштабирование (увеличение количества шардов в Clickhouse), значительно проще чем в OLTP.
* Автоматическое партиционирование по времени (в PostgreSQL необходимо создавать новые партиции).
* Быстрая агрегация, сжатие данных за счет колоночного хранения.
* Простота настройки TTL для перевода архивных данных в холодное хранилище.

## Проектирование архитектуры

Простая архитектура

[Продьюсер] --> [Kafka] --> [ClickHouse]

Масштабирование Kafka осуществляется путем добавления брокеров. Дедубликация через Exactly-once семантику.

Масштабирование Clickouse - через шардирование. Дедупликация в ClickHouse через движок ReplacingMergeTree.

## Автоматизация развертывания ETL (CI/CD + мониторинг)

Для автоматизации развертывание ETL-пайплайна через CI/CD предлагаю следующую схему:
* Репозиторий с кодом ETL-пайплайна в GitLab
* CI/CD с автоматической сборкой Docker-контейнера с кодом пайплайна, его пушем в Docker-registry и созданием секретов в Kubernetes из переменных окружения при Merge Request в ветку
* Запуск контейнера чере KubernetesOperator в AirFlow или если весь пайплайн в одном контейнере, то можно настроить регулярный запуск через Pipeline Schedules в GitLab
* Запись метрик в Prometheus, мониторинг и алертинг в  Grafana