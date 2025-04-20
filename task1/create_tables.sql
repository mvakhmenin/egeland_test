CREATE TABLE platforms (
    platform_id SERIAL PRIMARY KEY,
    platform_name VARCHAR(100) UNIQUE,
    create_ts TIMESTAMP DEFAULT NOW()
)

INSERT INTO platforms (platform_name)
	VALUES ('TikTok')

CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    user_name VARCHAR(100) UNIQUE,
    subject VARCHAR(100),
    platform_id INT REFERENCES platforms(platform_id),
    create_ts TIMESTAMP DEFAULT NOW()
)

INSERT INTO users (user_name, subject, platform)
	VALUES ('obschestvoznaika_el', 'Обществознание', 0),
            ('himichka_el', 'Химия', 0),
			('anglichanka_el', 'Английский', 0),
			('fizik_el', 'Физика', 0),
			('katya_matematichka', 'Математика', 0)

CREATE TABLE user_stats (
    id SERIAL,
    user_id INT REFERENCES users(user_id),
    platform_id INT REFERENCES platforms(platform_id),
    followers INT,
    subscriptions INT,
    likes INT,
    record_ts TIMESTAMP DEFAULT NOW(), 
    record_hour TIMESTAMP GENERATED ALWAYS AS (DATE_TRUNC('HOUR', record_ts)) STORED, -- вычисляемая колонка для проверки униальности по часам
    PRIMARY KEY (id, user_id), -- необходимо включить user_id, так как это ключ партиционирования
    CONSTRAINT user_id_uniq_record_hour UNIQUE(user_id, platform_id, record_hour)
) PARTITION BY LIST (user_id);

-- Для timeseries предполагается сортировка по времени, поэтому добавим этот индекс для быстрой сортировки
CREATE INDEX idx_user_stats_record_hour ON user_stats (record_hour);

-- Дополнительная таблица для сохранения метрик, которых нет в основной таблице
-- Создается при необходимости расширения исходного перечня метрик
CREATE TABLE user_stats_extended (
    stats_id INT REFERENCES user_stats(id),
    metric_name VARCHAR(50) NOT NULL,
    metric_value INT NOT NULL,
    PRIMARY KEY (core_id, metric_name)
);
-- индекс для поиска по stats_id
CREATE INDEX idx_extended_core_id ON user_stats_extended (stats_id);

-- Функция для автоматического создания партиции в таблице user_stats при добавления пользователя в таблицу users
CREATE OR REPLACE FUNCTION create_user_partition()
RETURNS TRIGGER AS $$
DECLARE
    partition_name TEXT;
    partition_query TEXT;
BEGIN
    partition_name := 'user_stats_' || NEW.user_id;
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind = 'r' 
        AND c.relname = partition_name
        AND n.nspname = current_schema()
    ) THEN
        partition_query := format(
            'CREATE TABLE %I PARTITION OF user_stats FOR VALUES IN (%L)',
            partition_name,
            NEW.user_id
        );
        EXECUTE partition_query;
        EXECUTE format('CREATE INDEX ON %I (record_ts)', partition_name);
        RAISE NOTICE 'Создана новая партиция: %', partition_name;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггер, который вызывает функцию выше при добавлении нового пользователя в таблицу users
CREATE TRIGGER trg_create_user_partition
BEFORE INSERT ON users
FOR EACH ROW EXECUTE FUNCTION create_user_partition();