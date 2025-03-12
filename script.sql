-- 1. Создаем таблицу в который будем хранить логи изменений UPDATE / INSERT / DELETE  для всех таблиц

CREATE TABLE global_change_log (
    log_id SERIAL PRIMARY KEY,  
    table_name TEXT,  -- Имя таблицы, где было изменение
    action_type VARCHAR(10),  -- INSERT / UPDATE / DELETE
    record_id BIGINT,  -- ID измененной записи
    old_data JSONB,  -- Данные до обновления (только для UPDATE / DELETE)
    new_data JSONB,  -- Данные после изменения (только для INSERT / UPDATE)
    changed_at TIMESTAMP DEFAULT NOW(),  -- Дата изменения
    changed_by TEXT DEFAULT current_user,  -- Кто изменил
    processed BOOLEAN DEFAULT FALSE  -- Флаг обработки
);

-- 2. Создаем функцию которая будет логировать изменения во всех таблицах 'TG_TABLE_NAME'

CREATE OR REPLACE FUNCTION log_global_changes()
RETURNS TRIGGER AS $$
BEGIN
    -- Логируем INSERT
    IF TG_OP = 'INSERT' THEN
        INSERT INTO global_change_log (table_name, action_type, record_id, new_data)
        VALUES (TG_TABLE_NAME, 'INSERT', NEW.id, row_to_json(NEW));
        RETURN NEW;
    END IF;

    -- Логируем UPDATE
    IF TG_OP = 'UPDATE' THEN
        INSERT INTO global_change_log (table_name, action_type, record_id, old_data, new_data)
        VALUES (TG_TABLE_NAME, 'UPDATE', OLD.id, row_to_json(OLD), row_to_json(NEW));
        RETURN NEW;
    END IF;

    -- Логируем DELETE
    IF TG_OP = 'DELETE' THEN
        INSERT INTO global_change_log (table_name, action_type, record_id, old_data)
        VALUES (TG_TABLE_NAME, 'DELETE', OLD.id, row_to_json(OLD));
        RETURN OLD;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- 3. Скрипт для создания триггеров сразу для всех таблиц

DO $$ 
DECLARE r RECORD;
BEGIN
    FOR r IN (SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('aggmobile', 'staff', 'my_info', 'bookings')) 
    LOOP
        EXECUTE format(
            'CREATE TRIGGER trg_log_changes_%I
             AFTER INSERT OR UPDATE OR DELETE
             ON %I
             FOR EACH ROW
             EXECUTE FUNCTION log_global_changes();',
            r.table_name, r.table_name
        );
    END LOOP;
END $$;

-- 4. создаем функцию под каждую таблицу которая смотрит в таблицу изменений
CREATE OR REPLACE FUNCTION get_incremental_my_info()
RETURNS TABLE (
    id INT,
    first_name VARCHAR(25),
    last_name VARCHAR(25),
    city VARCHAR(25),
    regist_date DATE
) 
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    -- Новые записи (INSERT)
    SELECT t.id, t.first_name, t.last_name, t.city, t.regist_date
    FROM my_info t
    INNER JOIN global_change_log c ON t.id = c.record_id
    WHERE c.action_type = 'INSERT' AND c.processed = FALSE AND c.table_name = 'my_info'
    
    UNION ALL
    
    -- Обновленные записи (UPDATE)
    SELECT t.id, t.first_name, t.last_name, t.city, t.regist_date
    FROM my_info t
    INNER JOIN global_change_log c ON t.id = c.record_id
    WHERE c.action_type = 'UPDATE' AND c.processed = FALSE AND c.table_name = 'my_info';
END;
$$;

CREATE OR REPLACE FUNCTION bqc_kap_get_changes(table_name TEXT)
RETURNS SETOF RECORD
LANGUAGE plpgsql
AS $$
DECLARE
    primary_key_column TEXT;
    dynamic_query TEXT;
BEGIN
    -- Определяем, есть ли поле 'id' в таблице
    SELECT attname INTO primary_key_column
    FROM pg_attribute 
    WHERE attrelid = table_name::regclass 
      AND attname IN ('id', 'idr') 
      AND attnum > 0 
      AND NOT attisdropped
    LIMIT 1;

    -- Строим динамический SQL с нужным полем
    dynamic_query := format(
        'SELECT t.* 
         FROM %I t 
         INNER JOIN global_change_log c 
         ON t.%I = c.record_id
         WHERE c.action_type IN (''INSERT'', ''UPDATE'') 
         AND c.processed = FALSE 
         AND c.table_name = %L',
        table_name, primary_key_column, table_name
    );

    -- Помечаем записи как обработанные
    EXECUTE format(
        'UPDATE global_change_log 
         SET processed = TRUE 
         WHERE table_name = %L 
         AND action_type IN (''INSERT'', ''UPDATE'') 
         AND processed = FALSE',
        table_name
    );

    -- Выполняем динамический SQL
    RETURN QUERY EXECUTE dynamic_query;
END;
$$;


CREATE OR REPLACE FUNCTION bqc_kap_clean_old_logs()
RETURNS VOID 
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM global_change_log 
    WHERE processed = TRUE 
      AND changed_at < NOW() - INTERVAL '60 days';
END;
$$;
