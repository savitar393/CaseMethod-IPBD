-- sql/00_create_panganwatch.sql

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_roles WHERE rolname = 'pangan_user'
    ) THEN
        CREATE ROLE pangan_user LOGIN PASSWORD 'user123';
    ELSE
        ALTER ROLE pangan_user WITH LOGIN PASSWORD 'user123';
    END IF;
END
$$;

SELECT 'CREATE DATABASE panganwatch OWNER pangan_user'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'panganwatch'
)\gexec

GRANT ALL PRIVILEGES ON DATABASE panganwatch TO pangan_user;