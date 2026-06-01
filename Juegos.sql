-- 1. Dimensión de Juegos (Los jueguitos)
CREATE TABLE dim_games (
    game_id INT PRIMARY KEY,
    title VARCHAR(255),
    developer VARCHAR(150),
    publisher VARCHAR(150)
);

-- 2. Dimensión de Etiquetas / Categorías (Relación 1:N con los juegos)
CREATE TABLE dim_game_tags (
    tag_id SERIAL PRIMARY KEY,
    game_id INT REFERENCES dim_games(game_id),
    tag_name VARCHAR(100),
    tag_priority INT -- 1 para el género dominante (ej. FPS), 2 para el segundo, etc.
);

-- 3. Dimensión de Tiempo (Para el análisis analítico histórico)
CREATE TABLE dim_time (
    time_id SERIAL PRIMARY KEY,
    full_date TIMESTAMP UNIQUE,
    hour INT,
    day INT,
    month INT,
    year INT
);

-- 4. Tabla de Hechos (Métrica de streaming: Jugadores Concurrentes)
CREATE TABLE fact_players (
    fact_id SERIAL PRIMARY KEY,
    game_id INT REFERENCES dim_games(game_id),
    time_id INT REFERENCES dim_time(time_id),
    ccu INT -- Cantidad de jugadores activos en ese momento
);