-- Creación del esquema OLAP
CREATE SCHEMA IF NOT EXISTS OLAP;

-- Dimensión: dim_cuentas
CREATE TABLE OLAP.dim_cuentas (
    id_dim_cuenta VARCHAR PRIMARY KEY,
    id_cuenta BIGINT,
    nombre_cuenta VARCHAR,
    correo VARCHAR,
    fecha_creacion TIMESTAMP,
    fecha_actualizacion TIMESTAMP,
    valido_desde DATE,
    valido_hasta DATE,
    es_actual BOOLEAN
);

-- Dimensión: dim_contenidos
CREATE TABLE OLAP.dim_contenidos (
    id_dim_contenido VARCHAR PRIMARY KEY,
    id_contenido BIGINT,
    titulo VARCHAR,
    descripcion TEXT,
    tipo_contenido VARCHAR,
    categoria VARCHAR,
    duracion INT,
    fecha_creacion TIMESTAMP,
    fecha_actualizacion TIMESTAMP,
    valido_desde DATE,
    valido_hasta DATE,
    es_actual BOOLEAN
);

-- Dimensión: dim_suscripciones
CREATE TABLE OLAP.dim_suscripciones (
    id_dim_suscripcion VARCHAR PRIMARY KEY,
    id_suscripcion BIGINT,
    nombre_suscripcion VARCHAR,
    max_contenidos_mensuales INT,
    fecha_creacion TIMESTAMP,
    fecha_actualizacion TIMESTAMP
);

-- Dimensión: dim_tiempo_dia
CREATE TABLE OLAP.dim_tiempo_dia (
    id_dim_tiempo_dia INT PRIMARY KEY,
    -- Aquí se pueden agregar columnas específicas de la dimensión de tiempo, como año, mes, día, etc.
);

-- Tabla de hechos: fact_cuentas_suscripcion
CREATE TABLE OLAP.fact_cuentas_suscripcion (
    id_dim_cuenta VARCHAR,
    id_dim_suscripcion VARCHAR,
    id_dim_tiempo_dia INT,
    FOREIGN KEY (id_dim_cuenta) REFERENCES OLAP.dim_cuentas (id_dim_cuenta),
    FOREIGN KEY (id_dim_suscripcion) REFERENCES OLAP.dim_suscripciones (id_dim_suscripcion),
    FOREIGN KEY (id_dim_tiempo_dia) REFERENCES OLAP.dim_tiempo_dia (id_dim_tiempo_dia)
);

-- Tabla de hechos: fact_creacion_contenido
CREATE TABLE OLAP.fact_creacion_contenido (
    id_dim_cuenta VARCHAR,
    id_dim_contenido VARCHAR,
    id_dim_tiempo_dia INT,
    FOREIGN KEY (id_dim_cuenta) REFERENCES OLAP.dim_cuentas (id_dim_cuenta),
    FOREIGN KEY (id_dim_contenido) REFERENCES OLAP.dim_contenidos (id_dim_contenido),
    FOREIGN KEY (id_dim_tiempo_dia) REFERENCES OLAP.dim_tiempo_dia (id_dim_tiempo_dia)
);


INSERT INTO OLAP.dim_cuentas (
    id_dim_cuenta, id_cuenta, nombre_cuenta, correo, fecha_creacion, fecha_actualizacion, valido_desde, valido_hasta, es_actual
)
SELECT
    'NA' AS id_dim_cuenta, NULL AS id_cuenta, 'No Aplica' AS nombre_cuenta, 'na@example.com' AS correo,
    NULL AS fecha_creacion, NULL AS fecha_actualizacion, TIMESTAMP '1900-01-01' AS valido_desde, TIMESTAMP '9999-12-31' AS valido_hasta, TRUE AS es_actual
UNION ALL
SELECT
    md5(CAST(account_id AS VARCHAR) || account_name || email || CAST(created_at AS VARCHAR) || CAST(updated_at AS VARCHAR)) AS id_dim_cuenta,
    account_id AS id_cuenta,
    account_name AS nombre_cuenta,
    email AS correo,
    created_at AS fecha_creacion,
    updated_at AS fecha_actualizacion,
    '1900-01-01' AS valido_desde, -- Puede ocurrir que la fecha de creacion de una cuenta en la base sea posterior a algÃºn hecho que se registre en las fact tables (aunque no deberÃ­a ocurrir), por eso no se usa created_at
    '9999-12-31' AS valido_hasta,
    TRUE AS es_actual
FROM
    reto_sql.main.accounts_historical;

SELECT * FROM OLAP.dim_cuentas; 

INSERT INTO OLAP.dim_suscripciones (
	id_dim_suscripcion, id_suscripcion, nombre_suscripcion, max_contenidos_mensuales, fecha_creacion, fecha_actualizacion
)
SELECT
    'NA' AS id_dim_suscripcion, NULL AS id_suscripcion, 'No Aplica' AS nombre_suscripcion, NULL AS max_contenidos_mensuales,
    NULL AS fecha_creacion, NULL AS fecha_actualizacion
UNION ALL
SELECT
    md5(CAST(subscription_id AS VARCHAR) || subscription_name || max_contents_per_month || CAST(created_at AS VARCHAR) || CAST(updated_at AS VARCHAR)) AS id_dim_suscripcion,
    subscription_id AS id_suscripcion,
    subscription_name AS nombre_suscripcion,
    max_contents_per_month AS max_contenidos_mensuales,
    created_at AS fecha_creacion,
    updated_at AS fecha_actualizacion
FROM
    reto_sql.main.subscriptions_historical;

INSERT INTO OLAP.dim_contenidos (
    id_dim_contenido, id_contenido, titulo, descripcion, tipo_contenido, categoria, duracion, fecha_creacion, fecha_actualizacion,
    valido_desde, valido_hasta, es_actual
)
SELECT
    md5(CAST(c.content_id AS VARCHAR) || CAST(c.account_id AS VARCHAR) || c.title || c.description || c.content_type || CAST(c.created_at AS VARCHAR) || CAST(c.updated_at AS VARCHAR)) AS id_dim_contenido,
    c.content_id AS id_contenido,
    c.title AS titulo,
    c.description AS descripcion,
    c.content_type AS tipo_contenido,
    MAX(CASE WHEN ca.attribute_name = 'Categoría' THEN ca.string_value END) AS categoria,
    MAX(CASE WHEN ca.attribute_name = 'Duración' THEN ca.decimal_value::NUMERIC END) AS duracion, -- Casteo a NUMERIC para evitar errores de tipo
    c.created_at AS fecha_creacion,
    c.updated_at AS fecha_actualizacion,
    '1900-01-01' AS valido_desde,
    '9999-12-31' AS valido_hasta,
    TRUE AS es_actual
FROM
    reto_sql.main.contents_historical c
LEFT JOIN
    reto_sql.main.content_attributes_historical ca ON c.content_id = ca.content_id
GROUP BY
    c.content_id, c.account_id, c.title, c.description, c.content_type, c.created_at, c.updated_at;

-- Manejo de casos donde no hay contenido
INSERT INTO OLAP.dim_contenidos (
    id_dim_contenido, id_contenido, titulo, descripcion, tipo_contenido, categoria, duracion, fecha_creacion, fecha_actualizacion,
    valido_desde, valido_hasta, es_actual
)
SELECT
    'NA' AS id_dim_contenido , NULL AS id_contenido, 'No Aplica' AS titulo, 'No Aplica' AS descripcion, 'No Aplica' AS tipo_contenido,
    'No Aplica' AS categoria, NULL AS duracion, NULL AS fecha_creacion, NULL AS fecha_actualizacion, TIMESTAMP '1900-01-01' AS valido_desde, 
    TIMESTAMP '9999-12-31' AS valido_hasta, TRUE AS es_actual
WHERE NOT EXISTS (SELECT 1 FROM OLAP.dim_contenidos WHERE id_dim_contenido = 'NA'); -- Evita duplicados del registro "No Aplica"

DELETE FROM OLAP.dim_contenidos

COMMENT ON COLUMN OLAP.dim_contenidos.id_dim_contenido IS 'Clave subrogada generada como un hash Ãºnico para identificar de manera Ãºnica cada registro en la dimensiÃ³n, basada en mÃºltiples atributos del contenido. Permite la identificaciÃ³n y vinculaciÃ³n con tablas de hechos';
COMMENT ON COLUMN OLAP.dim_contenidos.id_contenido IS 'Clave natural del contenido, proveniente del sistema fuente.';
COMMENT ON COLUMN OLAP.dim_contenidos.titulo IS 'TÃ­tulo del contenido, obtenido directamente de la tabla contents, que puede ser utilizado para bÃºsquedas y visualizaciÃ³n en reportes (SCD Tipo 1)';
COMMENT ON COLUMN OLAP.dim_contenidos.descripcion IS 'DescripciÃ³n detallada del contenido, que proporciona contexto adicional y puede ser utilizado en anÃ¡lisis de texto o para enriquecer la informaciÃ³n presentada (SCD Tipo 1)';
COMMENT ON COLUMN OLAP.dim_contenidos.tipo_contenido IS 'Tipo de contenido, como video, artÃ­culo, etc., que puede cambiar con el tiempo y requiere un seguimiento histÃ³rico (SCD Tipo 2)';
COMMENT ON COLUMN OLAP.dim_contenidos.categoria IS 'CategorÃ­a del contenido, que clasifica el contenido en diferentes grupos temÃ¡ticos y puede cambiar con el tiempo, requiriendo un seguimiento histÃ³rico (SCD Tipo 2). Proviene de la tabla content_attributes';
COMMENT ON COLUMN OLAP.dim_contenidos.duracion IS 'DuraciÃ³n del contenido en minutos, que puede ser relevante para anÃ¡lisis de consumo y planificaciÃ³n de tiempo, y puede cambiar con el tiempo (SCD Tipo 2). Proviene de la tabla content_attributes';
COMMENT ON COLUMN OLAP.dim_contenidos.fecha_creacion IS 'Fecha en la que el contenido fue creado originalmente en el sistema fuente, utilizada para anÃ¡lisis histÃ³ricos y auditorÃ­as';
COMMENT ON COLUMN OLAP.dim_contenidos.fecha_actualizacion IS 'Ãšltima fecha en la que el contenido fue actualizado en el sistema fuente, utilizada para identificar cambios recientes y mantener la informaciÃ³n actualizada';
COMMENT ON COLUMN OLAP.dim_contenidos.valido_desde IS 'Fecha de inicio de validez del registro en la dimensiÃ³n, utilizada para gestionar la historia de cambios en los atributos del contenido (SCD Tipo 2)';
COMMENT ON COLUMN OLAP.dim_contenidos.valido_hasta IS 'Fecha de fin de validez del registro en la dimensiÃ³n, utilizada para gestionar la historia de cambios en los atributos del contenido (SCD Tipo 2)';
COMMENT ON COLUMN OLAP.dim_contenidos.es_actual IS 'Indicador booleano que seÃ±ala si el registro es la versiÃ³n actual del contenido, utilizado para facilitar consultas y anÃ¡lisis de la versiÃ³n vigente';

-- Insertar registros iniciales usando contents_historical y content_attributes_historical
INSERT INTO OLAP.dim_contenidos (
    id_dim_contenido, id_contenido, titulo, descripcion, tipo_contenido, categoria, duracion, fecha_creacion, fecha_actualizacion, valido_desde, valido_hasta, es_actual
)
SELECT
    md5(CAST(c.content_id AS VARCHAR) || c.title || c.description || c.content_type || COALESCE(ca1.string_value, '') || CAST(COALESCE(ca2.decimal_value, -1) AS VARCHAR) || CAST(c.created_at AS VARCHAR) || CAST(c.updated_at AS VARCHAR)) AS id_dim_contenido,
    c.content_id AS id_contenido,
    c.title AS titulo,
    c.description AS descripcion,
    c.content_type AS tipo_contenido,
    ca1.string_value AS categoria, -- Atributo "CategorÃ­a"
    ca2.decimal_value AS duracion, -- Atributo "DuraciÃ³n"
    c.created_at AS fecha_creacion,
    c.updated_at AS fecha_actualizacion,
    '1900-01-01' AS valido_desde,
    '9999-12-31' AS valido_hasta,
    TRUE AS es_actual
FROM
    contents_historical c
LEFT JOIN
    content_attributes_historical ca1
ON
    c.content_id = ca1.content_id AND ca1.attribute_name = 'CategorÃ­a'
LEFT JOIN
    content_attributes_historical ca2
ON
    c.content_id = ca2.content_id AND ca2.attribute_name = 'DuraciÃ³n'
UNION ALL
SELECT
    'NA', NULL, 'No Aplica', NULL, 'NA', NULL, NULL, '1900-01-01', '1900-01-01', '1900-01-01', '9999-12-31', TRUE
;

DROP TABLE IF EXISTS OLAP.dim_tiempo_dia;

CREATE TABLE OLAP.dim_tiempo_dia (
    id_dim_tiempo_dia INT PRIMARY KEY,    -- Clave primaria en formato yyyyMMdd
    fecha DATE NOT NULL,                  -- Fecha completa
    anio INT,                             -- AÃ±o
    mes INT,                              -- Mes
    dia INT,                              -- DÃ­a
    dia_semana VARCHAR,                   -- DÃ­a de la semana (ej. "Lunes")
    es_fin_de_semana BOOLEAN,             -- Indicador de fin de semana
    trimestre INT                         -- Trimestre del aÃ±o
);

INSERT INTO OLAP.dim_tiempo_dia (
    id_dim_tiempo_dia, fecha, anio, mes, dia, dia_semana, es_fin_de_semana, trimestre
)
SELECT
    CAST(strftime(fecha, '%Y%m%d') AS INT) AS id_dim_tiempo_dia, -- Clave primaria en formato yyyyMMdd
    fecha,
    EXTRACT(YEAR FROM fecha) AS anio,
    EXTRACT(MONTH FROM fecha) AS mes,
    EXTRACT(DAY FROM fecha) AS dia,
    strftime(fecha, '%w') AS dia_semana, -- DÃ­a de la semana como nÃºmero (0 = Domingo, 6 = SÃ¡bado)
    CASE WHEN EXTRACT(DOW FROM fecha) IN (0, 6) THEN TRUE ELSE FALSE END AS es_fin_de_semana,
    CEIL(EXTRACT(MONTH FROM fecha) / 3.0) AS trimestre -- Trimestre del aÃ±o
FROM (
    SELECT DATE '2010-01-01' + INTERVAL (x - 1) DAY AS fecha
    FROM range(1, (DATE '2030-12-31' - DATE '2010-01-01')::INT + 2) AS t(x)
) AS fechas;