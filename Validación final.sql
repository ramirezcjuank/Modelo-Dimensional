-----Validaciones
-- Integridad referencial
SELECT *
FROM OLAP.fact_creacion_contenido f
LEFT JOIN OLAP.dim_contenidos d ON f.id_dim_contenido = d.id_dim_contenido
WHERE d.id_dim_contenido IS NULL;

-- Duplicados
SELECT id_dim_contenido, COUNT(*)
FROM OLAP.dim_contenidos
GROUP BY id_dim_contenido
HAVING COUNT(*) > 1;

-- Rango de valores
SELECT min(duracion), max(duracion)
FROM OLAP.dim_contenidos;

-- Valores vÃ¡lidos
SELECT *
FROM OLAP.dim_contenidos
WHERE duracion>10*60; -- 10 minutos

--Valores aceptados
SELECT DISTINCT categoria
FROM OLAP.dim_contenidos
WHERE categoria NOT IN ('Arte','Ciencia','Economía');

WITH cambios_suscripcion AS (
    SELECT
        id_dim_cuenta,
        COUNT(DISTINCT id_dim_suscripcion) AS cantidad_cambios
    FROM
        OLAP.fact_cuentas_suscripcion
    GROUP BY
        id_dim_cuenta
    HAVING
        COUNT(DISTINCT id_dim_suscripcion) > 1
)
SELECT
    COUNT(*) AS cantidad_cuentas_con_cambios
FROM
    cambios_suscripcion;

--Contenidos mÃ¡s populares por suscripcion
   SELECT
     ds.nombre_suscripcion,
     dc.tipo_contenido,
     COUNT(*) AS cantidad_contenidos
 FROM
     OLAP.fact_creacion_contenido AS fcc
 LEFT JOIN
     OLAP.dim_contenidos AS dc ON fcc.id_dim_contenido = dc.id_dim_contenido
 LEFT JOIN
     OLAP.dim_cuentas AS dcu ON fcc.id_dim_cuenta = dcu.id_dim_cuenta
 LEFT JOIN
     OLAP.fact_cuentas_suscripcion AS fcs ON dcu.id_dim_cuenta = fcs.id_dim_cuenta
 LEFT JOIN
     OLAP.dim_suscripciones AS ds ON fcs.id_dim_suscripcion = ds.id_dim_suscripcion
 WHERE ds.id_dim_suscripcion<>'NA'
 GROUP BY
     ds.nombre_suscripcion,
     dc.tipo_contenido
 ORDER BY
     ds.nombre_suscripcion,
     cantidad_contenidos DESC;