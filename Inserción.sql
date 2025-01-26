BEGIN TRANSACTION;

-- Cerrar registros actuales si hay cambios
UPDATE OLAP.dim_cuentas
SET
    valido_hasta = cast(daily.updated_at - INTERVAL '1 day' as date), -- aquÃ­ generalmente se usa current_date en vez de updated_at, depende que tan confiable es ese campo
    es_actual = FALSE
FROM
    reto_sql.main.accounts_daily AS daily
WHERE
    OLAP.dim_cuentas.id_cuenta = daily.account_id
    AND (
        OLAP.dim_cuentas.nombre_cuenta <> daily.account_name OR
        OLAP.dim_cuentas.correo <> daily.email
    )
    AND OLAP.dim_cuentas.es_actual = TRUE;

-- Insertar nuevos registros para los cambios detectados
INSERT INTO OLAP.dim_cuentas (
    id_dim_cuenta, id_cuenta, nombre_cuenta, correo, fecha_creacion, fecha_actualizacion, valido_desde, valido_hasta, es_actual
)
SELECT
    md5(CAST(daily.account_id AS VARCHAR) || daily.account_name || daily.email || CAST(daily.created_at AS VARCHAR) || CAST(daily.updated_at AS VARCHAR)) AS id_dim_cuenta,
    daily.account_id AS id_cuenta,
    daily.account_name AS nombre_cuenta,
    daily.email AS correo,
    daily.created_at AS fecha_creacion,
    daily.updated_at AS fecha_actualizacion,
    daily.updated_at::date AS valido_desde,
    '9999-12-31' AS valido_hasta,
    TRUE AS es_actual
FROM
    reto_sql.main.accounts_daily AS daily
LEFT JOIN
    OLAP.dim_cuentas AS dim
ON
    daily.account_id = dim.id_cuenta
    AND dim.es_actual = TRUE
WHERE
    dim.nombre_cuenta <> daily.account_name
    OR dim.correo <> daily.email
    OR dim.id_cuenta IS NULL;

COMMIT;