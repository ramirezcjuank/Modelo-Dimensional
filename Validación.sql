-- Primero, agrupa las cuentas por id_cuenta y selecciona aquellas que tienen mÃ¡s de una ocurrencia.
-- Es decir las que sufrieron cambios en el tiempo.
SELECT
id_cuenta,
id_dim_cuenta,
nombre_cuenta,
correo,
valido_desde,
valido_hasta,
es_actual
FROM
OLAP.dim_cuentas
WHERE
id_cuenta IN (
    SELECT id_cuenta
    FROM OLAP.dim_cuentas
    GROUP BY id_cuenta
    HAVING COUNT(*) > 1
    LIMIT 5
)
ORDER BY
id_cuenta, valido_desde;

-- Validar que los registros con es_actual = TRUE tengan valido_hasta = '9999-12-31'
SELECT
    id_cuenta,
    id_dim_cuenta,
    nombre_cuenta,
    correo,
    valido_desde,
    valido_hasta,
    es_actual
FROM
    OLAP.dim_cuentas
WHERE
    es_actual = TRUE
    AND valido_hasta <> '9999-12-31';

-- Validar que no haya solapamiento en las fechas de validez de un mismo registro
SELECT
    id_cuenta,
    nombre_cuenta,
    correo,
    valido_desde,
    valido_hasta,
    es_actual
FROM
    OLAP.dim_cuentas AS a
WHERE
    EXISTS (
        SELECT 1
        FROM OLAP.dim_cuentas AS b
        WHERE
            a.id_cuenta = b.id_cuenta
            AND a.id_dim_cuenta <> b.id_dim_cuenta
            AND a.valido_desde <= b.valido_hasta
            AND a.valido_hasta >= b.valido_desde
    );