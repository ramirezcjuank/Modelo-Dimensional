/* 
Carga por chequeo de duplicados.
Insertamos solo los registros que no existen en la tabla destino 
y filtramos los que tienen valores nulos para evitar conflictos con 'NA'.
*/

INSERT INTO OLAP.fact_creacion_contenido (
    id_dim_contenido,
    id_dim_cuenta,
    id_dim_tiempo_dia
)
SELECT
    COALESCE(dc.id_dim_contenido, 'NA') AS id_dim_contenido,
    COALESCE(dcu.id_dim_cuenta, 'NA') AS id_dim_cuenta,
    CAST(strftime(cd.created_at, '%Y%m%d') AS INT) AS id_dim_tiempo_dia
FROM
    main.contents_daily cd
LEFT JOIN
    OLAP.dim_contenidos dc
    ON dc.id_contenido = cd.content_id
    AND cd.created_at::date BETWEEN dc.valido_desde AND dc.valido_hasta -- Calce en periodo de validez
LEFT JOIN
    OLAP.dim_cuentas dcu
    ON dcu.id_cuenta = cd.account_id
    AND cd.created_at::date BETWEEN dcu.valido_desde AND dcu.valido_hasta -- Calce en periodo de validez
WHERE 
    dc.id_dim_contenido IS NOT NULL -- Filtrar registros con nulos en id_dim_contenido
    AND dcu.id_dim_cuenta IS NOT NULL -- Filtrar registros con nulos en id_dim_cuenta
    AND NOT EXISTS (
        SELECT 1
        FROM OLAP.fact_creacion_contenido fc
        WHERE fc.id_dim_contenido = COALESCE(dc.id_dim_contenido, 'NA')
          AND fc.id_dim_cuenta = COALESCE(dcu.id_dim_cuenta, 'NA')
          AND fc.id_dim_tiempo_dia = CAST(strftime(cd.created_at, '%Y%m%d') AS INT)
    );

