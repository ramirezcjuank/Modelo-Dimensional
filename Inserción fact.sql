DROP TABLE IF EXISTS OLAP.fact_creacion_contenido;

CREATE TABLE OLAP.fact_creacion_contenido (
    id_dim_contenido VARCHAR NOT NULL,      -- Clave forÃ¡nea hacia dim_contenidos
    id_dim_cuenta VARCHAR NOT NULL,         -- Clave forÃ¡nea hacia dim_cuentas
    id_dim_tiempo_dia INT NOT NULL,         -- Clave forÃ¡nea hacia dim_tiempo_dia
    PRIMARY KEY (id_dim_contenido,id_dim_cuenta, id_dim_tiempo_dia), -- CombinaciÃ³n Ãºnica
    FOREIGN KEY (id_dim_contenido) REFERENCES OLAP.dim_contenidos(id_dim_contenido),
    FOREIGN KEY (id_dim_cuenta) REFERENCES OLAP.dim_cuentas(id_dim_cuenta),
    FOREIGN KEY (id_dim_tiempo_dia) REFERENCES OLAP.dim_tiempo_dia(id_dim_tiempo_dia)
);

-- carga historica
INSERT INTO OLAP.fact_creacion_contenido (
    id_dim_contenido,
    id_dim_cuenta,
    id_dim_tiempo_dia
)
SELECT
    coalesce(dc.id_dim_contenido,'NA') as id_dim_contenido,
    coalesce(dcu.id_dim_cuenta,'NA') as id_dim_cuenta,
    CAST(strftime(ch.created_at, '%Y%m%d') AS INT) AS id_dim_tiempo_dia
FROM
     reto_sql.main.contents_historical ch
LEFT JOIN
    OLAP.dim_contenidos dc
    ON dc.id_contenido = ch.content_id
    AND ch.created_at::date BETWEEN dc.valido_desde AND dc.valido_hasta -- Calce en periodo de validez
LEFT JOIN
    OLAP.dim_cuentas dcu
    ON dcu.id_cuenta = ch.account_id
    AND ch.created_at::date BETWEEN dcu.valido_desde AND dcu.valido_hasta -- Calce en periodo de validez
LEFT JOIN
    OLAP.dim_tiempo_dia dt
    ON dt.id_dim_tiempo_dia = CAST(strftime(ch.created_at, '%Y%m%d') AS INT);