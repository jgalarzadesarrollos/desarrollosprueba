WITH BaseAndes AS 
  (SELECT 
    a.*, b.Departamento, b.Provincia, b.Distrito, b.LIMAoPROV, d.* --, E.TiendaDestino, E.FacilityCode2, E.BU_DESTINO_group2
  FROM 
    (SELECT * EXCEPT (Departamento, Provincia, Distrito), UPPER(CONCAT(Departamento, Provincia, Distrito)) key , concat(left(bu3,1),pickupPointId) keyPUPoint,
    FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.Catalyst.vw_Andes_Fact_Ventas`)  a
  LEFT JOIN `tc-sc-bi-bigdata-hdpe-pjx-dev.BI_HD_Peru_2.master-ubigeo-mquiliche` b
    ON a.key = b.Concatenado
  LEFT JOIN   
    (SELECT CONCAT(LEFT(BU_Origen,1), Facility) KEY_PU, NombreNodo TiendaPU,Facility FacilityCode,BU_Origen,BU_Destino_Group,BU_Destino
    --,* EXCEPT(Facility,NombreNodo, Ubigeo, Direcci__n, Latitud, Longitud, _3PL, Departamento, Provincia, Distrito)
      FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.BI_HD_Peru_2.PU_Points_fcom`) d
    ON d.KEY_PU = a.keyPUPoint
  --6591217
  WHERE 1 = 1
  AND deliveryMethod = 'COLLECT'
  AND date_delivered_ch IS NOT NULL

  -- LIMIT 100
  )
  --select distinct bu3,keyPUPoint from BaseAndes where bu3 like "G%"
  ,

Base AS (
  SELECT  distinct
    deliveryOrderGroupId,
    Folio_BU,
    seller_type2,
    MAX(event_status_v1) event_status,
    KEY_PU,
    FacilityCode,
    orderNumber,
    TiendaPU,
    BU_Origen,
    BU_Destino_Group,
    BU_Destino,
    CURRENT_DATETIME('America/Lima') actualizacion,
    MIN(date_charged) date_charged,
    MIN(DATE(date_delivered_ch)) date_delivered_ch,
    createdAt,
    SUM(units) unidades,
    bu3,
   -- TiendaDestino,
   -- BU_Destino_Group2,
  FROM BaseAndes
  GROUP BY 1,2,3,5,6,7,8,9,10,11,12,15,bu3 --,TiendaDestino,BU_Destino_Group2
),


CONTEO AS (
  SELECT 
    deliveryOrderGroupId,
    CAST(COUNT(*) AS FLOAT64) DivideBy
  FROM Base
  GROUP BY 1),

  CONTEO2 AS (
  SELECT 
    Folio_BU,
    CAST(COUNT(*) AS FLOAT64) DivideBy2
  FROM Base
  GROUP BY 1),

  PAGO AS (
  SELECT 
    *,
    concat(left(BU_Origen,1),Facility) KEY
  FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.BI_HD_Peru_2.LogisticaCCollect` ),



PAGO_SinProrr AS (
  SELECT 
    B.*,
    C.DivideBy,
    C2.DivideBy2,
    P.Tienda TiendaPago,
    P.BU_Origen BU_OrigenPago,
    P.Bu_Destino Bu_DestinoPago,
    P.Logistica,
    P.KEY KEYPago,
    
    -- Lógica de pago: Logística de Entrega
      -- 1.5 si BU origen = BU Destino 
      -- Lógica según Excel 
      
    CASE
      WHEN B.BU_Origen = B.BU_Destino_Group 
        THEN 1.8
      WHEN KEY_PU IS NULL 
        THEN NULL
      ELSE Pago_BU_Destino + Pago_BU_Origen
    END AS Logis_De_EntregaCC_TOT,

    CASE
      WHEN B.BU_Origen = B.BU_Destino_Group 
        THEN B.BU_Origen       
      WHEN KEY_PU IS NULL 
        THEN NULL
      WHEN Pago_BU_Destino > 0
        THEN B.BU_Destino 
      WHEN Pago_BU_Origen > 0
        THEN B.BU_Origen 
      ELSE NULL
    END AS BU_Logis_De_EntregaCC,

    -- Lógica de pago: Servicio de Entrega
      -- 3.5 si BU destino = Fala, Sodimac o Tottus 
      -- y se paga al destino

    CASE
      WHEN B.BU_Destino_Group IN ('Falabella', 'Sodimac', 'Tottus', 'Mall Plaza') 
        THEN 3.5
      ELSE NULL
    END AS Serv_Entrega_TOT,

    CASE
      WHEN B.BU_Destino_Group IN ('Falabella', 'Sodimac', 'Tottus', 'Mall Plaza') 
        THEN B.BU_Destino_Group
      ELSE NULL
    END AS BU_Serv_Entrega, 

  FROM BASE B
  LEFT JOIN CONTEO C  
    ON B.deliveryOrderGroupId = C.deliveryOrderGroupId
  LEFT JOIN CONTEO2 C2
    ON B.Folio_BU = C2.Folio_BU
  LEFT JOIN PAGO P 
    ON P.KEY = B.KEY_PU
),

----folio tms--------------
TMS AS(
  SELECT
  DISTINCT(numero_folio)
  FROM tc-sc-bi-bigdata-hdpe-pjx-dev.TMS.TMS_OPS),

TablaFinal AS (
SELECT 
  PAGO_SinProrr.*, 
  CASE
    WHEN BU_Destino_Group IN ('Sodimac', 'Tottus', 'Mall Plaza') THEN SAFE_DIVIDE(Logis_De_EntregaCC_TOT, DivideBy) 
    WHEN BU_Destino_Group IN ('Falabella') THEN SAFE_DIVIDE(Logis_De_EntregaCC_TOT, DivideBy2) 
  END AS Logis_De_EntregaCC ,
  CASE
    WHEN BU_Destino_Group IN ('Sodimac', 'Tottus', 'Mall Plaza') THEN SAFE_DIVIDE(Serv_Entrega_TOT, DivideBy) 
    WHEN BU_Destino_Group IN ('Falabella') THEN SAFE_DIVIDE(Serv_Entrega_TOT, DivideBy2)
  END AS Serv_Entrega,
  if(TMS.numero_folio = PAGO_SinProrr.Folio_BU,1,0) PASO_POR_TMS
FROM PAGO_SinProrr 
LEFT JOIN  TMS on TMS.numero_folio = PAGO_SinProrr.Folio_BU
---WHERE date_delivered_ch BETWEEN '2022-08-01' AND '2022-08-15'
---WHERE tIENDA = 'Maestro Comas'
)

SELECT 
TablaFinal.*,
if(TablaFinal.Folio_BU is null,"Folio por validar","Validado") validacion_cruce
FROM TablaFinal
WHERE 1=1
--and crossdockRuedas = 1
-- AND PASO_POR_TMS = 0
-- AND Logis_De_EntregaCC <> 1.5
---and BU_Serv_Entrega = 'Mall Plaza'
---AND TiendaPU IS NULL
-- limit 10 
---AND DivideBy <> 1
-- AND deliveryOrderGroupId = '149079926572'
------group by 1,2,3,4,6,8

