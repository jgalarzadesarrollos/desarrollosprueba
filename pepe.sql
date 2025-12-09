INSERT INTO `tc-sc-bi-bigdata-hdpe-pjx-dev.promise.tabla_historica_planning_service`
SELECT
planning_id,
logistic_order_id,
source_order_id,
origin_node_id,
destination_node_id,
transport_info.operator_name,
transport_info.pre_staging_id,
transport_info.staging_id,
event_attr.country,
event_attr.commerce,
dfl_crte_tmst
FROM `tc-sc-bi-bigdata-dfl-prod.trf_corp_corp_dfl_prod.btd_scha_corp_corp_planning_service` actual
WHERE event_attr.country = "PE"
AND NOT EXISTS (
SELECT 1
FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.promise.tabla_historica_planning_service` historico
WHERE historico.planning_id = actual.planning_id
)
