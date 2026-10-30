# Genera provisioning/dashboards/nids.json. Se versiona el generador y el
# JSON resultante; si se cambia un panel, editar aquí y volver a correr:
#   powershell -File grafana/generar_dashboard.ps1

$ds = @{ type = 'prometheus'; uid = 'prometheus' }
$script:id = 0
$script:y = 0
$panels = New-Object System.Collections.ArrayList

function Nuevo-Id { $script:id++; return $script:id }

function Fila($titulo) {
    [void]$panels.Add([ordered]@{
        id = (Nuevo-Id); type = 'row'; title = $titulo; collapsed = $false
        gridPos = @{ h = 1; w = 24; x = 0; y = $script:y }; panels = @()
    })
    $script:y += 1
}

function Objetivos($consultas) {
    $refs = 'A','B','C','D','E','F'
    # @(@('expr','leyenda')) se aplana a dos strings: lo re-envolvemos.
    if ($consultas.Count -gt 0 -and $consultas[0] -is [string]) { $consultas = ,$consultas }
    $i = 0
    foreach ($c in $consultas) {
        [ordered]@{ refId = $refs[$i]; datasource = $ds; expr = $c[0]; legendFormat = $c[1] }
        $i++
    }
}

function Panel($tipo, $titulo, $x, $w, $h, $consultas, $unidad = 'short', $extra = @{}) {
    $p = [ordered]@{
        id = (Nuevo-Id); type = $tipo; title = $titulo; datasource = $ds
        gridPos = @{ h = $h; w = $w; x = $x; y = $script:y }
        targets = @(Objetivos $consultas)
        fieldConfig = @{ defaults = @{ unit = $unidad }; overrides = @() }
        options = @{}
    }
    foreach ($k in $extra.Keys) { $p[$k] = $extra[$k] }
    [void]$panels.Add($p)
}

$statOpts = @{ options = @{ reduceOptions = @{ calcs = @('lastNotNull') }; colorMode = 'value'; graphMode = 'area' } }

# --- Resumen ejecutivo ---------------------------------------------------
Fila 'Resumen en vivo - red del laboratorio'
Panel 'stat' 'Throughput (Mbps)' 0 4 4 @(,@('sum(nids_trafico_kpi{kpi="mbps"})','')) 'none' $statOpts
Panel 'stat' 'Hosts activos (min)' 4 4 4 @(,@('sum(nids_trafico_kpi{kpi="hosts_activos"})','')) 'none' $statOpts
Panel 'stat' 'Paquetes / min' 8 4 4 @(,@('sum(nids_trafico_kpi{kpi="paquetes_min"})','')) 'short' $statOpts
Panel 'stat' 'Trafico cifrado (TLS)' 12 4 4 @(,@('avg(nids_trafico_kpi{kpi="pct_trafico_cifrado"})','')) 'percent' $statOpts
Panel 'stat' 'Incidentes abiertos' 16 4 4 @(,@('sum(nids_incidentes_abiertos)','')) 'none' $statOpts
Panel 'stat' 'Anomalias detectadas ahora' 20 4 4 @(,@('sum(nids_trafico_anomalia)','')) 'none' (@{
    options = @{ reduceOptions = @{ calcs = @('lastNotNull') }; colorMode = 'background'; graphMode = 'none' }
    fieldConfig = @{ defaults = @{ unit = 'none'; thresholds = @{ mode = 'absolute'; steps = @(@{ color = 'green'; value = $null }, @{ color = 'red'; value = 1 }) } }; overrides = @() }
})
$script:y += 4

# --- Una fila por dimension U2 (indicador + prediccion + baseline) -------
$dimensiones = @(
    @{ ind = 'bytes';  titulo = 'Dimension 1 (David Romero) - Volumen de trafico (bytes/min)'; unidad = 'decbytes' },
    @{ ind = 'flujos'; titulo = 'Dimension 2 (Nick Mayta) - Conexiones nuevas (flujos/min)'; unidad = 'short' },
    @{ ind = 'syn';    titulo = 'Dimension 3 (Henyelrey Garcia) - Intentos de conexion TCP SYN (por min)'; unidad = 'short' },
    @{ ind = 'dns';    titulo = 'Dimension 4 (Jhan Ramos) - Consultas DNS (flujos al puerto 53 por min)'; unidad = 'short' }
)
foreach ($d in $dimensiones) {
    $i = $d.ind
    Fila $d.titulo
    Panel 'timeseries' 'Real vs prediccion del minuto (modelo y persistencia)' 0 14 8 @(
        @("sum(nids_trafico_valor{indicador=`"$i`"})", 'Real'),
        @("sum(nids_trafico_prediccion{indicador=`"$i`",modelo=`"ml`"} offset 1m)", 'Prediccion modelo'),
        @("sum(nids_trafico_prediccion{indicador=`"$i`",modelo=`"persistencia`"} offset 1m)", 'Baseline persistencia'),
        @("sum(nids_trafico_umbral_anomalia{indicador=`"$i`"})", 'Umbral de anomalia')
    ) $d.unidad
    Panel 'timeseries' 'Error absoluto en vivo' 14 5 8 @(
        @("sum by (modelo) (nids_trafico_error_abs{indicador=`"$i`"})", '{{modelo}}')
    ) $d.unidad
    Panel 'bargauge' 'RMSE en test (ultimo 20 %, nunca visto)' 19 5 5 @(
        @("nids_modelo_rmse{indicador=`"$i`"}", '{{modelo}}')
    ) $d.unidad (@{ options = @{ orientation = 'horizontal'; reduceOptions = @{ calcs = @('lastNotNull') } } })
    $script:y += 5
    Panel 'stat' 'Mejora vs persistencia' 19 5 3 @(
        ,@("nids_modelo_mejora_vs_persistencia_pct{indicador=`"$i`"}", '')
    ) 'percent' $statOpts
    $script:y += 3
}

function PanelTabla($titulo, $x, $w, $h, $expr, $ocultar, $orden) {
    $p = [ordered]@{
        id = (Nuevo-Id); type = 'table'; title = $titulo; datasource = $ds
        gridPos = @{ h = $h; w = $w; x = $x; y = $script:y }
        targets = @([ordered]@{ refId = 'A'; datasource = $ds; expr = $expr; format = 'table'; instant = $true })
        fieldConfig = @{ defaults = @{}; overrides = @() }
        options = @{ showHeader = $true; sortBy = @(@{ displayName = $orden; desc = $true }) }
        transformations = @(@{ id = 'organize'; options = @{ excludeByName = $ocultar } })
    }
    [void]$panels.Add($p)
}
$ocultarBase = @{ Time = $true; __name__ = $true; instance = $true; job = $true }

# --- Deteccion de anomalias por flujo (K-Means) ---------------------------
Fila 'Deteccion de anomalias por flujo (K-Means entrenado con TRCU.csv)'
Panel 'stat' 'Flujos anomalos (ultimo micro-batch)' 0 4 6 @(,@('sum(nids_flujos_anomalos)','')) 'none' (@{
    options = @{ reduceOptions = @{ calcs = @('lastNotNull') }; colorMode = 'background'; graphMode = 'area' }
    fieldConfig = @{ defaults = @{ unit = 'none'; thresholds = @{ mode = 'absolute'; steps = @(@{ color = 'green'; value = $null }, @{ color = 'orange'; value = 1 }, @{ color = 'red'; value = 10 }) } }; overrides = @() }
})
Panel 'stat' '% de flujos anomalos' 4 4 6 @(,@('avg(nids_flujos_pct_anomalos)','')) 'percent' $statOpts
Panel 'timeseries' 'Puntaje maximo vs umbral (distancia al centro del cluster)' 8 16 6 @(
    @('max(nids_flujos_puntaje_max)', 'puntaje maximo del batch'),
    @('max(nids_flujos_umbral)', 'umbral (percentil 99.5 del historico)')
) 'none'
$script:y += 6
PanelTabla 'Flujos mas sospechosos (IP seudonimizadas)' 0 16 8 'nids_flujo_sospechoso_puntaje' $ocultarBase 'Value'
Panel 'bargauge' 'Silhouette por k (eleccion de k)' 16 8 8 @(
    ,@('nids_anomalias_silhouette', 'k={{k}}')
) 'none' (@{ options = @{ orientation = 'horizontal'; reduceOptions = @{ calcs = @('lastNotNull') } } })
$script:y += 8

# --- Evento empresarial ----------------------------------------------------
Fila 'Evento empresarial - incidentes de seguridad (mesa de TI + abiertos por el modelo)'
Panel 'timeseries' 'Incidentes abiertos por severidad' 0 8 7 @(
    ,@('sum by (severidad) (nids_incidentes_abiertos)', '{{severidad}}')
) 'none' (@{ fieldConfig = @{ defaults = @{ unit = 'none'; custom = @{ stacking = @{ mode = 'normal' }; fillOpacity = 40 } }; overrides = @() } })
Panel 'timeseries' 'Incidentes abiertos por origen' 8 6 7 @(
    ,@('sum by (origen) (nids_incidentes_abiertos)', '{{origen}}')
) 'none'
Panel 'bargauge' 'Abiertos en los ultimos 5 min (ventana deslizante)' 14 5 7 @(
    ,@('sum by (severidad) (nids_incidentes_ultimos_5min)', '{{severidad}}')
) 'none' (@{ options = @{ orientation = 'horizontal'; reduceOptions = @{ calcs = @('lastNotNull') } } })
Panel 'stat' 'MTTR (min, ultimos 50)' 19 5 3 @(,@('nids_incidentes_mttr_minutos','')) 'm' $statOpts
$script:y += 3
Panel 'stat' 'Abiertos por el modelo (total)' 19 5 4 @(,@('max(nids_incidentes_automaticos_total)','')) 'none' $statOpts
$script:y += 4

# --- Modelos -------------------------------------------------------------
Fila 'Modelos de serie de tiempo (validacion cruzada temporal + test)'
PanelTabla 'Ganador por indicador' 0 8 6 'nids_modelo_ganador_info' (@{ Time = $true; __name__ = $true; instance = $true; job = $true; Value = $true }) 'indicador'
Panel 'bargauge' 'Mejora del RMSE vs persistencia (test)' 8 8 6 @(
    ,@('nids_modelo_mejora_vs_persistencia_pct', '{{indicador}}')
) 'percent' (@{ options = @{ orientation = 'horizontal'; reduceOptions = @{ calcs = @('lastNotNull') } } })
Panel 'bargauge' 'R2 en test por modelo' 16 8 6 @(
    ,@('nids_modelo_r2', '{{indicador}} / {{modelo}}')
) 'none' (@{ options = @{ orientation = 'horizontal'; reduceOptions = @{ calcs = @('lastNotNull') } } })
$script:y += 6

# --- KPIs de composicion del trafico --------------------------------------
Fila 'KPIs del flujo de red'
Panel 'timeseries' 'Composicion del trafico' 0 12 7 @(
    @('avg(nids_trafico_kpi{kpi="pct_trafico_cifrado"})', '% cifrado (443)'),
    @('avg(nids_trafico_kpi{kpi="pct_trafico_externo"})', '% hacia/desde internet'),
    @('avg(nids_trafico_kpi{kpi="pct_tcp"})', '% TCP')
) 'percent'
Panel 'timeseries' 'Consultas DNS y hosts activos por minuto' 12 12 7 @(
    @('sum(nids_trafico_kpi{kpi="consultas_dns_min"})', 'DNS / min'),
    @('sum(nids_trafico_kpi{kpi="hosts_activos"})', 'Hosts activos')
) 'short'
$script:y += 7

# --- Observabilidad del pipeline ------------------------------------------
Fila 'Observabilidad del pipeline (Kafka -> Spark -> Grafana)'
Panel 'timeseries' 'Mensajes/s publicados por topico (Kafka)' 0 8 7 @(
    ,@('sum by (topic) (rate(kafka_topic_partition_current_offset{topic!~"__.*"}[2m]))', '{{topic}}')
) 'short'
Panel 'timeseries' 'Spark: filas/s procesadas vs entrada' 8 8 7 @(
    @('nids_spark_filas_procesadas_por_seg', 'procesadas {{consulta}}'),
    @('nids_spark_filas_entrada_por_seg', 'entrada {{consulta}}')
) 'short'
Panel 'timeseries' 'Lag Kafka -> Spark (registros pendientes)' 16 8 7 @(
    ,@('nids_spark_lag_kafka_registros', '{{consulta}}')
) 'short'
$script:y += 7
Panel 'timeseries' 'Duracion de micro-batch (ms)' 0 8 7 @(
    ,@('nids_spark_duracion_batch_ms', '{{consulta}}')
) 'ms'
Panel 'timeseries' 'Latencia extremo a extremo (fin de ventana -> Grafana)' 8 8 7 @(
    @('nids_trafico_latencia_ventana_segundos', 'ventana trafico'),
    @('nids_ingestor_retraso_segundos', 'ingestor PCAP')
) 's'
Panel 'stat' 'Segundos desde el ultimo archivo del sensor' 16 8 7 @(
    ,@('time() - nids_ingestor_ultimo_archivo_timestamp', '')
) 's' (@{
    options = @{ reduceOptions = @{ calcs = @('lastNotNull') }; colorMode = 'background'; graphMode = 'none' }
    fieldConfig = @{ defaults = @{ unit = 's'; thresholds = @{ mode = 'absolute'; steps = @(@{ color = 'green'; value = $null }, @{ color = 'orange'; value = 120 }, @{ color = 'red'; value = 300 }) } }; overrides = @() }
})
$script:y += 7

# --- Sensor IoT ------------------------------------------------------------
Fila 'Sensor IoT - Raspberry Pi 5 (telemetria por MQTT)'
Panel 'timeseries' 'Temperatura y CPU del sensor' 0 8 7 @(
    @('nids_pi_temp_prom', 'temp prom (C)'),
    @('nids_pi_temp_max', 'temp max (C)'),
    @('nids_pi_cpu_prom', 'CPU %')
) 'short'
Panel 'timeseries' 'Interfaz espejo vs pipeline (Mbps)' 8 8 7 @(
    @('nids_pi_mbps_interfaz', 'interfaz del sensor'),
    @('sum(nids_trafico_kpi{kpi="mbps"})', 'procesado por Spark')
) 'none'
Panel 'stat' 'Disco del sensor libre' 16 4 7 @(,@('nids_pi_disco_libre_gb','')) 'decgbytes' $statOpts
Panel 'stat' 'Paquetes descartados (min)' 20 4 7 @(,@('nids_pi_descartados','')) 'short' $statOpts
$script:y += 7

# --- Costos ----------------------------------------------------------------
Fila 'Uso de la VM (base de la estimacion de costos)'
Panel 'timeseries' 'CPU y memoria de la VM' 0 12 7 @(
    @('100 * (1 - avg(rate(node_cpu_seconds_total{mode="idle"}[2m])))', 'CPU %'),
    @('100 * (1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)', 'RAM %')
) 'percent'
Panel 'timeseries' 'Red y disco de la VM' 12 12 7 @(
    @('sum(rate(node_network_receive_bytes_total{device!~"lo|veth.*|docker.*|br-.*"}[2m]))', 'entrada'),
    @('sum(rate(node_network_transmit_bytes_total{device!~"lo|veth.*|docker.*|br-.*"}[2m]))', 'salida')
) 'Bps'
$script:y += 7

$dashboard = [ordered]@{
    uid = 'nids-campus'; title = 'NIDS campus - trafico de red en tiempo real'
    timezone = 'browser'; refresh = '30s'; schemaVersion = 39; editable = $true
    time = @{ from = 'now-3h'; to = 'now' }
    tags = @('nids', 'big-data', 'u2')
    panels = $panels
}

$salida = Join-Path $PSScriptRoot 'provisioning/dashboards/nids.json'
$json = $dashboard | ConvertTo-Json -Depth 30
[System.IO.File]::WriteAllText($salida, $json, (New-Object System.Text.UTF8Encoding $false))
"Dashboard generado: $salida ($($panels.Count) paneles)"
