require('dotenv').config();
const express = require('express');
const path = require('path');
const sql = require('mssql');
const redis = require('redis');
const cron = require('node-cron');
const utc6Middleware = require('./middlewares/utc6Middleware');

const app = express();

// --- Middleware global para convertir fechas a UTC-6 ---
app.use(utc6Middleware);

// 👉 Servir carpeta "public" como estática
// Cualquier archivo dentro de /public será accesible en /static/
app.use('/static', express.static(path.join(__dirname, './public')));

const UTC_MINUS_6_MS = 6 * 60 * 60 * 1000;
const QUINCE_MINUTOS_MS = 15 * 60 * 1000;

// --- Configuración SQL Server ---
const sqlConfig = {
  user: process.env.SQL_USER,
  password: process.env.SQL_PASSWORD,
  server: process.env.SQL_SERVER,
  database: process.env.SQL_DATABASE,
  port: parseInt(process.env.SQL_PORT, 10),
  options: {
    encrypt: false,
    trustServerCertificate: true,
  },
};

// --- Conexión Redis ---
const redisClient = redis.createClient({
  socket: {
    host: process.env.REDIS_HOST,
    port: parseInt(process.env.REDIS_PORT, 10),
  },
});

redisClient.on('error', (err) => console.error('Error en Redis:', err));

let pool;

async function getPool() {
  if (!pool) {
    pool = await sql.connect(sqlConfig);
  }
  return pool;
}

// --- Función para dividir en chunks ---
function chunkArray(array, size) {
  const chunks = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

// --- Función para sincronizar datos SQL Server -> Redis ---
async function syncDataToRedis() {
  try {
    // Usar UTC puro en logs
    const fechaUTC = new Date().toISOString();
    console.log(`[${fechaUTC}] Iniciando sincronización...`);

    const pool = await getPool();

    // Leer última fecha desde Redis
    const ultimaFechaMsGen = Number(await redisClient.get('View_Datalog_Gen_LAST_DATE')) || 0;
    const fecha = new Date(ultimaFechaMsGen);
    const ultimaFechaGen = fecha.toISOString();

    console.log('Consulta para fechas mayores a:', ultimaFechaGen);

    const query = `SELECT * FROM View_Datalog_Gen WHERE TimestampUTC > '${ultimaFechaGen}' ORDER BY TimestampUTC`;

    const start = Date.now();
    const result = await pool.request().query(query);
    const end = Date.now();

    console.log(`Consulta terminada en ${end - start} ms`);
    console.log(`Filas recibidas: ${result.recordset.length}`);

    if (result.recordset.length === 0) {
      console.log('ℹ️ No hay registros nuevos para insertar.');
      console.log('Sincronización finalizada');
      return;
    }

    // 1. Aplicar reglas ANTES de insertar en Redis
    const datosProcesados = aplicarReglas(result);

    // 2. No convertir a UTC-6, usar UTC directo
    const zaddItems = datosProcesados.map(item => ({
      value: JSON.stringify(item),
      score: new Date(item.TimestampUTC).getTime() // timestamp en ms UTC
    }));

    // 3. Calcular máximo score
    let maxFechaInsertadaGen = -Infinity;
    for (const item of zaddItems) {
      if (item.score > maxFechaInsertadaGen) {
        maxFechaInsertadaGen = item.score;
      }
    }
    if (maxFechaInsertadaGen === -Infinity) {
      maxFechaInsertadaGen = 0;
    }

    // 4. Insertar en Redis en chunks
    const chunks = chunkArray(zaddItems, 1000);

    console.log(`Dividiendo en ${chunks.length} chunks para insertar en Redis...`);

    const startInsert = Date.now();
    for (const [index, chunk] of chunks.entries()) {
      await redisClient.zAdd('View_Datalog_Gen', chunk);
      console.log(`Chunk ${index + 1} de ${chunks.length} insertado`);
    }
    const endInsert = Date.now();

    console.log(`Inserción en Redis terminada en ${endInsert - startInsert} ms`);

    // 5. Guardar última fecha procesada en UTC
    if (maxFechaInsertadaGen > 0) {
      await redisClient.set('View_Datalog_Gen_LAST_DATE', maxFechaInsertadaGen);
      console.log('🕒 Última fecha procesada guardada:', maxFechaInsertadaGen, new Date(maxFechaInsertadaGen).toISOString());
    } else {
      console.log('ℹ️ No se guardaron registros nuevos.');
    }

    console.log('Sincronización finalizada');
  } catch (err) {
    console.error('Error sincronizando datos:', err);
  }
}

// --- Función para aplicar reglas de transformación ---
function aplicarReglas(result) {
  return result.recordset.map(row => {
    let valuesKwh = parseFloat(row.Values_KWH) || 0;

    // 1. Si no está generando => poner en 0
    if (row.EstaGenerando === "FALSE") {
      valuesKwh = 0;
    }

    // 2. Si está generando y es negativo => volver positivo
    if (row.EstaGenerando === "TRUE" && valuesKwh < 0) {
      valuesKwh = Math.abs(valuesKwh);
    }

    // Sobreescribimos el valor en la fila y Convertir KWh → MW
    row.Values_KWH = valuesKwh / 1000;

    if (!row.Maximo_Cogeneracion_MW){
      // 2,35 por cada eolica y son 9 en el cacao
      if (row.Name === 'P.E. Cacao')
        row.Maximo_Cogeneracion_MW = 21.15;
      else if (row.Name === 'P.S. Juanilama')
        row.Maximo_Cogeneracion_MW = 5;
      // 3 MW por cada eolica y son 3 PERN
      else if (row.Name === 'PERN')
        row.Maximo_Cogeneracion_MW = 9;
      // 17,5 MW por cada turbina y son 2 turbinas
      else if (row.Name === 'Canalete_Unidad1')
        row.Maximo_Cogeneracion_MW = 8.75;
      // 17,58 MW por cada turbina y son 2 turbinas
      else if (row.Name === 'BIJAGUA_U1' || row.Name === 'BIJAGUA_U2')
        row.Maximo_Cogeneracion_MW = 8.5;
      else if (row.Name === 'P.S. Huacas')
        row.Maximo_Cogeneracion_MW = 7.23;
    }
    if (!row.Tipo_Cogeneracion && row.Name === 'PERN'){
      row.Tipo_Cogeneracion = 'EOLICO';
    }

    return row; // devolvemos el mismo objeto
  });
}

// Función para convertir fecha ISO/epoch a timestamp en UTC-6
function toUTCMinus6(dateInput) {
  let d = new Date(isNaN(dateInput) ? dateInput : parseInt(dateInput));
  // Restamos 6 horas a UTC
  d = new Date(d.getTime() - (UTC_MINUS_6_MS));
  return d.getTime();
}

// Devolver fechas en UTC-6 (sin restar, en la misma zona que Redis)
function formatUTCMinus6(ts) {
  const d = new Date(ts);
  // Sumamos de nuevo las 6h para que la hora se muestre en UTC-6
  const local = new Date(d.getTime() + (6 * 60 * 60 * 1000));
  return local.toISOString().replace('Z', '-06:00');
}

// --- Cron cada 10 minutos ---
cron.schedule('*/10 * * * *', () => {
  syncDataToRedis();
});

// --- Cron cada 7 días para limpiar Redis ---
cron.schedule('0 0 */7 * *', async () => {
  try {
    console.log(`[${new Date().toISOString()}] Limpiando Redis...`);
    await redisClient.del('View_Datalog_Gen');
    await redisClient.del('View_Datalog_Gen_LAST_DATE');
    console.log('Redis limpiado correctamente');
  } catch (err) {
    console.error('Error limpiando Redis:', err);
  }
});

// 🧪 Endpoints básicos
app.get('/', (req, res) => {
  res.send({ status: 'ok' });
});

app.get('/health', (req, res) => {
  res.send({ healthy: true });
});

// 📊 Endpoint para Grafana: últimos X minutos (ajustando zona horaria)
app.get('/api/scada-service/redis', async (req, res) => {
  const vista = req.query.vista;
  let desde, hasta;
  const ahora = Date.now();

  if (req.dates.fromParam && req.dates.toParam) {
    // Ajustar el timestamp enviado por Grafana (UTC) a hora local
    desde = parseInt(req.dates.fromParam);
    hasta = parseInt(req.dates.toParam);
  } else {
    const minutos = parseInt(req.query.minutos || '60');
    desde = ahora - minutos * 60 * 1000;
    hasta = ahora;
  }
  try {
    //const raw = await redisClient.zrangebyscore(vista, desde, hasta);
    const raw = await redisClient.zRangeByScore(vista, desde, hasta);
    const parsed = raw.map(JSON.parse);

    // Mapear los datos para quitar la 'Z' del timestamp
    const dataWithoutZ = parsed.map(item => {
      // Suponemos que el campo del timestamp se llama 'TimestampUTC'
      if (item.TimestampUTC && typeof item.TimestampUTC === 'string' && item.TimestampUTC.endsWith('Z')) {
        // Elimina la 'Z' y el timestamp ahora será interpretado como local
        item.TimestampUTC = item.TimestampUTC.slice(0, -1);
      }
      return item;
    });

    res.json(dataWithoutZ);
  } catch (err) {
    console.error('❌ Error al leer Redis:', err.message);
    res.status(500).json({ error: err.message });
  }
});


// --- Endpoint para métricas agrupadas ---
app.get('/api/scada-service/metrics', async (req, res) => {
  try {
    const view = req.query.view
    const fromParam = req.query.from;
    const toParam = req.query.to;
    const plantsParam = req.query.plant;

    let fromDate = fromParam
      ? (isNaN(fromParam) ? new Date(fromParam) : new Date(parseInt(fromParam)))
      : new Date(Date.now() - 24 * 60 * 60 * 1000);
    let toDate = toParam
      ? (isNaN(toParam) ? new Date(toParam) : new Date(parseInt(toParam)))
      : new Date();

    if (isNaN(fromDate) || isNaN(toDate)) {
      return res.status(400).json({ error: 'Fechas inválidas' });
    }

    let plantNames = [];
    if (plantsParam) {
      plantNames = plantsParam
        .split(',')
        .map(p => p.trim())
        .filter(p => p.length > 0);
    }

    const pool = await getPool();

    let whereClauses = [`TimestampUTC BETWEEN @fromDate AND @toDate`];
    if (plantNames.length > 0) {
      const plantParams = plantNames.map((_, i) => `@plant${i}`);
      whereClauses.push(`Name IN (${plantParams.join(',')})`);
    }
    const whereClause = whereClauses.join(' AND ');

    const query = `
      SELECT Name, Expr1, Values_KWH, TimestampUTC, Maximo_Cogeneracion_MW, EstaGenerando
      FROM View_Datalog_Gen 
      WHERE ${whereClause} 
      ORDER BY TimestampUTC`;

    const request = pool.request()
      .input('fromDate', sql.DateTime, fromDate)
      .input('toDate', sql.DateTime, toDate);

    plantNames.forEach((plant, i) => {
      request.input(`plant${i}`, sql.VarChar, plant);
    });

    const result = await request.query(query);

    // Procesar los datos según las reglas
    const data = result.recordset
      // Filtrar para no incluir Energia Entregada ni Energia Recibida
      .filter(row => row.Expr1 !== 'Energia Entregada' && row.Expr1 !== 'Energia Recibida')
      .map(row => {
        let value = parseFloat(row.Values_KWH) || 0;

        // Regla EstaGenerando
        if (row.EstaGenerando == "FALSE") {
          value = 0;
        } else if (value < 0) {
          value = Math.abs(value);
        }

        // Convertir KWh → MW
        const valueMW = value / 1000;

        return {
          plant: row.Name,
          tipo: row.Expr1,
          timestamp: row.TimestampUTC,
          maxPotenciaMW: Number((row.Maximo_Cogeneracion_MW || 0)),
          value_MW: Number(row.Values_KWH),
          isGenerated: row.EstaGenerando
        };
      });

    res.json({ from: fromDate, to: toDate, plants: plantNames, count: data.length, data });

  } catch (error) {
    console.error('Error en /api/scada-service/metrics:', error);
    res.status(500).json({ error: 'Error al obtener métricas' });
  }
});

app.get('/api/plantas/suma-15min', async (req, res) => {
  try {
    const fromParam = req.query.from;
    const toParam = req.query.to;

    let fromTimestamp, toTimestamp;

    // 1. Determinar el rango de tiempo
    if (fromParam && toParam) {
      fromTimestamp = toUTCMinus6(fromParam);
      toTimestamp = toUTCMinus6(toParam);
    } else {
      // Si no mandan from/to, calcular últimos 15 min desde el último registro
      const ultimoRegistroRaw = await redisClient.zRange('View_Datalog_Gen', -1, -1);
      if (!ultimoRegistroRaw || ultimoRegistroRaw.length === 0) {
        return res.json({
          sumas: {
            'kWh Entregada': 0,
            'kWh Recibida': 0
          },
          aviso: 'No hay registros en Redis'
        });
      }

      const ultimoRegistro = JSON.parse(ultimoRegistroRaw[0]);
      const horaUltimoMs = new Date(ultimoRegistro.TimestampUTC).getTime();

      toTimestamp = horaUltimoMs;
      fromTimestamp = toTimestamp;
      console.log('🕒 Última fecha en redis:', toTimestamp, new Date(toTimestamp).toISOString());
    }

    // 2. Obtener registros del ZSET dentro del rango
    const registrosRaw = await redisClient.zRangeByScore(
      'View_Datalog_Gen',
      fromTimestamp,
      toTimestamp
    );

    const registros = registrosRaw.map(r => {
      try { return JSON.parse(r); } catch { return null; }
    }).filter(r => r);

    console.log(`Registros filtrados: ${registros.length}`);

    // 3. Inicializar sumas
    const energiaEntregada = {
      'energiaEntregada_MWH': 0,
      'capacidadGeneracion_MW': 0,
      'potenciaOperacion_MW': 0,
      'porcentajeOperacion':0
    };

    const energiaRecibida = {
      'energiaRecibida_MWH': 0
    };

    // 3. Inicializar acumuladores y valores
    let totalEnergiaEntregada = 0;
    let totalEnergiaRecibida = 0;
    let totalPotenciaOperacion = 0;
    let count = 0;
    let totalCapacidadMaxMW = 0; // Se inicializa para capturar el valor de una sola vez
 
    // 4. Procesar registros y acumular totales
    for (const row of registros) {
      // Obtiene la energía real en MWh durante el intervalo de 15 min.
      let valor = parseFloat(row.Values_KWH) || 0;
      // Fórmula: Energía = Potencia x Tiempo.  
      // Si la misma planta de 100 MW funciona a su máxima capacidad durante 10 horas, 
      // habrá producido 100 MW x 10 horas = 1,000 MWh de energía.
      
      // Obtiene la capacidad máxima de potencia de la planta en MW.
      let capacidadMaximaMW = parseFloat(row.Maximo_Cogeneracion_MW) || 0;

      if (row.Expr1.includes('Entrega')) {
        totalEnergiaEntregada += valor;
        let potenciaPromedioMW = valor / 0.25;
        totalPotenciaOperacion += potenciaPromedioMW;
        totalCapacidadMaxMW += capacidadMaximaMW;
        count++;


        // Calcula la potencia promedio real en MW para ese intervalo.
        // Se usa 0.25 porque 15 minutos es un cuarto de una hora.
        //let potenciaPromedioMW =  valor / 0.25;
        // Calcula el porcentaje de operación comparando potencia con potencia.
        //let porcOperacion =  (potenciaPromedioMW / capacidadMaximaMW) * 100;

        // energiaEntregada[row.Expr1] += valor;
        // energiaEntregada['capacidadGeneracion_MW'] += capacidadMaximaMW;
        // energiaEntregada['potenciaPromedio_MW'] += potenciaPromedioMW;
        // energiaEntregada['porcentajeOperacion'] += porcOperacion;
      } else if (row.Expr1.includes('Recibida')) {
        totalEnergiaRecibida += valor;
      }
    }

    // 5. Calcular los valores finales para el panel fuera del bucle
    const potenciaPromedioFinal = totalPotenciaOperacion;
    const porcentajeOperacionFinal = (potenciaPromedioFinal / totalCapacidadMaxMW) * 100;

    energiaEntregada['energiaEntregada_MWH'] = totalEnergiaEntregada;
    energiaEntregada['capacidadGeneracion_MW'] = totalCapacidadMaxMW;
    energiaEntregada['potenciaOperacion_MW'] = potenciaPromedioFinal;
    energiaEntregada['porcentajeOperacion'] = porcentajeOperacionFinal;
    energiaRecibida['energiaRecibida_MWH'] = totalEnergiaRecibida;

    res.json({
      desde: new Date(fromTimestamp).toISOString(),
      hasta: new Date(toTimestamp).toISOString(),
      energiaEntregada,
      energiaRecibida
    });

  } catch (error) {
    console.error('Error en /api/plantas/suma-15min:', error);
    res.status(500).json({ error: 'Error interno' });
  }
});

app.get('/api/plantas/suma-15min2', async (req, res) => {
  try {
    const { fromParam, toParam } = req.dates; 
    const groupBy = req.query.groupBy; 

    let fromTimestamp, toTimestamp;

    // 1. Determinar el rango de tiempo
    if (!fromParam && !toParam) {
      const ultimoRegistroRaw = await redisClient.zRange('View_Datalog_Gen', -1, -1);
      if (!ultimoRegistroRaw || ultimoRegistroRaw.length === 0) {
        return res.json({
          sumas: {
            'kWh Entregada': 0,
            'kWh Recibida': 0
          },
          aviso: 'No hay registros en Redis'
        });
      }

      const ultimoRegistro = JSON.parse(ultimoRegistroRaw[0]);
      const horaUltimoMs = new Date(ultimoRegistro.TimestampUTC).getTime();
      toTimestamp = horaUltimoMs;
      fromTimestamp = toTimestamp;
      console.log('🕒 Última fecha en redis:', toTimestamp, new Date(toTimestamp).toISOString());
    }

    // 2. Obtener registros del ZSET dentro del rango
    const registrosRaw = await redisClient.zRangeByScore(
      'View_Datalog_Gen',
      fromTimestamp,
      toTimestamp
    );

    const registros = registrosRaw.map(r => {
      try { return JSON.parse(r); } catch { return null; }
    }).filter(r => r);

    console.log(`Registros filtrados: ${registros.length}`);

    // 3. Definir estructura para agrupar
    const agrupados = new Map();

    // 4. Procesar registros
    for (const row of registros) {
      const key = groupBy ? row[groupBy] : 'total'; // Si no hay groupBy, acumula en "total"

      if (!agrupados.has(key)) {
        agrupados.set(key, {
          'kWh Entregada': 0,
          'kWh Recibida': 0,
          'capacidadGeneracion_MWH': 0,
          'porcentajeOperacion': 0,
          count: 0 // para calcular promedio del porcentaje
        });
      }

      const grupo = agrupados.get(key);

      let valor = parseFloat(row.Values_KWH) || 0;
      let maxGeneracion = parseFloat(row.Maximo_Cogeneracion_MW) || 0;
      maxGeneracion = maxGeneracion * 0.25; // convertir MW → MWh en 15 minutos

      let potenciaMW = valor / 0.25;
      let porcOperacion = (maxGeneracion > 0) ? (potenciaMW / maxGeneracion) * 100 : 0;

      if (row.Expr1.includes('Entrega')) {
        grupo['kWh Entregada'] += valor;
        grupo['capacidadGeneracion_MWH'] += maxGeneracion;
        grupo['porcentajeOperacion'] += porcOperacion;
        grupo.count++;
      } else if (row.Expr1.includes('Recibida')) {
        grupo['kWh Recibida'] += valor;
      }
    }

    // 5. Preparar salida
    const resultado = [];
    for (const [key, valores] of agrupados.entries()) {
      resultado.push({
        group: key,
        'kWh Entregada': valores['kWh Entregada'],
        'kWh Recibida': valores['kWh Recibida'],
        'capacidadGeneracion_MWH': valores['capacidadGeneracion_MWH'],
        'porcentajeOperacion': valores.count > 0 ? valores['porcentajeOperacion'] / valores.count : 0
      });
    }

    res.json({
      desde: new Date(fromTimestamp).toISOString(),
      hasta: new Date(toTimestamp).toISOString(),
      resultados: resultado
    });

  } catch (error) {
    console.error('Error en /api/plantas/suma-15min:', error);
    res.status(500).json({ error: 'Error interno' });
  }
});

app.get('/api/plantas/suma-15min3', async (req, res) => {
  try {
    let { fromParam, toParam } = req.dates;
    const groupBy = req.query.groupBy;
    
    // 1. Determinar el rango de tiempo
    if (!fromParam && !toParam) {
      const ultimoRegistroRaw = await redisClient.zRange('View_Datalog_Gen', -1, -1);
      if (!ultimoRegistroRaw || ultimoRegistroRaw.length === 0) {
        return res.json({
          resultados: [],
          aviso: 'No hay registros en Redis'
        });
      }
      const ultimoRegistro = JSON.parse(ultimoRegistroRaw[0]);
      const horaUltimoMs = new Date(ultimoRegistro.TimestampUTC).getTime();
      toParam = horaUltimoMs;
      fromParam = toParam;
    }
    const registrosRaw = await redisClient.zRangeByScore('View_Datalog_Gen', fromParam, toParam);
    const registros = registrosRaw.map(r => {
      try { return JSON.parse(r); } catch { return null; }
    }).filter(r => r);
    
    console.log(`Registros filtrados: ${registros.length}`);

    // 2. Definir estructura para agrupar y acumular
    const agrupados = new Map();
    
    // 3. Procesar registros y acumular valores
    for (const row of registros) {
      const key = groupBy ? row[groupBy] : 'totalPlantas';
      
      if (!agrupados.has(key)) {
        agrupados.set(key, {
          totalEnergiaEntregada: 0,
          totalEnergiaRecibida: 0,
          totalPotenciaMW: 0,
          potenciaPromedioMW:0,
          totalCapacidadMW: 0, 
          count: 0
        });
      }
      
      const grupo = agrupados.get(key);
      const valor = parseFloat(row.Values_KWH) || 0;
      const capacidadMaximaMW = parseFloat(row.Maximo_Cogeneracion_MW) || 0;
      
      if (row.Expr1.includes('Entrega')) {
        grupo.totalEnergiaEntregada += valor;
        const potenciaMW = valor / 0.25;
        grupo.totalPotenciaMW += potenciaMW;
        grupo.totalCapacidadMW += capacidadMaximaMW;
        grupo.count++;
      } else if (row.Expr1.includes('Recibida')) {
        grupo.totalEnergiaRecibida += valor;
      }
    }
    
    // 4. Calcular valores finales y preparar salida
    const resultado = [];
    for (const [key, valores] of agrupados.entries()) {
      //const potenciaPromedio = valores.count > 0 ? valores.totalPotenciaMW / valores.count : 0;
      //const capacidadMaximaPromedio = valores.count > 0 ? valores.totalCapacidadMW / valores.count : 0; // Promedia la capacidad
      const potenciaPromedio = valores.totalPotenciaMW;
      const capacidadMaximaPromedio = valores.totalCapacidadMW;
      const capacidadPromedioMWH = capacidadMaximaPromedio * 0.25; // Convertir a MWh
      const porcOperacion = (capacidadMaximaPromedio > 0) ? (potenciaPromedio / capacidadMaximaPromedio) * 100 : 0;

      resultado.push({
        group: key,
        'energiaEntregada_MWH': valores.totalEnergiaEntregada,
        'energiaRecibida_MWH': valores.totalEnergiaRecibida,
        'capacidadGeneracion_MW': capacidadMaximaPromedio,
        'capacidadGeneracion_MWH': capacidadPromedioMWH,
        'potenciaOperacion_MW': potenciaPromedio,
        'porcentajeOperacion': porcOperacion
      });
    }
    
    res.json({
      desde: new Date(fromParam).toISOString(),
      hasta: new Date(toParam).toISOString(),
      resultados: resultado
    });
    
  } catch (error) {
    console.error('Error en /api/plantas/suma-15min:', error);
    res.status(500).json({ error: 'Error interno' });
  }
});

// Nuevo endpoint que suma datos y los agrupa por parámetros de consulta
app.get('/api/scada-service/suma-agrupada', async (req, res) => {
  try {
    // Extraer los parámetros de la solicitud, incluyendo el nuevo 'groupBy'
    const { fromParam, toParam } = req.dates;
    const { groupBy, orderBy } = req.query;

    let fromTimestamp, toTimestamp;

    // 1. Determinar el rango de tiempo
    if (fromParam && toParam && Number.isFinite(parseInt(fromParam)) && Number.isFinite(parseInt(toParam))) {
      fromTimestamp = parseInt(fromParam);
      toTimestamp = parseInt(toParam);
    } else {
      const ultimoRegistroRaw = await redisClient.zRange('View_Datalog_Gen', -1, -1);
      if (!ultimoRegistroRaw || ultimoRegistroRaw.length === 0) {
        return res.json({
          sumas: { 
            'kWh Entregada': 0, 
            'kWh Recibida': 0, 
            'capacidadGeneracion_MWH': 0
          },
          aviso: 'No hay registros en Redis'
        });
      }

      const ultimoRegistro = JSON.parse(ultimoRegistroRaw[0]);
      const horaUltimoMs = new Date(ultimoRegistro.TimestampUTC).getTime();
      toTimestamp = horaUltimoMs;
      fromTimestamp = toTimestamp;
      console.log('🕒 Última fecha en redis:', toTimestamp, new Date(toTimestamp).toISOString());
    }

    // 2. Obtener registros de Redis dentro del rango de tiempo
    const registrosRaw = await redisClient.zRangeByScore(
      'View_Datalog_Gen',
      fromTimestamp,
      toTimestamp
    );

    const registros = registrosRaw.map(r => {
      try { return JSON.parse(r); } catch { return null; }
    }).filter(r => r);

    // 3. Inicializar el objeto para almacenar los datos agrupados
    const grupos = new Map();
    let defaultGroupKey = 'Total';

    // 4. Procesar y agrupar los registros
    for (const row of registros) {
      // Determinar la clave de agrupación dinámicamente
      let groupKey = defaultGroupKey;
      if (groupBy) {
        if (groupBy.toLowerCase() === 'name' && row.Name) {
          groupKey = row.Name;
        } else if (groupBy.toLowerCase() === 'tipo_cogeneracion' && row.Tipo_Cogeneracion) {
          groupKey = row.Tipo_Cogeneracion;
        }
      }

      // Inicializar el grupo si no existe
      if (!grupos.has(groupKey)) {
        grupos.set(groupKey, {
          'kWh Entregada': 0,
          'kWh Recibida': 0,
          'capacidadGeneracion_MWH': 0,
        });
      }

      const grupo = grupos.get(groupKey);
      let valor = parseFloat(row.Values_KWH) || 0;
      // Fórmula: Energía = Potencia x Tiempo.  
      // Si la misma planta de 100 MW funciona a su máxima capacidad durante 10 horas, 
      // habrá producido 100 MW x 10 horas = 1,000 MWh de energía.
      let maxGeneracion = parseFloat(row.Maximo_Cogeneracion_MW) || 0;
      // 0.25 hrs = 15 min porque así son los intervalos en que se reciben los datos
      maxGeneracion = maxGeneracion * 0.25;

      // Sumar los valores al grupo correspondiente
      if (row.Expr1.includes('Entrega')) {
        grupo['kWh Entregada'] += valor;
        grupo['capacidadGeneracion_MWH'] += maxGeneracion;
      } else if (row.Expr1.includes('Recibida')) {
        grupo['kWh Recibida'] += valor;
        grupo['capacidadGeneracion_MWH'] += maxGeneracion;
      }
    }
    
    // 5. Convertir el mapa de grupos a un objeto para la respuesta JSON
    let gruposObj = Object.fromEntries(grupos);
    
    // 6. Ordenar los resultados si se especifica el parámetro 'orderBy'
    if (orderBy) {
        let sortedEntries = Object.entries(gruposObj);
        
        if (orderBy.toLowerCase() === 'entregada') {
            sortedEntries.sort(([, a], [, b]) => b['kWh Entregada'] - a['kWh Entregada']);
        } else if (orderBy.toLowerCase() === 'recibida') {
            sortedEntries.sort(([, a], [, b]) => b['kWh Recibida'] - a['kWh Recibida']);
        } else {
             // Por defecto, se ordena por la clave del grupo
             sortedEntries.sort(([a], [b]) => a.localeCompare(b));
        }

        gruposObj = Object.fromEntries(sortedEntries);
    }
    
    // 7. Adaptar la respuesta para el Bar Chart de Grafana si se usa 'groupBy'
    if (groupBy) {
        const barChartData = [];
        for (const [key, values] of Object.entries(gruposObj)) {
            barChartData.push({
                [groupBy]: key,
                'kWh Entregada': values['kWh Entregada'],
                'kWh Recibida': values['kWh Recibida'],
                'capacidadGeneracion_MWH': values['capacidadGeneracion_MWH']
            });
        }
        return res.json(barChartData);
    }

    // 8. Enviar la respuesta original si no hay agrupación
    res.json({
      desde: new Date(fromTimestamp).toISOString(),
      hasta: new Date(toTimestamp).toISOString(),
      grupos: gruposObj
    });

  } catch (error) {
    console.error('Error en /api/plantas/suma-agrupada:', error);
    res.status(500).json({ error: 'Error interno' });
  }
});

app.get('/api/plantas/serie-times2', async (req, res) => {
  try {
    const QUINCE_MINUTOS_MS = 15 * 60 * 1000;
    const { fromParam, toParam } = req.dates || req.query; // Usa req.dates si está disponible, si no, usa req.query
    const groupBy = req.query.groupBy;

    let fromTimestamp, toTimestamp;

    if (fromParam && toParam && Number.isFinite(parseInt(fromParam)) && Number.isFinite(parseInt(toParam))) {
      fromTimestamp = parseInt(fromParam);
      toTimestamp = parseInt(toParam);
    } else {
      const ultimoRegistroRaw = await redisClient.zRange('View_Datalog_Gen', -1, -1);
      if (!ultimoRegistroRaw || ultimoRegistroRaw.length === 0) return res.json([]);
      const ultimoRegistro = JSON.parse(ultimoRegistroRaw[0]);
      toTimestamp = new Date(ultimoRegistro.TimestampUTC).getTime();
      fromTimestamp = toTimestamp - QUINCE_MINUTOS_MS;
    }

    const registrosRaw = await redisClient.zRangeByScore('View_Datalog_Gen', fromTimestamp, toTimestamp);
    
    // --- Agrupación
    const groupedData = new Map();

    for (const r of registrosRaw) {
      const registro = JSON.parse(r);
      const timestampMs = new Date(registro.TimestampUTC).getTime();
      const bucketTimestamp = Math.floor(timestampMs / QUINCE_MINUTOS_MS) * QUINCE_MINUTOS_MS;

      const groupKey = groupBy && registro[groupBy] ? registro[groupBy] : "ALL";
      const compositeKey = `${bucketTimestamp}_${groupKey}`;

      if (!groupedData.has(compositeKey)) {
        groupedData.set(compositeKey, {
          group: groupKey,
          timestamp: bucketTimestamp,
          totalEnergiaEntregada_KWH: 0,
          totalEnergiaRecibida_KWH: 0,
          totalPotenciaMW: 0,
          totalCapacidadMW: 0,
          count: 0
        });
      }
      
      const bucket = groupedData.get(compositeKey);
      let valorKWH = parseFloat(registro.Values_KWH) || 0;
      let capacidadMW = parseFloat(registro.Maximo_Cogeneracion_MW) || 0;

      if (registro.Expr1.includes('Entrega')) {
        bucket.totalEnergiaEntregada_KWH += valorKWH;
        const potenciaMW = valorKWH / 0.25; // 0.25 horas = 15 minutos
        bucket.totalPotenciaMW += potenciaMW;
        bucket.totalCapacidadMW += capacidadMW;
        bucket.count++;
      } else if (registro.Expr1.includes('Recibida')) {
        bucket.totalEnergiaRecibida_KWH += valorKWH;
      }
    }

    const resultados = [];
    for (const [, bucket] of groupedData.entries()) {
      //const potenciaPromedioMW = bucket.count > 0 ? bucket.totalPotenciaMW / bucket.count : 0;
      //const capacidadPromedioMW = bucket.count > 0 ? bucket.totalCapacidadMW / bucket.count : 0;
      const potenciaPromedioMW = bucket.totalPotenciaMW;
      const capacidadPromedioMW = bucket.totalCapacidadMW;
      const capacidadPromedioMWH = capacidadPromedioMW * 0.25; // Convertir a MWh
      const porcOperacion = (capacidadPromedioMW > 0) ? (potenciaPromedioMW / capacidadPromedioMW) * 100 : 0;

      resultados.push({
        timestamp: new Date(bucket.timestamp).toISOString().replace('Z', '-06:00'),
        group: bucket.group,
        'energiaEntregada_MWH': bucket.totalEnergiaEntregada_KWH,
        'energiaRecibida_MWH': bucket.totalEnergiaRecibida_KWH,
        'capacidadPromedio_MW': capacidadPromedioMW,
        'capacidadPromedio_MWH': capacidadPromedioMWH,
        'PotenciaPromedio_MW': potenciaPromedioMW,
        'PorcentajeOperacion': porcOperacion
      });
    }

    resultados.sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());

    res.json(resultados);

  } catch (error) {
    console.error('Error en /api/plantas/serie-times:', error);
    res.status(500).json({ error: 'Error interno' });
  }
});

app.get('/api/plantas/serie-times', async (req, res) => {
  try {
    // --- Parámetros
    const { fromParam, toParam } = req.dates; 
    const groupBy = req.query.groupBy;
    // si usas middleware req.dates, tomará de ahí; si no, cae al query normal

    console.log(`Solicitud de rango:
        Desde: ${new Date(fromParam).toISOString()}
        Hasta: ${new Date(toParam).toISOString()}
        groupBy: ${groupBy}`);

    let fromTimestamp, toTimestamp;

    if (fromParam && toParam && Number.isFinite(parseInt(fromParam)) && Number.isFinite(parseInt(toParam))) {
      fromTimestamp = Math.floor((parseInt(fromParam)) / QUINCE_MINUTOS_MS) * QUINCE_MINUTOS_MS;
      toTimestamp = Math.floor((parseInt(toParam)) / QUINCE_MINUTOS_MS) * QUINCE_MINUTOS_MS;
    } else {
      const ultimoRegistroRaw = await redisClient.zRange('View_Datalog_Gen', -1, -1);
      if (!ultimoRegistroRaw || ultimoRegistroRaw.length === 0) return res.json([]);
      const ultimoRegistro = JSON.parse(ultimoRegistroRaw[0]);
      toTimestamp = new Date(ultimoRegistro.TimestampUTC).getTime();
      fromTimestamp = toTimestamp - QUINCE_MINUTOS_MS;
    }

    const registrosRaw = await redisClient.zRangeByScore('View_Datalog_Gen', fromTimestamp, toTimestamp);

    // --- Agrupación
    const groupedData = new Map();

    for (const r of registrosRaw) {
      const registro = JSON.parse(r);
      const timestampMs = new Date(registro.TimestampUTC).getTime();

      const bucketTimestamp = Math.floor(timestampMs / QUINCE_MINUTOS_MS) * QUINCE_MINUTOS_MS;

      // --- clave dinámica: timestamp + valor de la columna por la que agrupamos
      const groupKey = groupBy && registro[groupBy] ? registro[groupBy] : "ALL";
      const compositeKey = `${bucketTimestamp}_${groupKey}`;

      if (!groupedData.has(compositeKey)) {
        groupedData.set(compositeKey, {
          group: groupKey,
          timestamp: bucketTimestamp,
          'kWh Entregada': 0,
          'kWh Recibida': 0,
          'capacidadGeneracion_MWH': 0
        });
      }

      const sumas = groupedData.get(compositeKey);
      let valor = parseFloat(registro.Values_KWH) || 0;
      let maxGeneracion = parseFloat(registro.Maximo_Cogeneracion_MW) || 0;
      maxGeneracion = maxGeneracion * 0.25; // 15 min

      sumas[registro.Expr1] += valor;
      sumas['capacidadGeneracion_MWH'] += maxGeneracion;
    }

    const resultados = [];
    for (const [, sumas] of groupedData.entries()) {
      resultados.push({
        timestamp: new Date(sumas.timestamp).toISOString().replace('Z', '-06:00'),
        group: sumas.group,
        sumas: {
          'kWh Entregada': sumas['kWh Entregada'],
          'kWh Recibida': sumas['kWh Recibida'],
          'capacidadGeneracion_MWH': sumas['capacidadGeneracion_MWH']
        }
      });
    }

    resultados.sort((a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime());

    res.json(resultados);

  } catch (error) {
    console.error('Error en /api/plantas/serie-times:', error);
    res.status(500).json({ error: 'Error interno' });
  }
});


// Endpoint para totalizar datos
app.get('/api/scada-service/totalizar_data', async (req, res) => {
  try {
    const { from, to, Tipo_Cogeneracion, Name } = req.query;

    // Validar que los parámetros from y to existen
    if (!from || !to) {
      return res.status(400).json({ error: 'Los parámetros "from" y "to" son obligatorios.' });
    }

    // Convertir los timestamps en milisegundos a números
    const fromMs = parseInt(from, 10);
    const toMs = parseInt(to, 10);

    // Restar el ajuste de la zona horaria (UTC-6) a los milisegundos
    // El ajuste de UTC-6 ya está en milisegundos
    const startTimestamp = fromMs - UTC_MINUS_6_MS;
    const endTimestamp = toMs - UTC_MINUS_6_MS;

    console.log(`Solicitud de rango:
        Desde: ${new Date(startTimestamp).toISOString()}
        Hasta: ${new Date(endTimestamp).toISOString()}`);

    // Obtener los datos del set ordenado de Redis usando el rango de timestamps
    const data = await redisClient.zRangeByScore('View_Datalog_Gen', startTimestamp, endTimestamp);

    if (data.length === 0) {
      return res.status(200).json({ message: 'No se encontraron datos para el rango especificado.', data: [] });
    }

    // Estructura para almacenar los datos totalizados
    const totalsMap = new Map();

    // Procesar y totalizar los datos
    for (const item of data) {
      const record = JSON.parse(item);
      const recordTimestamp = new Date(record.TimestampUTC).getTime();

      // Redondear el timestamp al intervalo de 15 minutos más cercano
      // Se usa el valor original de la base de datos para la agrupación
      const quarterHourMs = 15 * 60 * 1000;
      const quarterHourKey = Math.floor(recordTimestamp / quarterHourMs) * quarterHourMs;

      // Determinar la clave de agrupación
      let groupKey;
      if (Tipo_Cogeneracion && Name) {
        groupKey = `${quarterHourKey}-${record.Tipo_Cogeneracion}-${record.Name}`;
      } else if (Tipo_Cogeneracion) {
        groupKey = `${quarterHourKey}-${record.Tipo_Cogeneracion}`;
      } else if (Name) {
        groupKey = `${quarterHourKey}-${record.Name}`;
      } else {
        // Caso por defecto: agrupar solo por intervalo de 15 minutos
        groupKey = `${quarterHourKey}`;
      }

      // Obtener el valor de KWH
      const kwhValue = parseFloat(record.Values_KWH) || 0;

      // Inicializar o actualizar el total para la clave de agrupación
      if (!totalsMap.has(groupKey)) {
        totalsMap.set(groupKey, {
          timestamp: new Date(quarterHourKey).toISOString(),
          total_kwh: 0,
          Tipo_Cogeneracion: record.Tipo_Cogeneracion,
          Name: record.Name
        });
      }

      const currentTotal = totalsMap.get(groupKey);
      currentTotal.total_kwh += kwhValue;
    }

    // Convertir el mapa de totales a un array de resultados
    const result = Array.from(totalsMap.values()).sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

    res.status(200).json(result);

  } catch (err) {
    console.error('Error al procesar la solicitud:', err);
    res.status(500).json({ error: 'Error interno del servidor.' });
  }
});

// --- Inicio del servidor después de conectar Redis ---
(async () => {
  try {
    await redisClient.connect();
    console.log('Conectado a Redis');

    app.listen(9002, () => {
      console.log('Servidor escuchando en puerto 9002');
      syncDataToRedis(); // Primera sincronización al iniciar
    });
  } catch (err) {
    console.error('No se pudo conectar a Redis:', err);
    process.exit(1);
  }
})();