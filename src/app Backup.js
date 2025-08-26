require('dotenv').config();
const express = require('express');
const sql = require('mssql');
const redis = require('redis');
const cron = require('node-cron');
const utc6Middleware = require('./middlewares/utc6Middleware');

const app = express();

// --- Middleware global para convertir fechas a UTC-6 ---
app.use(utc6Middleware);

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
    const fechaUTC6 = new Date().toLocaleString('es-CR', { timeZone: 'America/Costa_Rica' });
    console.log(`[${fechaUTC6}] Iniciando sincronización...`);

    const pool = await getPool();

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

    // 2. Convertir a zona horaria -6
    let zaddItems = UTC6(datosProcesados);

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

    // 5. Guardar última fecha procesada con -6h
    if (maxFechaInsertadaGen > 0) {
      await redisClient.set('View_Datalog_Gen_LAST_DATE', maxFechaInsertadaGen);
      console.log('🕒 Última fecha procesada guardada:', maxFechaInsertadaGen, new Date(maxFechaInsertadaGen).toISOString().replace('Z', '-06:00'));
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

    return row;
  });
}

function UTC6(result) {
  return result.map(row => {
    // Timestamp de la BD (UTC-6)
    const fecha = new Date(row.TimestampUTC);
    
    // Reconstruimos string para que Redis muestre correctamente la hora en UTC-6
    const fechaUTCMinus6 = new Date(fecha.getTime() + 6 * 60 * 60 * 1000);
    const yyyy = fechaUTCMinus6.getFullYear();
    const mm = String(fechaUTCMinus6.getMonth() + 1).padStart(2, '0');
    const dd = String(fechaUTCMinus6.getDate()).padStart(2, '0');
    const hh = String(fechaUTCMinus6.getHours()).padStart(2, '0');
    const min = String(fechaUTCMinus6.getMinutes()).padStart(2, '0');
    const ss = String(fechaUTCMinus6.getSeconds()).padStart(2, '0');
    const ms = String(fechaUTCMinus6.getMilliseconds()).padStart(3, '0');

    row.TimestampUTC = `${yyyy}-${mm}-${dd}T${hh}:${min}:${ss}.${ms}-06:00`;

    // Ajustamos score para que coincida con la hora local en UTC-6
    const score = new Date(row.TimestampUTC).getTime() + 6 * 60 * 60 * 1000;

    return { score, value: JSON.stringify(row) };
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

  const timezoneOffsetMs = 6 * 60 * 60 * 1000; // UTC-6 para Costa Rica

  if (req.query.from && req.query.to) {
    // Ajustar el timestamp enviado por Grafana (UTC) a hora local
    desde = parseInt(req.query.from) - timezoneOffsetMs;
    hasta = parseInt(req.query.to) - timezoneOffsetMs;
  } else {
    const minutos = parseInt(req.query.minutos || '60');
    desde = ahora - minutos * 60 * 1000;
    hasta = ahora;
  }
  try {
    //const raw = await redisClient.zrangebyscore(vista, desde, hasta);
    const raw = await redisClient.zRangeByScore(vista, desde, hasta);
    const parsed = raw.map(JSON.parse);
    res.json(parsed);
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

      fromTimestamp = horaUltimoMs - UTC_MINUS_6_MS;
      console.log('🕒 Última fecha en redis:', fromTimestamp, new Date(fromTimestamp).toISOString().replace('Z', '-06:00'));
      toTimestamp = fromTimestamp - (QUINCE_MINUTOS_MS);
      console.log('🕒 Última fecha en redis:', toTimestamp, new Date(toTimestamp).toISOString().replace('Z', '-06:00'));
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
    const sumas = {
      'kWh Entregada': 0,
      'kWh Recibida': 0
    };

    // 4. Procesar registros
    for (const row of registros) {
      let valor = parseFloat(row.Values_KWH) || 0;
      if (sumas[row.Expr1] !== undefined) {
        sumas[row.Expr1] += valor;
      }
    }

    res.json({
      desde: formatUTCMinus6(fromTimestamp),
      hasta: formatUTCMinus6(toTimestamp),
      sumas
    });

  } catch (error) {
    console.error('Error en /api/plantas/suma-15min:', error);
    res.status(500).json({ error: 'Error interno' });
  }
});

app.get('/api/plantas/serie-times', async (req, res) => {
  try {
    const fromParam = req.query.from;
    const toParam = req.query.to;

    let fromTimestamp, toTimestamp;

    if (fromParam && toParam && Number.isFinite(parseInt(fromParam)) && Number.isFinite(parseInt(toParam))) {
      // Convertimos los timestamps UTC del front a UTC-6 para buscar en Redis
      fromTimestamp = Math.floor((parseInt(fromParam) - UTC_MINUS_6_MS) / QUINCE_MINUTOS_MS) * QUINCE_MINUTOS_MS;
      toTimestamp = Math.floor((parseInt(toParam) - UTC_MINUS_6_MS) / QUINCE_MINUTOS_MS) * QUINCE_MINUTOS_MS;
    } else {
      const ultimoRegistroRaw = await redisClient.zRange('View_Datalog_Gen', -1, -1);
      if (!ultimoRegistroRaw || ultimoRegistroRaw.length === 0) return res.json([]);
      const ultimoRegistro = JSON.parse(ultimoRegistroRaw[0]);
      toTimestamp = new Date(ultimoRegistro.TimestampUTC).getTime();
      fromTimestamp = toTimestamp - QUINCE_MINUTOS_MS;
    }

    const registrosRaw = await redisClient.zRangeByScore('View_Datalog_Gen', fromTimestamp, toTimestamp);

    const groupedData = new Map();
    for (const r of registrosRaw) {
      const registro = JSON.parse(r);
      const timestampMs = new Date(registro.TimestampUTC).getTime();

      // Se calcula el bucket de 15 minutos al que pertenece el registro
      const bucketTimestamp = Math.floor(timestampMs / QUINCE_MINUTOS_MS) * QUINCE_MINUTOS_MS;

      if (!groupedData.has(bucketTimestamp)) {
        groupedData.set(bucketTimestamp, {
          'Potencia Activa': 0,
          'Potencia Reactiva': 0,
          'kWh Entregada': 0,
          'kWh Recibida': 0
        });
      }

      const sumas = groupedData.get(bucketTimestamp);
      if (['Potencia Activa', 'Potencia Reactiva', 'kWh Entregada', 'kWh Recibida'].includes(registro.Expr1)) {
        let valor = parseFloat(registro.Values_KWH) || 0;
        if (registro.EstaGenerando === "FALSE") valor = 0;
        else if (valor < 0) valor = Math.abs(valor);

        if (registro.Expr1 === 'Potencia Activa' || registro.Expr1 === 'Potencia Reactiva') valor = valor / 1000;
        if (registro.Expr1 === 'kWh Entregada' || registro.Expr1 === 'kWh Recibida') valor = valor / 1000;

        sumas[registro.Expr1] += valor;
      }
    }

    const resultados = [];
    for (const [timestamp, sumas] of groupedData.entries()) {
      resultados.push({
        // Devolvemos la hora tal como está en Redis (UTC-6)
        desde: new Date(timestamp).toISOString().replace('Z', '-06:00'),
        hasta: new Date(timestamp + QUINCE_MINUTOS_MS).toISOString().replace('Z', '-06:00'),
        sumas
      });
    }

    resultados.sort((a, b) => new Date(a.desde).getTime() - new Date(b.desde).getTime());
    res.json(resultados);

  } catch (error) {
    console.error('Error en /api/plantas/suma-15min:', error);
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