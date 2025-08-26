// middlewares/utc6Middleware.js

function utc6Middleware(req, res, next) {
  const { from, to } = req.query;

  function convertirUtc6(valor) {
    if (!valor) return null;
    let fecha;

    if (!isNaN(valor)) {
      fecha = new Date(parseInt(valor, 10));
    } else {
      fecha = new Date(valor);
    }

    if (isNaN(fecha.getTime())) return null;

    return fecha.getTime() - (6 * 60 * 60 * 1000);
  }

  // Crea un nuevo objeto para almacenar los datos convertidos
  req.dates = {
    fromParam: null,
    toParam: null
  };

  if (from) {
    req.dates.fromParam = convertirUtc6(from);
  }
  if (to) {
    req.dates.toParam = convertirUtc6(to);
  }

  console.log('Middleware - req.dates:', req.dates); // Verifica aquí

  next();
}

module.exports = utc6Middleware;