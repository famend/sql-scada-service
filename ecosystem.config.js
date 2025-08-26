module.exports = {
  apps: [
    {
      name: "sql-scada-service",
      script: "./src/app.js",
      cwd: "./",
      env: {
        NODE_ENV: "development",
        SQL_USER: "grafana",
        SQL_PASSWORD: "Monitoreo2026**",
        SQL_SERVER: "172.16.1.206",
        SQL_DATABASE: "GeneracionBD",
        SQL_PORT: "1433",
        REDIS_HOST: "localhost",
        REDIS_PORT: "6379"
      }
    }
  ]
};