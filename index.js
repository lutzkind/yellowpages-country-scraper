const fs = require("fs");
const { createStore } = require("./src/store");
const { createWorker } = require("./src/worker");
const { createApp } = require("./src/server");
const { createNocoDbService } = require("./src/nocodb");
const config = require("./src/config");

fs.mkdirSync(config.dataDir, { recursive: true });
fs.mkdirSync(config.exportsDir, { recursive: true });

const store = createStore(config);
const nocoDb = createNocoDbService({ store, config });
const worker = createWorker({ store, config, nocoDb });
const app = createApp({ store, config, nocoDb });

const server = app.listen(config.port, config.host, () => {
  worker
    .start()
    .then(() => {
      console.log(
        `yellowpages-country-scraper listening on http://${config.host}:${config.port}`
      );
    })
    .catch((error) => {
      console.error("Failed to start worker:", error);
      server.close(() => {
        process.exitCode = 1;
      });
    });
});
