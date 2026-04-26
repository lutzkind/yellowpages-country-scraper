const fs = require("fs");
const path = require("path");
const { fork } = require("child_process");
const { createStore } = require("./src/store");
const { createApp } = require("./src/server");
const { createNocoDbService } = require("./src/nocodb");
const config = require("./src/config");

fs.mkdirSync(config.dataDir, { recursive: true });
fs.mkdirSync(config.exportsDir, { recursive: true });

const store = createStore(config);
const nocoDb = createNocoDbService({ store, config });
const app = createApp({ store, config, nocoDb });
let workerChild = null;
let workerRestartTimer = null;
let shuttingDown = false;

const server = app.listen(config.port, config.host, () => {
  console.log(
    `yellowpages-country-scraper listening on http://${config.host}:${config.port}`
  );
});

if (config.runScraperWorker) {
  startWorkerChild();
}

async function shutdown() {
  shuttingDown = true;
  if (workerRestartTimer) clearTimeout(workerRestartTimer);
  if (workerChild && !workerChild.killed) {
    workerChild.kill("SIGTERM");
  }
  server.close();
}
process.once("SIGTERM", shutdown);
process.once("SIGINT", shutdown);

function startWorkerChild() {
  workerChild = fork(path.join(__dirname, "worker-entry.js"), {
    stdio: "inherit",
    env: {
      ...process.env,
      RUN_SCRAPER_WORKER: "false",
    },
  });

  workerChild.on("exit", (code, signal) => {
    if (shuttingDown) return;
    console.error(
      `yellowpages-country-scraper worker exited (code=${code ?? "null"}, signal=${signal ?? "null"}); restarting in 5s`
    );
    workerRestartTimer = setTimeout(() => {
      workerRestartTimer = null;
      startWorkerChild();
    }, 5000);
    workerRestartTimer.unref?.();
  });
}
