#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const ScreenLogic = require('node-screenlogic');

const DEFAULT_SYSTEM_NAME = 'Pentair: 22-8E-AD';
const DEFAULT_PASSWORD = '';
const DEFAULT_DAYS = 365;
const DEFAULT_CHUNK_DAYS = 14;

function parseArgs(argv) {
  const args = {
    systemName: process.env.SCREENLOGIC_SYSTEM_NAME || DEFAULT_SYSTEM_NAME,
    password: process.env.SCREENLOGIC_PASSWORD || DEFAULT_PASSWORD,
    days: Number(process.env.SCREENLOGIC_HISTORY_DAYS || DEFAULT_DAYS),
    chunkDays: Number(process.env.SCREENLOGIC_HISTORY_CHUNK_DAYS || DEFAULT_CHUNK_DAYS),
    outDir: path.resolve(process.cwd(), 'tmp'),
  };

  for (let i = 2; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--system-name') args.systemName = argv[++i];
    else if (arg === '--password') args.password = argv[++i];
    else if (arg === '--days') args.days = Number(argv[++i]);
    else if (arg === '--chunk-days') args.chunkDays = Number(argv[++i]);
    else if (arg === '--out-dir') args.outDir = path.resolve(argv[++i]);
    else throw new Error(`Unknown argument: ${arg}`);
  }

  return args;
}

function dedupePoints(points) {
  const seen = new Set();
  return (points || [])
    .filter((point) => {
      const key = `${new Date(point.time).toISOString()}|${point.temp}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .sort((a, b) => new Date(a.time) - new Date(b.time));
}

function dedupeRuns(runs) {
  const seen = new Set();
  return (runs || [])
    .filter((run) => {
      const key = `${new Date(run.on).toISOString()}|${new Date(run.off).toISOString()}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .sort((a, b) => new Date(a.on) - new Date(b.on));
}

function sortHistory(history) {
  return {
    airTemps: dedupePoints(history.airTemps),
    poolTemps: dedupePoints(history.poolTemps),
    poolSetPointTemps: dedupePoints(history.poolSetPointTemps),
    spaTemps: dedupePoints(history.spaTemps),
    spaSetPointTemps: dedupePoints(history.spaSetPointTemps),
    poolRuns: dedupeRuns(history.poolRuns),
    spaRuns: dedupeRuns(history.spaRuns),
    solarRuns: dedupeRuns(history.solarRuns),
    heaterRuns: dedupeRuns(history.heaterRuns),
    lightRuns: dedupeRuns(history.lightRuns),
  };
}

async function resolveUnit(systemName) {
  const gateway = new ScreenLogic.RemoteLogin(systemName);
  const unit = await gateway.connectAsync();
  await gateway.closeAsync();
  if (!unit || !unit.gatewayFound || !unit.portOpen || !unit.ipAddr) {
    throw new Error(`Unable to locate ScreenLogic system ${systemName} through Pentair remote access`);
  }
  return unit;
}

async function fetchChunk(client, from, to) {
  return client.equipment.getHistoryDataAsync(from, to);
}

async function fetchHistory(systemName, password, days, chunkDays) {
  const unit = await resolveUnit(systemName);
  const now = new Date();
  const merged = {
    airTemps: [],
    poolTemps: [],
    poolSetPointTemps: [],
    spaTemps: [],
    spaSetPointTemps: [],
    poolRuns: [],
    spaRuns: [],
    solarRuns: [],
    heaterRuns: [],
    lightRuns: [],
  };

  for (let startDaysAgo = 0; startDaysAgo < days; startDaysAgo += chunkDays) {
    const to = new Date(now.getTime() - startDaysAgo * 24 * 3600 * 1000);
    const from = new Date(now.getTime() - Math.min(days, startDaysAgo + chunkDays) * 24 * 3600 * 1000);
    const client = new ScreenLogic.UnitConnection();
    client.init(systemName, unit.ipAddr, unit.port, password || '');
    client.netTimeout = 20000;

    try {
      await client.connectAsync();
      const chunk = await fetchChunk(client, from, to);
      for (const key of Object.keys(merged)) {
        merged[key].push(...(chunk[key] || []));
      }
    } finally {
      await client.closeAsync();
    }
  }

  return sortHistory(merged);
}

async function main() {
  const args = parseArgs(process.argv);
  fs.mkdirSync(args.outDir, { recursive: true });
  const history = await fetchHistory(args.systemName, args.password, args.days, args.chunkDays);
  const rawPath = path.join(args.outDir, 'screenlogic_history_raw.json');
  fs.writeFileSync(rawPath, JSON.stringify(history, null, 2));
  console.log(JSON.stringify({ rawPath, systemName: args.systemName }, null, 2));
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : error);
  process.exit(1);
});

