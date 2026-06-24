import { spawnSync } from "child_process";
import os from "os";

if (!process.env.__RESIZED) {
  const maxMem = Math.floor((os.totalmem() / 1024 / 1024) * 0.75);
  const result = spawnSync(process.execPath, [
    `--max-old-space-size=${maxMem}`,
    "--expose-gc",
    ...process.argv.slice(1)
  ], {
    stdio: "inherit",
    env: { ...process.env, __RESIZED: "1" }
  });
  process.exit(result.status ?? 0);
}

import { makeWASocket, useMultiFileAuthState, generateWAMessageFromContent, prepareWAMessageMedia, downloadMediaMessage } from "baileys";
import pino from "pino";
import readline from "readline";
import fs from "fs";
import axios from "axios";
import path from "path";
import sharp from "sharp";
import { fileURLToPath } from "url";
import { tmpdir } from "os";
import Crypto from "crypto";
import ff from "fluent-ffmpeg";
import webp from "node-webpmux";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const C = {
  reset: "\x1b[0m", bold: "\x1b[1m", dim: "\x1b[2m",
  red: "\x1b[91m", green: "\x1b[92m", yellow: "\x1b[93m",
  blue: "\x1b[94m", magenta: "\x1b[95m", cyan: "\x1b[96m",
  white: "\x1b[97m", bgGreen: "\x1b[42m", bgRed: "\x1b[41m",
  bgBlue: "\x1b[44m", bgMagenta: "\x1b[45m", bgCyan: "\x1b[46m", bgYellow: "\x1b[43m"
};

const log = {
  info:   (tag, msg) => console.log(`${C.cyan}${C.bold}[${tag}]${C.reset} ${C.white}${msg}${C.reset}`),
  ok:     (tag, msg) => console.log(`${C.green}${C.bold}[${tag}]${C.reset} ${C.green}${msg}${C.reset}`),
  warn:   (tag, msg) => console.log(`${C.yellow}${C.bold}[${tag}]${C.reset} ${C.yellow}${msg}${C.reset}`),
  error:  (tag, msg) => console.log(`${C.red}${C.bold}[${tag}]${C.reset} ${C.red}${msg}${C.reset}`),
  perf:   (tag, msg) => console.log(`${C.magenta}${C.bold}[${tag}]${C.reset} ${C.magenta}${msg}${C.reset}`),
  mem:    (tag, msg) => console.log(`${C.blue}${C.bold}[${tag}]${C.reset} ${C.blue}${msg}${C.reset}`),
  banner: (msg)      => console.log(`${C.bgMagenta}${C.white}${C.bold}${msg}${C.reset}`)
};

const _logStack = new Map();
function logStacked(level, tag, msg, delayMs = 1000) {
  const key = `${level}|${tag}|${msg}`;
  if (_logStack.has(key)) {
    _logStack.get(key).count++;
    return;
  }
  const entry = { count: 1, timer: null };
  entry.timer = setTimeout(() => {
    const suffix = entry.count > 1 ? ` ${C.dim}(x${entry.count})${C.reset}` : "";
    log[level](tag, msg + suffix);
    _logStack.delete(key);
  }, delayMs);
  _logStack.set(key, entry);
}

const PACK_NAME         = "Denji Sticker pack:";
const AUTHOR            = "denji.indevs.in";
const PREFIX            = [".", "-", "d!", "+", "#"];
const COMMANDS          = ["sly", "stickerly", "setpackname", "setauthor", "setwm", "swgc"];
const TEMP_DIR          = path.join(__dirname, "temp");
const BATCH_SIZE        = 50;
const MAX_STICKER_BYTES = 900 * 1024;
const CPU_COUNT         = os.cpus().length;
const TOTAL_MEMORY      = os.totalmem();
const DB_FILE           = path.join(__dirname, "database.json");
const MAX_PACK_STICKERS = 60;
const OMEGATECH_BASE    = "https://omegatech-api.dixonomega.tech/api/tools/Sticker";

function getAdaptiveConcurrency() {
  const freeMB  = os.freemem() / 1024 / 1024;
  const cpuBase = CPU_COUNT;
  const memBased = Math.floor(freeMB / 60);
  return Math.max(2, Math.min(cpuBase * 3, memBased));
}

const BASE_CONCURRENCY           = getAdaptiveConcurrency();
const MAX_CONCURRENT_DOWNLOADS   = BASE_CONCURRENCY;
const MAX_CONCURRENT_CONVERSIONS = Math.max(2, Math.floor(BASE_CONCURRENCY * 0.7));
const MAX_CONCURRENT_UPLOADS     = Math.max(2, Math.floor(BASE_CONCURRENCY * 0.5));
const FFMPEG_THREADS             = Math.max(2, Math.floor(CPU_COUNT / 2));

sharp.cache({ memory: 50, files: 0, items: 50 });
sharp.concurrency(Math.max(1, Math.floor(CPU_COUNT / 2)));
sharp.simd(true);

axios.defaults.timeout = 30000;
axios.defaults.maxContentLength = 100 * 1024 * 1024;
axios.defaults.maxBodyLength    = 100 * 1024 * 1024;
axios.defaults.httpAgent  = new (await import('http')).Agent({ keepAlive: true, maxSockets: MAX_CONCURRENT_DOWNLOADS, maxFreeSockets: CPU_COUNT });
axios.defaults.httpsAgent = new (await import('https')).Agent({ keepAlive: true, maxSockets: MAX_CONCURRENT_DOWNLOADS, maxFreeSockets: CPU_COUNT });

if (!fs.existsSync(TEMP_DIR)) fs.mkdirSync(TEMP_DIR, { recursive: true });

const maxMemMB = Math.floor((TOTAL_MEMORY / 1024 / 1024) * 0.75);

log.banner(` DENJI STICKER BOT `);
log.perf("INIT",
  `CPU: ${CPU_COUNT} cores | ` +
  `RAM: ${(TOTAL_MEMORY/1024/1024/1024).toFixed(1)}GB total / ${(os.freemem()/1024/1024/1024).toFixed(1)}GB free | ` +
  `Heap limit: ${maxMemMB}MB`
);
log.perf("INIT",
  `DL: ${MAX_CONCURRENT_DOWNLOADS} | ` +
  `Conv: ${MAX_CONCURRENT_CONVERSIONS} | ` +
  `Upload: ${MAX_CONCURRENT_UPLOADS} | ` +
  `FFmpeg threads: ${FFMPEG_THREADS} | ` +
  `Batch: ${BATCH_SIZE}`
);

const swgcPendingSessions = new Map();

class Semaphore {
  constructor(max, memThresholdMB = 200) {
    this.max = max; this.count = 0; this.queue = [];
    this.memThreshold = memThresholdMB * 1024 * 1024;
  }

  async acquire() {
    while (os.freemem() < this.memThreshold) {
      if (global.gc) global.gc();
      await new Promise(r => setTimeout(r, 300));
    }
    if (this.count < this.max) { this.count++; return; }
    await new Promise(resolve => this.queue.push(resolve));
  }

  release() {
    this.count--;
    if (this.queue.length > 0) { this.count++; this.queue.shift()(); }
  }

  async use(fn) {
    await this.acquire();
    try { return await fn(); } finally { this.release(); }
  }
}

const TOTAL_MEM_MB       = TOTAL_MEMORY / 1024 / 1024;
const DL_MEM_THRESHOLD   = Math.max(50,  Math.floor(TOTAL_MEM_MB * 0.08));
const CONV_MEM_THRESHOLD = Math.max(60,  Math.floor(TOTAL_MEM_MB * 0.10));
const UP_MEM_THRESHOLD   = Math.max(40,  Math.floor(TOTAL_MEM_MB * 0.06));

const downloadSemaphore   = new Semaphore(MAX_CONCURRENT_DOWNLOADS,   DL_MEM_THRESHOLD);
const conversionSemaphore = new Semaphore(MAX_CONCURRENT_CONVERSIONS, CONV_MEM_THRESHOLD);
const uploadSemaphore     = new Semaphore(MAX_CONCURRENT_UPLOADS,     UP_MEM_THRESHOLD);

let _lastMemWarn = 0;
async function checkMemoryPressure() {
  const freeMB = os.freemem() / 1024 / 1024;
  if (freeMB < 100) {
    if (global.gc) global.gc();
    const now = Date.now();
    if (now - _lastMemWarn > 10_000) {
      log.warn("MEM", `Free RAM low (${freeMB.toFixed(0)}MB), throttling...`);
      _lastMemWarn = now;
    }
    await new Promise(r => setTimeout(r, 500));
    return true;
  }
  return false;
}

class BufferPool {
  constructor(maxSize = 20, maxItemBytes = 1 * 1024 * 1024) {
    this.pool = []; this.maxSize = maxSize; this.maxItemBytes = maxItemBytes;
  }
  get(size) {
    const idx = this.pool.findIndex(b => b.length >= size);
    if (idx !== -1) return this.pool.splice(idx, 1)[0].slice(0, size);
    return Buffer.allocUnsafe(size);
  }
  return(buffer) {
    if (this.pool.length < this.maxSize && buffer.length <= this.maxItemBytes)
      this.pool.push(buffer);
  }
  clear() { this.pool = []; if (global.gc) global.gc(); }
}

const bufferPool = new BufferPool(20, 1 * 1024 * 1024);

// ─── Database ──────────────────────────────────────────────────────────────────
function loadDB() {
  try {
    if (!fs.existsSync(DB_FILE)) return { sessions: {}, settings: {} };
    const parsed = JSON.parse(fs.readFileSync(DB_FILE, "utf-8"));
    if (!parsed.sessions) parsed.sessions = {};
    if (!parsed.settings) parsed.settings = {};
    return parsed;
  } catch (_) { return { sessions: {}, settings: {} }; }
}

function saveDB(db) {
  try { fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2)); }
  catch (err) { log.warn("DB", `Save failed: ${err.message}`); }
}

global.__db = loadDB();

function getSession(jid)        { return global.__db.sessions[jid] || null; }
function setSession(jid, s)     { global.__db.sessions[jid] = s; saveDB(global.__db); }
function deleteSession(jid)     { delete global.__db.sessions[jid]; saveDB(global.__db); }
function getSettings()          { if (!global.__db.settings) global.__db.settings = {}; return global.__db.settings; }
function getPackName()          { return getSettings().packName || PACK_NAME; }
function getAuthor()            { return getSettings().author  || AUTHOR; }
function saveSettings(partial)  { global.__db.settings = { ...getSettings(), ...partial }; saveDB(global.__db); }

// ─── EXIF / WebP helpers ───────────────────────────────────────────────────────
function buildExif(packname, author, categories = [""], isPremium = false, isAntiSteal = false) {
  const json = {
    "sticker-pack-id": (isPremium || isAntiSteal)
      ? "2be7e369-b5ce-4706-a3d4-f78805a20328"
      : Crypto.randomBytes(32).toString("hex"),
    "sticker-pack-name": packname,
    "sticker-pack-publisher": author,
    emojis: categories
  };

  if (isPremium) Object.assign(json, {
    "accessibility-text": "Premium Sticker",
    "android-app-store-link": "https://whatsapp.com",
    "ios-app-store-link": "https://whatsapp.com/ios",
    "is-from-sticker-maker": 0, "is-avatar-sticker": 1,
    "avatar-sticker-template-id": "whatsapp", "is-ai-sticker": 1,
    "is-avatar-country-sticker": 1, "is-avatar-instant-sticker": 1,
    "sticker-maker-source-type": 4, "is-avatar-social-sticker": 1,
    "avatar-sticker-style": "whatsapp", "avatar-sticker-revision-id": "2026",
    "is-from-user-created-pack": 1, "origin-pack-id": "whatsapp",
    "is-text-sticker": 1, "premium": 1
  });

  if (isAntiSteal && !isPremium) Object.assign(json, {
    "accessibility-text": "Protected Sticker",
    "android-app-store-link": "https://whatsapp.com",
    "ios-app-store-link": "https://whatsapp.com/ios",
    "is-from-sticker-maker": 0, "is-avatar-sticker": 1,
    "avatar-sticker-template-id": "whatsapp", "is-ai-sticker": 1,
    "is-avatar-country-sticker": 1, "is-avatar-instant-sticker": 1,
    "sticker-maker-source-type": 4, "is-avatar-social-sticker": 1,
    "avatar-sticker-style": "whatsapp", "avatar-sticker-revision-id": "2026",
    "is-from-user-created-pack": 1, "origin-pack-id": "whatsapp",
    "is-text-sticker": 1
  });

  const exifAttr = Buffer.from([
    0x49,0x49,0x2a,0x00,0x08,0x00,0x00,0x00,
    0x01,0x00,0x41,0x57,0x07,0x00,0x00,0x00,
    0x00,0x00,0x16,0x00,0x00,0x00
  ]);
  const jsonBuffer = Buffer.from(JSON.stringify(json), "utf-8");
  const exif = Buffer.concat([exifAttr, jsonBuffer]);
  exif.writeUIntLE(jsonBuffer.length, 14, 4);
  return exif;
}

async function addExifToBuffer(webpBuffer, packname, author, isPremium = false, isAntiSteal = false) {
  try {
    const img = new webp.Image();
    await img.load(webpBuffer);
    img.exif = buildExif(packname, author, [""], isPremium, isAntiSteal);
    const result = await img.save(null);
    bufferPool.return(webpBuffer);
    return result;
  } catch (err) {
    logStacked("warn", "EXIF", `Failed: ${err.message}`);
    return webpBuffer;
  }
}

function detectMimeType(buffer) {
  if (!buffer || buffer.length < 12) return null;
  if (buffer[0]===0x89&&buffer[1]===0x50&&buffer[2]===0x4e&&buffer[3]===0x47) return "image/png";
  if (buffer[0]===0xff&&buffer[1]===0xd8&&buffer[2]===0xff) return "image/jpeg";
  if (buffer[0]===0x52&&buffer[1]===0x49&&buffer[2]===0x46&&buffer[3]===0x46&&
      buffer[8]===0x57&&buffer[9]===0x45&&buffer[10]===0x42&&buffer[11]===0x50) return "image/webp";
  if (buffer[0]===0x47&&buffer[1]===0x49&&buffer[2]===0x46) return "image/gif";
  if (buffer[4]===0x66&&buffer[5]===0x74&&buffer[6]===0x79&&buffer[7]===0x70) return "video/mp4";
  if (buffer[0]===0x1a&&buffer[1]===0x45&&buffer[2]===0xdf&&buffer[3]===0xa3) return "video/webm";
  return null;
}

function getFileExtension(mimeType) {
  return { "image/png":"png","image/jpeg":"jpg","image/webp":"webp","image/gif":"gif","video/mp4":"mp4","video/webm":"webm" }[mimeType] || "tmp";
}

async function validateBuffer(buffer, mimeType) {
  if (buffer.length < 100)            throw new Error("Buffer too small");
  if (buffer.length > 100*1024*1024)  throw new Error("Buffer too large");
  if (mimeType?.startsWith("image/")) {
    const meta = await sharp(buffer, { limitInputPixels: false }).metadata();
    if (!meta.width || !meta.height)  throw new Error("Cannot detect image dimensions");
  }
  return true;
}

function uid() { return Crypto.randomBytes(6).readUIntLE(0,6).toString(36); }

async function imageToWebpFfmpeg(mediaBuffer, mimeType) {
  return new Promise((resolve, reject) => {
    const ext   = getFileExtension(mimeType);
    const tmpIn = path.join(tmpdir(), `${uid()}.${ext}`);
    const tmpOut= path.join(tmpdir(), `${uid()}.webp`);
    try { fs.writeFileSync(tmpIn, mediaBuffer); }
    catch (e) { return reject(new Error(`Failed to write temp: ${e.message}`)); }

    const cmd = ff(tmpIn);
    let timedOut = false;
    const timer = setTimeout(() => { timedOut = true; cmd.kill("SIGKILL"); }, 20000);

    cmd
      .on("error", (err) => { clearTimeout(timer); safeUnlink(tmpIn, tmpOut); reject(new Error(`FFmpeg error: ${err.message}`)); })
      .on("end", () => {
        clearTimeout(timer);
        if (timedOut) { safeUnlink(tmpIn, tmpOut); return reject(new Error("FFmpeg timeout")); }
        try {
          const buff = fs.readFileSync(tmpOut);
          if (!buff.length) throw new Error("Empty output");
          safeUnlink(tmpIn, tmpOut);
          resolve(buff);
        } catch (e) { safeUnlink(tmpIn, tmpOut); reject(e); }
      })
      .addOutputOptions([
        "-c:v","libwebp","-quality","100","-lossless","1",
        "-threads",FFMPEG_THREADS.toString(),
        "-vf","scale='min(512,iw)':min'(512,ih)':force_original_aspect_ratio=decrease,fps=15,pad=512:512:-1:-1:color=white@0.0",
        "-preset","default","-compression_level","6"
      ])
      .toFormat("webp").save(tmpOut);
  });
}

async function videoToWebpFfmpeg(mediaBuffer, mimeType) {
  return new Promise((resolve, reject) => {
    const ext   = getFileExtension(mimeType);
    const tmpIn = path.join(tmpdir(), `${uid()}.${ext}`);
    const tmpOut= path.join(tmpdir(), `${uid()}.webp`);
    try { fs.writeFileSync(tmpIn, mediaBuffer); }
    catch (e) { return reject(new Error(`Failed to write temp: ${e.message}`)); }

    const cmd = ff(tmpIn);
    let timedOut = false;
    const timer = setTimeout(() => { timedOut = true; cmd.kill("SIGKILL"); }, 45000);

    cmd
      .on("error", (err) => { clearTimeout(timer); safeUnlink(tmpIn, tmpOut); reject(new Error(`FFmpeg video error: ${err.message}`)); })
      .on("end", () => {
        clearTimeout(timer);
        if (timedOut) { safeUnlink(tmpIn, tmpOut); return reject(new Error("FFmpeg timeout")); }
        try {
          const buff = fs.readFileSync(tmpOut);
          if (!buff.length) throw new Error("Empty output");
          safeUnlink(tmpIn, tmpOut);
          resolve(buff);
        } catch (e) { safeUnlink(tmpIn, tmpOut); reject(e); }
      })
      .addOutputOptions([
        "-c:v","libwebp","-quality","100",
        "-threads",FFMPEG_THREADS.toString(),
        "-vf","scale='min(512,iw)':min'(512,ih)':force_original_aspect_ratio=decrease,fps=20,pad=512:512:-1:-1:color=white@0.0",
        "-loop","0","-ss","00:00:00","-t","00:00:10",
        "-preset","default","-an","-vsync","0","-compression_level","6"
      ])
      .toFormat("webp").save(tmpOut);
  });
}

async function compressStaticQuality(buffer) {
  try {
    let result = await sharp(buffer, { limitInputPixels: false }).webp({ lossless: true, effort: 6 }).toBuffer();
    if (result.length <= MAX_STICKER_BYTES) { bufferPool.return(buffer); return result; }
    for (const q of [100,95,90,85]) {
      result = await sharp(buffer, { limitInputPixels: false }).webp({ quality: q, effort: 6 }).toBuffer();
      if (result.length <= MAX_STICKER_BYTES) { bufferPool.return(buffer); return result; }
    }
    const meta = await sharp(buffer).metadata();
    const resized = await sharp(buffer, { limitInputPixels: false })
      .resize(Math.round((meta.width||512)*0.9), Math.round((meta.height||512)*0.9), { fit:"inside", withoutEnlargement:true })
      .webp({ quality: 90, effort: 6 }).toBuffer();
    bufferPool.return(buffer);
    return resized;
  } catch (err) { throw new Error(`compressStatic failed: ${err.message}`); }
}

async function compressAnimatedQuality(buffer) {
  try {
    let result = await sharp(buffer, { animated:true, limitInputPixels:false }).webp({ lossless:true, effort:5, loop:0 }).toBuffer();
    if (result.length <= MAX_STICKER_BYTES) { bufferPool.return(buffer); return result; }
    for (const q of [100,95,90,85]) {
      result = await sharp(buffer, { animated:true, limitInputPixels:false }).webp({ quality:q, effort:5, loop:0 }).toBuffer();
      if (result.length <= MAX_STICKER_BYTES) { bufferPool.return(buffer); return result; }
    }
    const meta = await sharp(buffer, { animated:true }).metadata();
    const resized = await sharp(buffer, { animated:true, limitInputPixels:false })
      .resize(Math.round((meta.width||512)*0.9), Math.round((meta.height||512)*0.9), { fit:"inside", withoutEnlargement:true })
      .webp({ quality:90, effort:5, loop:0 }).toBuffer();
    bufferPool.return(buffer);
    return resized;
  } catch (err) { throw new Error(`compressAnimated failed: ${err.message}`); }
}

async function convertAndEmbed(rawBuffer, isAnimated, packname, author, isPremium=false, isAntiSteal=false) {
  const mimeType = detectMimeType(rawBuffer);
  if (!mimeType) throw new Error("Cannot detect file format");
  await validateBuffer(rawBuffer, mimeType);

  let webpBuffer;
  if (mimeType === "image/webp") {
    webpBuffer = rawBuffer;
  } else {
    try {
      webpBuffer = isAnimated
        ? await videoToWebpFfmpeg(rawBuffer, mimeType)
        : await imageToWebpFfmpeg(rawBuffer, mimeType);
    } catch {
      webpBuffer = isAnimated
        ? await compressAnimatedQuality(rawBuffer)
        : await compressStaticQuality(rawBuffer);
    }
  }

  if (webpBuffer.length > MAX_STICKER_BYTES)
    webpBuffer = isAnimated ? await compressAnimatedQuality(webpBuffer) : await compressStaticQuality(webpBuffer);

  try {
    const withExif = await addExifToBuffer(webpBuffer, packname, author, isPremium, isAntiSteal);
    return withExif.length <= MAX_STICKER_BYTES ? withExif : webpBuffer;
  } catch { return webpBuffer; }
}

async function buildEmbeddedCover(stickerBuffer) {
  const strategies = [
    async () => sharp(stickerBuffer, { animated:false, limitInputPixels:false })
      .resize(96, 96, { fit:"contain", background:{r:255,g:255,b:255,alpha:1} })
      .flatten({ background:"#ffffff" }).jpeg({ quality:75, progressive:false }).toBuffer(),
    async () => {
      const png = await sharp(stickerBuffer, { limitInputPixels:false }).toFormat("png").toBuffer();
      return sharp(png).resize(96,96,{fit:"contain",background:{r:255,g:255,b:255,alpha:1}})
        .flatten({background:"#ffffff"}).jpeg({quality:75}).toBuffer();
    },
    async () => sharp(stickerBuffer, { animated:false, limitInputPixels:false })
      .flatten({background:"#ffffff"}).resize(96,96).jpeg({quality:70}).toBuffer(),
    async () => sharp({ create:{width:96,height:96,channels:3,background:{r:255,g:255,b:255}} })
      .jpeg({quality:60}).toBuffer()
  ];

  for (let i = 0; i < strategies.length; i++) {
    try {
      const buf = await strategies[i]();
      if (Buffer.isBuffer(buf) && buf.length > 0) {
        const cover = Buffer.allocUnsafe(buf.length);
        buf.copy(cover);
        bufferPool.return(buf);
        return cover;
      }
    } catch (err) { logStacked("warn", "COVER", `Strategy ${i+1} failed: ${err.message}`); }
  }
  throw new Error("All cover strategies failed");
}

function saveTempFile(buffer, filename) {
  const fp = path.join(TEMP_DIR, filename);
  fs.writeFileSync(fp, buffer);
  return fp;
}

function safeUnlink(...files) {
  for (const f of files)
    try { if (fs.existsSync(f)) fs.unlinkSync(f); } catch (_) {}
}

function parseMessage(msgText) {
  if (!msgText) return null;
  const usedPrefix = PREFIX.find(p => msgText.startsWith(p));
  if (!usedPrefix) return null;
  const withoutPrefix = msgText.slice(usedPrefix.length).trim();
  const [rawCmd, ...args] = withoutPrefix.split(/\s+/);
  return { prefix: usedPrefix, command: rawCmd.toLowerCase(), text: args.join(" ").trim() };
}

function getRawArgs(msgText, usedPrefix, command) {
  const withoutPrefix = msgText.slice(usedPrefix.length);
  const cmdIndex = withoutPrefix.toLowerCase().indexOf(command.toLowerCase());
  if (cmdIndex === -1) return "";
  return withoutPrefix.slice(cmdIndex + command.length).replace(/^\s+/, "");
}

function getMessageText(msg) {
  const m = msg.message;
  if (!m) return "";
  return (
    m.conversation ||
    m.extendedTextMessage?.text ||
    m.imageMessage?.caption ||
    m.videoMessage?.caption ||
    ""
  );
}

async function downloadBuffer(url, retries = 2) {
  for (let i = 0; i < retries; i++) {
    try {
      const res = await axios.get(url, {
        responseType: "arraybuffer", timeout: 15000,
        headers: {
          "user-agent": "androidapp.stickerly/3.17.0 (Redmi Note 4; U; Android 29; in-ID; id;)",
          "accept-encoding": "gzip, deflate, br", "connection": "keep-alive"
        },
        maxRedirects: 5, decompress: true
      });
      const buffer = Buffer.from(res.data);
      if (buffer.toString().includes("error") || buffer.toString().includes("404"))
        throw new Error("Server error response");
      return buffer;
    } catch (err) {
      if (i === retries - 1) throw err;
      await new Promise(r => setTimeout(r, 200 * (i + 1)));
    }
  }
}

async function react(sock, jid, msg, emoji) {
  try { await sock.sendMessage(jid, { react: { text: emoji, key: msg.key } }); } catch (_) {}
}

class ProgressTracker {
  constructor(total, label = "Progress") {
    this.total = total; this.current = 0; this.label = label;
    this.startTime = Date.now(); this.lastUpdate = 0;
  }

  increment() {
    this.current++;
    const now = Date.now();
    if (now - this.lastUpdate > 1000 || this.current === this.total) {
      this.display(); this.lastUpdate = now;
    }
  }

  display() {
    const percent = ((this.current / this.total) * 100).toFixed(1);
    const elapsed = ((Date.now() - this.startTime) / 1000).toFixed(1);
    const rate    = (this.current / (elapsed || 1)).toFixed(1);
    const eta     = this.current > 0 ? (((this.total - this.current) / rate) || 0).toFixed(0) : '?';
    process.stdout.write(
      `\r${C.cyan}[${this.label}]${C.reset} ${C.white}${this.current}/${this.total}${C.reset} ` +
      `${C.yellow}(${percent}%)${C.reset} | ${C.green}${rate}/s${C.reset} | ` +
      `${C.magenta}ETA: ${eta}s${C.reset}` + ' '.repeat(10)
    );
    if (this.current === this.total) console.log();
  }
}

// ─── StickerLy via Omegatech API ──────────────────────────────────────────────
class StickerLy {
  detail = async (url) => {
    const { data } = await axios.get(OMEGATECH_BASE, {
      params: { action: "pack", id: url, needRelation: true },
      timeout: 20000
    });

    if (!data.success) throw new Error("Omegatech API error: " + (data.message || "unknown"));

    const result = data.data.result;
    const prefix = result.resourceUrlPrefix;

    return {
      name: result.name,
      author: {
        name: result.user?.displayName || result.authorName,
        username: result.authorName
      },
      stickers: result.stickers.map(s => ({
        fileName: s.fileName,
        isAnimated: s.isAnimated || s.animated || false,
        imageUrl: `${prefix}${s.fileName}`
      })),
      stickerCount: result.stickers.length,
      viewCount: result.viewCount,
      exportCount: result.exportCount,
      isPaid: result.isPaid,
      isAnimated: result.isAnimated || result.animated || false,
      trayIndex: result.trayIndex ?? 0,
      url: result.shareUrl
    };
  };
}

async function processInChunks(items, chunkSize, processor) {
  const results = [];
  for (let i = 0; i < items.length; i += chunkSize) {
    await checkMemoryPressure();
    const chunk = items.slice(i, i + chunkSize);
    const chunkResults = await Promise.allSettled(chunk.map(processor));
    results.push(...chunkResults);
    if (global.gc && i % (chunkSize * 3) === 0) global.gc();
  }
  return results;
}

async function sendStickerPack(sock, jid, detail, msg, isPremium=false, isAntiSteal=false) {
  const { stickers, trayIndex } = detail;
  const packname   = getPackName();
  const author     = getAuthor();
  const ts         = Date.now();
  const startTotal = Date.now();

  const flags = [isPremium && "Premium", isAntiSteal && "Anti-Steal"].filter(Boolean).join(", ");
  log.info("PACK", `"${detail.name}" | ${stickers.length} stickers${flags ? ` | ${flags}` : ""}`);

  // ── Phase 1: Download ────────────────────────────────────────────────────────
  const downloadProgress = new ProgressTracker(stickers.length, "DOWNLOAD");
  let dlFail = 0;

  const downloadTasks = stickers.map((s, i) => async () => {
    try {
      const buffer = await downloadSemaphore.use(() => downloadBuffer(s.imageUrl));
      downloadProgress.increment();
      return { index: i, buffer, isAnimated: s.isAnimated };
    } catch (err) {
      dlFail++;
      downloadProgress.increment();
      return null;
    }
  });

  const downloadResults = await processInChunks(downloadTasks, MAX_CONCURRENT_DOWNLOADS, t => t());
  const downloadedData  = downloadResults.map(r => r.status==="fulfilled"?r.value:null).filter(Boolean);

  log.ok("PACK", `DL: ${downloadedData.length}/${stickers.length}${dlFail ? ` (${dlFail} failed)` : ""}`);
  if (downloadedData.length === 0) throw new Error("No stickers downloaded");

  // ── Phase 2: Convert ─────────────────────────────────────────────────────────
  const convertProgress = new ProgressTracker(downloadedData.length, "CONVERT");
  let convFail = 0;

  const conversionTasks = downloadedData.map(item => async () => {
    try {
      const webpBuffer = await conversionSemaphore.use(() =>
        convertAndEmbed(item.buffer, item.isAnimated, packname, author, isPremium, isAntiSteal)
      );
      const filePath = saveTempFile(webpBuffer, `stk_${ts}_${item.index}.webp`);
      convertProgress.increment();
      bufferPool.return(item.buffer);
      return { index: item.index, filePath, buffer: webpBuffer };
    } catch (err) {
      convFail++;
      convertProgress.increment();
      return null;
    }
  });

  const conversionResults = await processInChunks(conversionTasks, MAX_CONCURRENT_CONVERSIONS, t => t());
  const stickerData       = conversionResults.map(r => r.status==="fulfilled"?r.value:null).filter(Boolean);

  log.ok("PACK", `Conv: ${stickerData.length}/${downloadedData.length}${convFail ? ` (${convFail} failed)` : ""}`);
  if (stickerData.length === 0) throw new Error("No stickers converted");

  downloadedData.length = 0;
  if (global.gc) global.gc();

  // ── Phase 3: Cover ───────────────────────────────────────────────────────────
  const coverIdx        = (trayIndex != null && stickerData[trayIndex]) ? trayIndex : 0;
  const coverJpegBuffer = await buildEmbeddedCover(stickerData[coverIdx].buffer);

  // ── Phase 4: Send ────────────────────────────────────────────────────────────
  const totalBatch = Math.ceil(stickerData.length / BATCH_SIZE);
  const sendProgress = new ProgressTracker(totalBatch, "SEND");
  let successSEND = 0;

  const sendTasks = [];
  for (let b = 0; b < totalBatch; b++) {
    const batchData    = stickerData.slice(b * BATCH_SIZE, (b+1) * BATCH_SIZE);
    const batchPackName= totalBatch > 1 ? `${packname} (${b+1}/${totalBatch})` : packname;

    sendTasks.push(async () => {
      try {
        await uploadSemaphore.use(async () => {
          await sock.sendMessage(jid, {
            cover: coverJpegBuffer,
            stickers: batchData.map(item => ({
              data: { url: item.filePath }, packName: batchPackName, author, jpegThumbnail: coverJpegBuffer
            })),
            name: batchPackName, publisher: author, description: detail.url, jpegThumbnail: coverJpegBuffer
          }, { quoted: msg });
        });
        sendProgress.increment();
        successSEND++;
        safeUnlink(...batchData.map(d => d.filePath));
        return true;
      } catch (err) {
        sendProgress.increment();
        let fallbackOk = 0;
        for (const item of batchData) {
          try {
            const stickerBuf = fs.readFileSync(item.filePath);
            await sock.sendMessage(jid, { sticker: stickerBuf, jpegThumbnail: coverJpegBuffer, packName: batchPackName, author }, { quoted: msg });
            fallbackOk++;
            safeUnlink(item.filePath);
          } catch (_) {}
        }
        if (fallbackOk > 0) successSEND++;
        return fallbackOk > 0;
      }
    });
  }

  await processInChunks(sendTasks, MAX_CONCURRENT_UPLOADS, t => t());

  bufferPool.clear();
  if (global.gc) global.gc();

  const totalTime = ((Date.now() - startTotal) / 1000).toFixed(2);
  const rate      = (stickers.length / parseFloat(totalTime)).toFixed(2);

  log.ok("PACK",
    `Done | ${stickers.length} stickers | ${successSEND}/${totalBatch} batches | ` +
    `${totalTime}s | ${rate}/s`
  );

  if (successSEND === 0) throw new Error("All batches failed");
}

function parseFlags(text) {
  const lower = (text || "").toLowerCase();
  return {
    isPremium:   lower.includes("--prem") || lower.includes("--premium") || lower.includes("-p"),
    isAntiSteal: lower.includes("--antisteal") || lower.includes("--anticolong") || lower.includes("--as")
  };
}

function extractUrls(text) {
  if (!text) return [];
  return text.match(/https?:\/\/[^\s]+/g) || [];
}

function chunkArray(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) chunks.push(arr.slice(i, i + size));
  return chunks;
}

// ─── SWGC HELPERS ─────────────────────────────────────────────────────────────

function getQuotedMedia(msg) {
  const m = msg.message;
  if (!m) return null;
  const quoted =
    m.extendedTextMessage?.contextInfo?.quotedMessage ||
    m.imageMessage?.contextInfo?.quotedMessage ||
    m.videoMessage?.contextInfo?.quotedMessage ||
    null;
  if (!quoted) return null;
  if (quoted.imageMessage)       return { type:"image",   quotedMsg: quoted.imageMessage,       msgType:"imageMessage"       };
  if (quoted.videoMessage)       return { type:"video",   quotedMsg: quoted.videoMessage,       msgType:"videoMessage"       };
  if (quoted.gifPlaybackMessage) return { type:"gif",     quotedMsg: quoted.gifPlaybackMessage, msgType:"gifPlaybackMessage" };
  if (quoted.stickerMessage)     return { type:"sticker", quotedMsg: quoted.stickerMessage,     msgType:"stickerMessage"     };
  return null;
}

async function downloadQuotedMedia(sock, msg) {
  const m = msg.message;
  if (!m) return null;
  const contextInfo =
    m.extendedTextMessage?.contextInfo ||
    m.imageMessage?.contextInfo ||
    m.videoMessage?.contextInfo ||
    null;
  if (!contextInfo?.quotedMessage) return null;

  const stanzaId    = contextInfo.stanzaId;
  const participant = contextInfo.participant || contextInfo.remoteJid || msg.key.remoteJid;
  const quotedMsg   = contextInfo.quotedMessage;

  let mediaType = null, mediaMsg = null;
  if      (quotedMsg.imageMessage)   { mediaType = "imageMessage";   mediaMsg = quotedMsg.imageMessage;   }
  else if (quotedMsg.videoMessage)   { mediaType = "videoMessage";   mediaMsg = quotedMsg.videoMessage;   }
  else if (quotedMsg.stickerMessage) { mediaType = "stickerMessage"; mediaMsg = quotedMsg.stickerMessage; }
  else return null;

  const fakeMsg = {
    key: { remoteJid: msg.key.remoteJid, fromMe: false, id: stanzaId },
    message: { [mediaType]: mediaMsg }
  };

  try {
    const stream = await downloadMediaMessage(fakeMsg, "buffer", {}, {
      logger: pino({ level:"silent" }), reuploadRequest: sock.updateMediaMessage
    });
    return { buffer: stream, mimeType: mediaMsg.mimetype || "image/jpeg", type: mediaType };
  } catch (err) {
    log.error("SWGC", `Download quoted media failed: ${err.message}`);
    return null;
  }
}

async function prepareMediaForStatus(buffer, mimeType) {
  try {
    const isVideo = mimeType?.startsWith("video/");
    if (isVideo) {
      const tmpIn  = path.join(tmpdir(), `${uid()}.mp4`);
      const tmpOut = path.join(tmpdir(), `${uid()}.jpg`);
      fs.writeFileSync(tmpIn, buffer);
      await new Promise((resolve, reject) => {
        ff(tmpIn)
          .on("error", (e) => { safeUnlink(tmpIn, tmpOut); reject(e); })
          .on("end",   ()  => { safeUnlink(tmpIn);          resolve(); })
          .screenshots({ count:1, filename:path.basename(tmpOut), folder:path.dirname(tmpOut), timemarks:["00:00:00"] });
      });
      if (fs.existsSync(tmpOut)) {
        const frame = fs.readFileSync(tmpOut); safeUnlink(tmpOut);
        return await sharp(frame).resize(512,512,{fit:"inside",withoutEnlargement:true}).jpeg({quality:80}).toBuffer();
      }
      return null;
    }
    return await sharp(buffer, { animated:false, limitInputPixels:false })
      .resize(512,512,{fit:"inside",withoutEnlargement:true}).jpeg({quality:80}).toBuffer();
  } catch (err) {
    logStacked("warn", "SWGC", `prepareMediaForStatus failed: ${err.message}`);
    return null;
  }
}

async function getEligibleGroups(sock) {
  try {
    const allGroups = await sock.groupFetchAllParticipating();
    const eligible  = [];
    for (const [jid, meta] of Object.entries(allGroups)) {
      if (meta.announce) continue;
      eligible.push({ jid, name: meta.subject || jid, participants: meta.participants?.length || 0 });
    }
    eligible.sort((a, b) => a.name.localeCompare(b.name));
    return eligible;
  } catch (err) {
    log.error("SWGC", `Gagal fetch groups: ${err.message}`);
    return [];
  }
}

async function sendGroupList(sock, senderJid, msg, swgcPayload) {
  const groups = await getEligibleGroups(sock);

  if (groups.length === 0) {
    await sock.sendMessage(senderJid, {
      text: "❌ Tidak ada grup yang tersedia (tidak ada grup terbuka atau bot belum bergabung ke grup manapun)."
    }, { quoted: msg });
    return;
  }

  swgcPendingSessions.set(senderJid, { groups, payload: swgcPayload, ts: Date.now() });

  let listText = `📋 *Pilih grup tujuan* untuk mengirim status grup:\n\n`;
  listText += `Ketik nomor grup yang ingin dituju.\n`;
  listText += `Ketik *0* atau *batal* untuk membatalkan.\n\n`;
  groups.forEach((g, i) => {
    listText += `*${i+1}.* ${g.name}\n`;
    listText += `     👥 ${g.participants} anggota | \`${g.jid}\`\n\n`;
  });
  listText += `\nSesi ini akan kedaluwarsa dalam *5 menit*.`;

  await sock.sendMessage(senderJid, { text: listText }, { quoted: msg });
}

async function sendSwgcToGroup(sock, targetGroupJid, payload, senderMsg, senderJid) {
  const { text: customText, mediaData } = payload;

  if (mediaData) {
    const { buffer, mimeType, type } = mediaData;
    const isVideo = type === "videoMessage";
    await prepareMediaForStatus(buffer, mimeType);

    try {
      const prepared = await prepareWAMessageMedia(
        isVideo ? { video: buffer, caption: customText || "" } : { image: buffer, caption: customText || "" },
        { upload: sock.waUploadToServer }
      );

      const sharedContextInfo = {
        mentionedJid: [], groupMentions: [],
        statusAttributions: [{ type: 10 }],
        forwardingScore: 0,
        featureEligibilities: { canBeReshared: true, canReceiveMultiReact: true },
        statusSourceType: 4,
        statusAudienceMetadata: { audienceType: 1 }
      };

      const finalPayload = isVideo
        ? { videoMessage: { ...prepared.videoMessage, caption: customText||"", contextInfo: sharedContextInfo } }
        : { imageMessage: { ...prepared.imageMessage, caption: customText||"", contextInfo: sharedContextInfo } };

      await sock.relayMessage(targetGroupJid, {
        messageContextInfo: { messageSecret: "BrRzGQ6/B0ddqBuasejEf+rJKLQ2pauxHtAw1nIMPvw=" },
        groupStatusMessageV2: { message: { ...finalPayload } }
      }, {});

      await sock.sendMessage(senderJid, { text: `✅ Status media berhasil dikirim ke grup!` }, { quoted: senderMsg });
      await react(sock, senderJid, senderMsg, "✅");
    } catch (err) {
      log.error("SWGC", `Gagal kirim status media: ${err.message}`);
      await react(sock, senderJid, senderMsg, "❌");
      await sock.sendMessage(senderJid, { text: `❌ Gagal mengirim status media: ${err.message}` }, { quoted: senderMsg });
    }
    return;
  }

  try {
    await sock.relayMessage(targetGroupJid, {
      messageContextInfo: { messageSecret: "BrRzGQ6/B0ddqBuasejEf+rJKLQ2pauxHtAw1nIMPvw=" },
      groupStatusMessageV2: {
        message: {
          extendedTextMessage: {
            endCardTiles: [],
            text: customText, textArgb: 4294967040, backgroundArgb: 4280669030,
            font: 5, previewType: 0,
            contextInfo: {
              mentionedJid: [], groupMentions: [],
              statusAttributions: [{ type: 10 }],
              forwardingScore: 0,
              featureEligibilities: { canBeReshared: true, canReceiveMultiReact: true },
              statusSourceType: 4,
              statusAudienceMetadata: { audienceType: 1 }
            },
            inviteLinkGroupTypeV2: 0
          }
        }
      }
    }, {});

    await sock.sendMessage(senderJid, { text: `✅ Status teks berhasil dikirim ke grup!` }, { quoted: senderMsg });
    await react(sock, senderJid, senderMsg, "✅");
  } catch (err) {
    log.error("SWGC", `Gagal kirim status teks: ${err.message}`);
    await react(sock, senderJid, senderMsg, "❌");
    await sock.sendMessage(senderJid, { text: `❌ Gagal mengirim status: ${err.message}` }, { quoted: senderMsg });
  }
}

async function handleSwgc(sock, msg, jid, text) {
  const isGroup = jid.endsWith("@g.us");
  await react(sock, jid, msg, "⏳");

  const quotedInfo = getQuotedMedia(msg);
  const customText = text.trim();

  // ── Di dalam grup ────────────────────────────────────────────────────────────
  if (isGroup) {
    if (quotedInfo) {
      const downloaded = await downloadQuotedMedia(sock, msg);
      if (!downloaded) {
        await react(sock, jid, msg, "❌");
        await sock.sendMessage(jid, { text: "Gagal mendownload media. Pastikan kamu reply ke foto atau video." }, { quoted: msg });
        return;
      }

      const { buffer, mimeType, type } = downloaded;
      const isVideo = type === "videoMessage";
      await prepareMediaForStatus(buffer, mimeType);

      try {
        const prepared = await prepareWAMessageMedia(
          isVideo ? { video: buffer, caption: customText||"" } : { image: buffer, caption: customText||"" },
          { upload: sock.waUploadToServer }
        );
        const sharedContextInfo = {
          mentionedJid: [], groupMentions: [],
          statusAttributions: [{ type: 10 }], forwardingScore: 0,
          featureEligibilities: { canBeReshared: true, canReceiveMultiReact: true },
          statusSourceType: 4, statusAudienceMetadata: { audienceType: 1 }
        };
        const finalPayload = isVideo
          ? { videoMessage: { ...prepared.videoMessage, caption: customText||"", contextInfo: sharedContextInfo } }
          : { imageMessage: { ...prepared.imageMessage, caption: customText||"", contextInfo: sharedContextInfo } };

        await sock.relayMessage(jid, {
          messageContextInfo: { messageSecret: "BrRzGQ6/B0ddqBuasejEf+rJKLQ2pauxHtAw1nIMPvw=" },
          groupStatusMessageV2: { message: { ...finalPayload } }
        }, {});
        await react(sock, jid, msg, "✅");
      } catch (err) {
        log.error("SWGC", `Gagal kirim status media: ${err.message}`);
        await react(sock, jid, msg, "❌");
        await sock.sendMessage(jid, { text: `Gagal mengirim status media: ${err.message}` }, { quoted: msg });
      }
      return;
    }

    if (!customText) {
      await react(sock, jid, msg, "?");
      await sock.sendMessage(jid, {
        text:
          `Cara penggunaan .swgc:\n\n` +
          `• Teks  : .swgc hai saya asep\n` +
          `• Media : reply foto/video → .swgc\n` +
          `• Media + teks : reply foto/video → .swgc teks caption kamu`
      }, { quoted: msg });
      return;
    }

    try {
      await sock.relayMessage(jid, {
        messageContextInfo: { messageSecret: "BrRzGQ6/B0ddqBuasejEf+rJKLQ2pauxHtAw1nIMPvw=" },
        groupStatusMessageV2: {
          message: {
            extendedTextMessage: {
              endCardTiles: [], text: customText,
              textArgb: 4294967040, backgroundArgb: 4280669030,
              font: 5, previewType: 0,
              contextInfo: {
                mentionedJid: [], groupMentions: [],
                statusAttributions: [{ type: 10 }], forwardingScore: 0,
                featureEligibilities: { canBeReshared: true, canReceiveMultiReact: true },
                statusSourceType: 4, statusAudienceMetadata: { audienceType: 1 }
              },
              inviteLinkGroupTypeV2: 0
            }
          }
        }
      }, {});
      await react(sock, jid, msg, "✅");
    } catch (err) {
      log.error("SWGC", `Gagal kirim status teks: ${err.message}`);
      await react(sock, jid, msg, "❌");
      await sock.sendMessage(jid, { text: `Gagal mengirim status: ${err.message}` }, { quoted: msg });
    }
    return;
  }

  // ── Di private chat ───────────────────────────────────────────────────────────
  let mediaData = null;
  if (quotedInfo) {
    const downloaded = await downloadQuotedMedia(sock, msg);
    if (!downloaded) {
      await react(sock, jid, msg, "❌");
      await sock.sendMessage(jid, { text: "Gagal mendownload media. Pastikan kamu reply ke foto atau video." }, { quoted: msg });
      return;
    }
    mediaData = downloaded;
  }

  if (!mediaData && !customText) {
    await react(sock, jid, msg, "?");
    await sock.sendMessage(jid, {
      text:
        `Cara penggunaan .swgc di private chat:\n\n` +
        `• Teks  : .swgc hai saya asep\n` +
        `• Media : reply foto/video → .swgc\n` +
        `• Media + teks : reply foto/video → .swgc teks caption kamu\n\n` +
        `Setelah itu pilih grup tujuan dari daftar yang muncul.`
    }, { quoted: msg });
    return;
  }

  await sendGroupList(sock, jid, msg, { text: customText, mediaData });
}

async function handleSwgcGroupSelection(sock, msg, jid) {
  const pending = swgcPendingSessions.get(jid);
  if (!pending) return false;

  if (Date.now() - pending.ts > 5 * 60 * 1000) {
    swgcPendingSessions.delete(jid);
    await sock.sendMessage(jid, { text: "⏰ Sesi pemilihan grup telah kedaluwarsa. Silakan ulangi perintah .swgc." }, { quoted: msg });
    return true;
  }

  const rawText = getMessageText(msg).trim();

  if (rawText === "0" || rawText.toLowerCase() === "batal") {
    swgcPendingSessions.delete(jid);
    await react(sock, jid, msg, "❌");
    await sock.sendMessage(jid, { text: "❌ Pemilihan grup dibatalkan." }, { quoted: msg });
    return true;
  }

  const num = parseInt(rawText, 10);
  if (isNaN(num) || num < 1 || num > pending.groups.length) {
    await sock.sendMessage(jid, {
      text: `⚠️ Masukkan nomor yang valid (1–${pending.groups.length}), atau ketik *0* / *batal* untuk membatalkan.`
    }, { quoted: msg });
    return true;
  }

  const selectedGroup = pending.groups[num - 1];
  swgcPendingSessions.delete(jid);

  log.info("SWGC", `→ ${selectedGroup.name}`);
  await react(sock, jid, msg, "⏳");
  await sock.sendMessage(jid, { text: `📤 Mengirim ke *${selectedGroup.name}*...` }, { quoted: msg });
  await sendSwgcToGroup(sock, selectedGroup.jid, pending.payload, msg, jid);
  return true;
}

// ─── Input-session handlers ────────────────────────────────────────────────────

async function handleStartInputSession(sock, msg, jid, text) {
  const { isPremium, isAntiSteal } = parseFlags(text);
  setSession(jid, {
    active: true, links: [], stickers: [], packName: null, firstUrl: null,
    isPremium, isAntiSteal, createdAt: Date.now()
  });

  const flags = [isPremium && "Premium", isAntiSteal && "Anti-Steal"].filter(Boolean).join(", ");

  await sock.sendMessage(jid, {
    text:
      `Mode Input Aktif${flags ? ` (${flags})` : ""}\n\n` +
      `Masukan url setelah pesan ini, ketik .cancel untuk membatalkan:\n\n` +
      `- Bisa kirim banyak link sekaligus (pisahkan dengan spasi/baris baru)\n` +
      `- Setiap pack berisi maks ${MAX_PACK_STICKERS} sticker\n` +
      `- Ketik .status untuk cek progress\n` +
      `- Ketik .done untuk mengirim pack sekarang`
  }, { quoted: msg });
}

async function handleCancelCmd(sock, msg, jid) {
  const session = getSession(jid);
  if (!session || !session.active) { await react(sock, jid, msg, "?"); return; }
  deleteSession(jid);
  await sock.sendMessage(jid, { text: "Sesi input dibatalkan." }, { quoted: msg });
}

async function handleStatusCmd(sock, msg, jid) {
  const session = getSession(jid);
  if (!session || !session.active) {
    await sock.sendMessage(jid, { text: "Tidak ada sesi input yang aktif.\n\nGunakan .sly --input untuk memulai." }, { quoted: msg });
    return;
  }

  const flags        = [session.isPremium && "Premium", session.isAntiSteal && "Anti-Steal"].filter(Boolean).join(", ");
  const totalStickers = session.stickers.length;
  const totalPacks    = Math.max(1, Math.ceil(totalStickers / MAX_PACK_STICKERS));

  let text = `Status Input Sticker${flags ? ` (${flags})` : ""}\n\n`;
  text += `Link terkumpul    : ${session.links.length}\n`;
  text += `Sticker terkumpul : ${totalStickers}\n`;
  text += `Pack yang akan dikirim : ${totalPacks} pack (maks ${MAX_PACK_STICKERS}/pack)\n\n`;
  if (session.links.length) {
    text += `Daftar link:\n`;
    session.links.forEach((l, i) => { text += `${i+1}. ${l.url} (${l.count} sticker)\n`; });
    text += `\n`;
  }
  text += `Kirim link lain, .status untuk cek progress, .done untuk mengirim, atau .cancel untuk batal.`;

  await sock.sendMessage(jid, { text }, { quoted: msg });
}

async function handleLinkInput(sock, msg, jid, msgText) {
  const session = getSession(jid);
  if (!session || !session.active) return;

  const urls = extractUrls(msgText);
  if (!urls.length) return;

  const sly = new StickerLy();
  await react(sock, jid, msg, "⏳");

  let addedTotal = 0;
  const failedUrls = [];

  for (const url of urls) {
    try {
      const detail = await sly.detail(url);
      if (!session.packName)  session.packName  = detail.name;
      if (!session.firstUrl)  session.firstUrl  = detail.url;
      session.stickers.push(...detail.stickers);
      session.links.push({ url: detail.url, name: detail.name, count: detail.stickers.length });
      addedTotal += detail.stickers.length;
    } catch (err) {
      log.error("INPUT", `${url}: ${err.message}`);
      failedUrls.push(url);
    }
  }

  setSession(jid, session);

  const totalStickers = session.stickers.length;
  const totalPacks    = Math.max(1, Math.ceil(totalStickers / MAX_PACK_STICKERS));

  let replyText = `+${addedTotal} sticker ditambahkan. Total: ${totalStickers} dari ${session.links.length} link → ${totalPacks} pack.\n`;
  if (failedUrls.length) {
    replyText += `\nGagal (${failedUrls.length}):\n` + failedUrls.map(u => `- ${u}`).join("\n") + "\n";
  }
  replyText += `\n.done untuk kirim | .status untuk info | .cancel untuk batal`;

  await react(sock, jid, msg, "✅");
  await sock.sendMessage(jid, { text: replyText }, { quoted: msg });
}

async function handleDoneCmd(sock, msg, jid) {
  const session = getSession(jid);
  if (!session || !session.active) {
    await sock.sendMessage(jid, { text: "Tidak ada sesi input yang aktif." }, { quoted: msg });
    return;
  }
  if (!session.stickers.length) {
    await sock.sendMessage(jid, { text: "Belum ada sticker yang terkumpul. Kirim link terlebih dahulu." }, { quoted: msg });
    return;
  }

  await react(sock, jid, msg, "⏳");

  const stickerChunks = chunkArray(session.stickers, MAX_PACK_STICKERS);
  const totalPacks    = stickerChunks.length;

  log.info("DONE", `${session.stickers.length} stickers → ${totalPacks} pack`);

  if (totalPacks > 1) {
    await sock.sendMessage(jid, {
      text: `Memproses ${session.stickers.length} sticker → ${totalPacks} pack...`
    }, { quoted: msg });
  }

  let successPacks = 0, failedPacks = 0;

  for (let i = 0; i < stickerChunks.length; i++) {
    const chunk     = stickerChunks[i];
    const packLabel = totalPacks > 1 ? ` (${i+1}/${totalPacks})` : "";
    const combinedDetail = {
      name: `${session.packName || "Custom Pack"}${packLabel}`,
      stickers: chunk, trayIndex: 0, url: session.firstUrl || ""
    };

    try {
      await sendStickerPack(sock, jid, combinedDetail, msg, session.isPremium, session.isAntiSteal);
      successPacks++;
      if (totalPacks > 1)
        await sock.sendMessage(jid, { text: `Pack ${i+1}/${totalPacks} selesai (${chunk.length} sticker).` });
    } catch (err) {
      log.error("DONE", `Pack ${i+1} failed: ${err.message}`);
      failedPacks++;
      await sock.sendMessage(jid, { text: `Pack ${i+1}/${totalPacks} gagal: ${err.message}` });
    }
  }

  deleteSession(jid);

  if (totalPacks > 1) {
    const summary = failedPacks === 0
      ? `Semua ${totalPacks} pack berhasil dikirim! (${session.stickers.length} sticker)`
      : `Selesai: ${successPacks}/${totalPacks} pack berhasil, ${failedPacks} gagal.`;
    await sock.sendMessage(jid, { text: summary }, { quoted: msg });
  }

  await react(sock, jid, msg, successPacks > 0 ? "✅" : "❌");
}

async function handleSetPackName(sock, msg, jid, raw) {
  const value = raw.trim();
  if (!value) {
    await sock.sendMessage(jid, {
      text: `Format: .setpackname <nama pack>\n\nPack name saat ini:\n${getPackName()}`
    }, { quoted: msg });
    return;
  }
  saveSettings({ packName: value });
  await sock.sendMessage(jid, { text: `Pack name diubah menjadi:\n${value}` }, { quoted: msg });
}

async function handleSetAuthor(sock, msg, jid, raw) {
  const value = raw.trim();
  if (!value) {
    await sock.sendMessage(jid, {
      text: `Format: .setauthor <nama author>\n\nAuthor saat ini:\n${getAuthor()}`
    }, { quoted: msg });
    return;
  }
  saveSettings({ author: value });
  await sock.sendMessage(jid, { text: `Author diubah menjadi:\n${value}` }, { quoted: msg });
}

async function handleSetWm(sock, msg, jid, raw) {
  const value = raw.trim();
  if (!value || !value.includes("|")) {
    await sock.sendMessage(jid, {
      text:
        `Format: .setwm packname|author\n\n` +
        `Saat ini:\nPack name: ${getPackName()}\nAuthor: ${getAuthor()}`
    }, { quoted: msg });
    return;
  }
  const sepIndex = value.indexOf("|");
  const packname = value.slice(0, sepIndex).trim();
  const author   = value.slice(sepIndex + 1).trim();
  if (!packname || !author) {
    await sock.sendMessage(jid, { text: "Pack name dan author tidak boleh kosong." }, { quoted: msg });
    return;
  }
  saveSettings({ packName: packname, author });
  await sock.sendMessage(jid, { text: `Watermark diubah:\nPack name: ${packname}\nAuthor: ${author}` }, { quoted: msg });
}

async function handleStickerLy(sock, msg, text, jid) {
  const sly = new StickerLy();
  if (!text) { await react(sock, jid, msg, "?"); return; }

  const lowerText = text.toLowerCase();
  if (lowerText.startsWith("--input") || lowerText === "input") {
    await handleStartInputSession(sock, msg, jid, text);
    return;
  }

  const urlMatch = text.match(/https?:\/\/[^\s]+/);
  if (!urlMatch) { await react(sock, jid, msg, "?"); return; }

  const { isPremium, isAntiSteal } = parseFlags(text);
  try {
    const detail = await sly.detail(urlMatch[0]);
    await sendStickerPack(sock, jid, detail, msg, isPremium, isAntiSteal);
  } catch (err) {
    log.error("SLY", err.message);
    await sock.sendMessage(jid, { text: `Error: ${err.message}` }, { quoted: msg });
  }
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────
(async function start() {
  function question(text) {
    return new Promise((resolve) => {
      const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
      rl.question(`${text} `, (a) => { rl.close(); resolve(a.trim()); });
    });
  }

  const session = await useMultiFileAuthState("session");

  const sock = makeWASocket({
    printQRInTerminal: false,
    markOnlineOnConnect: false,
    auth: session.state,
    logger: pino({ level: "silent" }),
    retryRequestDelayMs: 250,
    maxMsgRetryCount: 3,
    connectTimeoutMs: 20000,
    defaultQueryTimeoutMs: 20000,
    keepAliveIntervalMs: 30000,
    getMessage: async () => undefined
  });

  if (!sock.authState.creds.registered) {
    const waNumber = await question("WhatsApp number (without +):");
    const code = await sock.requestPairingCode(waNumber.replace(/\D/g, ""));
    log.ok("AUTH", `Pairing Code: ${code}`);
  }

  sock.ev.on("connection.update", async ({ connection, lastDisconnect }) => {
    if (connection === "close") {
      const { statusCode, error } = lastDisconnect?.error?.output?.payload || {};
      if (statusCode === 401 && error === "Unauthorized")
        await fs.promises.rm("session", { recursive: true, force: true });
      log.warn("CONN", "Reconnecting...");
      bufferPool.clear();
      if (global.gc) global.gc();
      return start();
    }
    if (connection === "open") {
      const used = process.memoryUsage();
      log.ok("CONN",
        `Connected: ${sock.user.id.split(":")[0]} | ` +
        `RSS: ${(used.rss/1024/1024).toFixed(0)}MB | ` +
        `Heap: ${(used.heapUsed/1024/1024).toFixed(0)}/${(used.heapTotal/1024/1024).toFixed(0)}MB`
      );
    }
  });

  sock.ev.on("creds.update", session.saveCreds);

  sock.ev.on("messages.upsert", async ({ messages, type }) => {
    if (type !== "notify") return;

    for (const msg of messages) {
      if (!msg.key.fromMe || !msg.message) continue;

      const jid     = msg.key.remoteJid;
      const msgText = getMessageText(msg);
      const isGroup = jid.endsWith("@g.us");

      if (!isGroup) {
        const handled = await handleSwgcGroupSelection(sock, msg, jid).catch(err => {
          log.error("SWGC", err.message); return false;
        });
        if (handled) continue;
      }

      const parsed        = parseMessage(msgText);
      const activeSession = getSession(jid);

      if (!parsed) {
        if (activeSession?.active) {
          try { await handleLinkInput(sock, msg, jid, msgText); }
          catch (err) { log.error("INPUT", err.message); }
        }
        continue;
      }

      const { command, text } = parsed;

      if (command === "status") { try { await handleStatusCmd(sock, msg, jid);  } catch (e) { log.error("CMD", e.message); } continue; }
      if (command === "cancel") { try { await handleCancelCmd(sock, msg, jid);  } catch (e) { log.error("CMD", e.message); } continue; }
      if (command === "done")   { try { await handleDoneCmd(sock, msg, jid);    } catch (e) { log.error("CMD", e.message); } continue; }

      if (command === "setpackname") {
        const raw = getRawArgs(msgText, parsed.prefix, command);
        try { await handleSetPackName(sock, msg, jid, raw); } catch (e) { log.error("CMD", e.message); }
        continue;
      }
      if (command === "setauthor") {
        const raw = getRawArgs(msgText, parsed.prefix, command);
        try { await handleSetAuthor(sock, msg, jid, raw); } catch (e) { log.error("CMD", e.message); }
        continue;
      }
      if (command === "setwm") {
        const raw = getRawArgs(msgText, parsed.prefix, command);
        try { await handleSetWm(sock, msg, jid, raw); } catch (e) { log.error("CMD", e.message); }
        continue;
      }

      if (command === "swgc") {
        try { await handleSwgc(sock, msg, jid, text); }
        catch (err) { log.error("SWGC", err.message); await react(sock, jid, msg, "❌"); }
        continue;
      }

      if (!COMMANDS.includes(command)) continue;

      log.info("CMD", `${command} | ${jid.split("@")[0]} | "${text}"`);
      try { await handleStickerLy(sock, msg, text, jid); }
      catch (err) { log.error("SLY", err.message); }
    }
  });

  if (global.gc) {
    setInterval(() => {
      bufferPool.clear();
      global.gc();
      const used = process.memoryUsage();
      log.mem("GC",
        `RSS: ${(used.rss/1024/1024).toFixed(0)}MB | ` +
        `Heap: ${(used.heapUsed/1024/1024).toFixed(0)}/${(used.heapTotal/1024/1024).toFixed(0)}MB`
      );
    }, 5 * 60 * 1000);
  }
})();
