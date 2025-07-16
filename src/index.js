require('dotenv').config();
const express = require('express');
const multer = require('multer');
const sqlite3 = require('sqlite3').verbose();
const { open } = require('sqlite');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const fs = require('fs');
const { z } = require('zod');
const { query } = require('./redis-query');


const requiredEnvVars = ['PORT', 'AUTH_REDIS_NAME'];

for (const varName of requiredEnvVars) {
  if (!process.env[varName]) {
    throw new Error(`Missing required environment variable: ${varName}`);
  }
}


const app = express();
const PORT = process.env.PORT || 3001;
const UPLOAD_DIR = path.resolve(process.cwd(), 'uploads');

let db;

// Ensure upload directory exists
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR);

// Multer setup
const storage = multer.diskStorage({
  destination: (_, __, cb) => cb(null, UPLOAD_DIR),
  filename: (_, file, cb) => {
    const ext = path.extname(file.originalname);
    cb(null, `${uuidv4()}${ext}`);
  }
});

const upload = multer({
  storage,
  limits: {
    fileSize: 5 * 1024 * 1024 // 5MB
  }
}).single('file');

// Initialize DB
const initDb = async () => {
  db = await open({ filename: './files.db', driver: sqlite3.Database });
  await db.exec(`
    CREATE TABLE IF NOT EXISTS uploads (
      id TEXT PRIMARY KEY,
      original_name TEXT,
      saved_name TEXT,
      mimetype TEXT,
      size INTEGER,
      path TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);
};

// Upload endpoint
app.post('/upload', (req, res) => {
  upload(req, res, async (err) => {
    if (err instanceof multer.MulterError) {
      return res.status(413).json({ error: 'File too large (max 5MB)' });
    } else if (err) {
      return res.status(500).json({ error: 'Error uploading file' });
    }

    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    const file = req.file;
    const id = path.basename(file.filename, path.extname(file.filename));

    await db.run(
      `INSERT INTO uploads (id, original_name, saved_name, mimetype, size, path)
       VALUES (?, ?, ?, ?, ?, ?)`,
      id,
      file.originalname,
      file.filename,
      file.mimetype,
      file.size,
      file.path
    );

    res.status(201).json({
      id,
      originalName: file.originalname,
      size: file.size,
      type: file.mimetype,
      url: `/download/${id}`
    });
  });
});

// Download endpoint w/ Redis access check
app.get('/download/:id', async (req, res) => {
  const sessionId = req.header('x-session-id');
  if (!sessionId) {
    return res.status(401).json({ error: 'Missing x-session-id header' });
  }

  const fileId = req.params.id;

  let allowed = false;
  try {
    const result = await query(
        process.env.AUTH_REDIS_NAME,
        'check-file-access',
        {
      sessionId,
      fileId
    }, z.boolean(), 1000);
    allowed = result?.allowed === true;
  } catch (err) {
    console.error('Access check failed:', err.message);
  }

  if (!allowed) {
    return res.status(403).json({ error: 'Access denied' });
  }

  const file = await db.get('SELECT * FROM uploads WHERE id = ?', fileId);
  if (!file) return res.status(404).json({ error: 'File not found' });

  res.sendFile(file.path);
});

// Optional: List all files
// app.get('/files', async (_, res) => {
//   const rows = await db.all('SELECT id, original_name, size, created_at FROM uploads ORDER BY created_at DESC');
//   res.json(rows);
// });

// Optional: Get metadata for a single file
app.get('/metadata/:id', async (req, res) => {
  const file = await db.get('SELECT id, original_name, mimetype, size, created_at FROM uploads WHERE id = ?', req.params.id);
  if (!file) return res.status(404).json({ error: 'Not found' });
  res.json(file);
});

// Start server
const start = async () => {
  await initDb();
  app.listen(PORT, () => {
    console.log(`[FileService] Running at http://localhost:${PORT}`);
  });
};

start().catch(console.error);
