import 'dotenv/config';
import express, { Request, Response } from 'express';
import multer, { MulterError } from 'multer';
import sqlite3 from 'sqlite3';
import { open, Database } from 'sqlite';
import path from 'path';
import fs from 'fs';
import { z } from 'zod';
import { Redis } from './redis';
import { uuid } from './uuid';
// -----------------------------
// Env Validation
// -----------------------------

const requiredEnvVars = ['PORT', 'AUTH_REDIS_NAME'] as const;

for (const varName of requiredEnvVars) {
  if (!process.env[varName]) {
    throw new Error(`Missing required environment variable: ${varName}`);
  }
}

// -----------------------------
// Config
// -----------------------------

const app = express();
const PORT = Number(process.env.PORT || 3001);
const UPLOAD_DIR = path.resolve(process.cwd(), 'uploads');

let db: Database<sqlite3.Database, sqlite3.Statement>;

// -----------------------------
// Ensure Upload Directory
// -----------------------------

if (!fs.existsSync(UPLOAD_DIR)) {
  fs.mkdirSync(UPLOAD_DIR);
}

// -----------------------------
// Multer Setup
// -----------------------------

const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, UPLOAD_DIR),
  filename: (_req, file, cb) => {
    const ext = path.extname(file.originalname);
    cb(null, `${uuid()}${ext}`);
  }
});

const upload = multer({
  storage,
  limits: { fileSize: 5 * 1024 * 1024 } // 5MB
}).single('file');

// -----------------------------
// DB Init
// -----------------------------

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

// -----------------------------
// Upload Endpoint
// -----------------------------

app.post('/upload', (req: Request, res: Response) => {
  upload(req, res, async (err: any) => {
    if (err instanceof MulterError) {
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

// -----------------------------
// Download Endpoint
// -----------------------------

app.get('/download/:id', async (req: Request, res: Response) => {
  const sessionId = req.header('x-session-id');
  if (!sessionId) {
    return res.status(401).json({ error: 'Missing x-session-id header' });
  }

  const fileId = req.params.id;
  let allowed = false;

  try {
    const result = await Redis.query(
      process.env.AUTH_REDIS_NAME!,
      'check-file-access',
      {
        sessionId,
        fileId
      },
      z.object({ allowed: z.boolean() }),
      1000
    ).unwrap();
    if (result.allowed) {
      allowed = true;
    }
  } catch (err: any) {
    console.error('Access check failed:', err.message);
  }

  if (!allowed) {
    return res.status(403).json({ error: 'Access denied' });
  }

  const file = await db.get('SELECT * FROM uploads WHERE id = ?', fileId);
  if (!file) {
    return res.status(404).json({ error: 'File not found' });
  }

  res.sendFile(path.resolve(file.path));
});

// -----------------------------
// Metadata Endpoint
// -----------------------------

app.get('/metadata/:id', async (req: Request, res: Response) => {
  const file = await db.get(
    'SELECT id, original_name, mimetype, size, created_at FROM uploads WHERE id = ?',
    req.params.id
  );

  if (!file) return res.status(404).json({ error: 'Not found' });
  res.json(file);
});

// -----------------------------
// Start Server
// -----------------------------

const start = async () => {
  await initDb();
  app.listen(PORT, () => {
    console.log(`[FileService] Running at http://localhost:${PORT}`);
  });
};

start().catch(console.error);
