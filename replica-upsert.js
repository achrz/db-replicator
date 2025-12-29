require('dotenv').config();
const mysql = require('mysql2/promise');

const config = {
    source: {
        host: process.env.SOURCE_DB_HOST,
        port: process.env.SOURCE_DB_PORT || 3306,
        user: process.env.SOURCE_DB_USER,
        password: process.env.SOURCE_DB_PASSWORD,
        database: process.env.SOURCE_DB_NAME,
        connectTimeout: 60000,
        dateStrings: true,
        compress: true
    },
    dest: {
        host: process.env.DEST_DB_HOST,
        port: process.env.DEST_DB_PORT,
        user: process.env.DEST_DB_USER,
        password: process.env.DEST_DB_PASSWORD,
        database: process.env.DEST_DB_NAME,
        multipleStatements: true,
        dateStrings: true
    }
};

async function runUpsert() {
    // console.time('Total Waktu Sync'); // Optional: matikan timer biar log cron bersih
    let sourcePool, destPool;

    try {
        sourcePool = mysql.createPool(config.source);
        destPool = mysql.createPool(config.dest);

        // HANYA ambil tabel fisik (Base Table)
        const [tablesRaw] = await sourcePool.query("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'");
        const tablesToSync = tablesRaw.map(row => Object.values(row)[0]);

        // Matikan FK check & Strict Mode (PENTING BUAT TABEL GEMOY)
        await destPool.query('SET FOREIGN_KEY_CHECKS=0');
        await destPool.query(`SET SESSION innodb_strict_mode=0`);

        for (const tableName of tablesToSync) {
            try {
                // ======================================================
                // 1. CEK & CREATE TABEL (LOGIC ANTI-OBESITAS & AUTO INNODB)
                // ======================================================
                const [checkTable] = await destPool.query(`SHOW TABLES LIKE ?`, [tableName]);

                if (checkTable.length === 0) {
                    // console.log(`[${tableName}] Tabel baru terdeteksi. Membuat struktur...`);
                    const [createSyntax] = await sourcePool.query(`SHOW CREATE TABLE ??`, [tableName]);
                    let sqlCreate = createSyntax[0]['Create Table'];

                    // --- BERSIH-BERSIH SQL (Sama kayak init-sync) ---
                    const dbName = process.env.SOURCE_DB_NAME;
                    sqlCreate = sqlCreate.split(`\`${dbName}\`.`).join('');
                    sqlCreate = sqlCreate.split(`${dbName}.`).join('');
                    sqlCreate = sqlCreate.replace(/`[^`]+`\./g, ''); 

                    // Paksa InnoDB & Dynamic Row
                    sqlCreate = sqlCreate.replace(/ENGINE=\w+/gi, 'ENGINE=InnoDB');
                    sqlCreate = sqlCreate.replace(/ROW_FORMAT=\w+/gi, '');
                    sqlCreate = sqlCreate.replace('ENGINE=InnoDB', 'ENGINE=InnoDB ROW_FORMAT=DYNAMIC');

                    // Bersihkan Charset
                    sqlCreate = sqlCreate.replace(/utf8mb4_uca1400_ai_ci/gi, 'utf8mb4_unicode_ci');
                    sqlCreate = sqlCreate.replace(/latin1_swedish_ci/gi, 'utf8mb4_unicode_ci');
                    sqlCreate = sqlCreate.replace(/latin1/gi, 'utf8mb4');
                    sqlCreate = sqlCreate.replace(/DEFAULT CHARSET=\w+/gi, 'DEFAULT CHARSET=utf8mb4');
                    sqlCreate = sqlCreate.replace(/COLLATE=\w+/gi, 'COLLATE=utf8mb4_unicode_ci');

                    await destPool.query(sqlCreate);
                    
                    // Paksa Index Updated_at
                    try { await destPool.query(`CREATE INDEX idx_updated_at ON \`${tableName}\` (updated_at)`); } catch (e) {}
                }

                // ======================================================
                // 2. LOGIC SMART CURSOR (TIE-BREAKER)
                // ======================================================
                
                // Cek PK & Updated_at
                const [keys] = await sourcePool.query(`SHOW KEYS FROM ?? WHERE Key_name = 'PRIMARY'`, [tableName]);
                const [cols] = await destPool.query(`SHOW COLUMNS FROM ?? LIKE 'updated_at'`, [tableName]);

                // Kalau gak punya syarat (PK/Updated_at), skip atau fallback ke mode bodoh
                if (keys.length === 0 || cols.length === 0) {
                     // Fallback: Ambil 100 data teratas (Risiko: Data baru yg masuk dibawah gak keambil)
                     // console.log(`[${tableName}] Skip Smart Logic (No PK/Updated_at)`);
                     continue; 
                }

                const pk = keys[0].Column_name;

                // Ambil Posisi Terakhir di Server Fisik
                const [rows] = await destPool.query(
                    `SELECT updated_at, ?? as last_id FROM ?? ORDER BY updated_at DESC, ?? DESC LIMIT 1`, 
                    [pk, tableName, pk]
                );

                let lastSync = new Date(0); 
                let lastId = 0;

                if (rows.length > 0 && rows[0].updated_at) {
                    lastSync = rows[0].updated_at;
                    lastId = rows[0].last_id;
                }

                // ======================================================
                // 3. TARIK DATA (LIMIT 3000 CUKUP UNTUK CRON 1 MENIT)
                // ======================================================
                // Limit 10.000 terlalu besar untuk cron 1 menit, risiko timeout. 
                // 3.000 lebih aman. Karena script ini jalan tiap menit, dia bakal kejar tayang kok.
                
                const query = `
                    SELECT * FROM ?? 
                    WHERE (updated_at > ?) 
                       OR (updated_at = ? AND ?? > ?) 
                    ORDER BY updated_at ASC, ?? ASC 
                    LIMIT 3000
                `;

                const [changes] = await sourcePool.query(query, [
                    tableName, lastSync, lastSync, pk, lastId, pk
                ]);

                if (changes.length > 0) {
                    const columns = Object.keys(changes[0]);
                    const values = changes.map(row => Object.values(row));
                    const placeholders = changes.map(() => `(${columns.map(() => '?').join(', ')})`).join(', ');
                    const flatValues = values.flat();

                    const escapedColumns = columns.map(col => `\`${col}\``).join(', ');
                    const updateOnDuplicate = columns.map(f => `\`${f}\` = VALUES(\`${f}\`)`).join(', ');

                    const sql = `INSERT INTO ?? (${escapedColumns}) VALUES ${placeholders} 
                                 ON DUPLICATE KEY UPDATE ${updateOnDuplicate}`;

                    await destPool.query(sql, [tableName, ...flatValues]);
                    console.log(`[${tableName}] ⚡ Synced ${changes.length} rows.`);
                } 
                // else { console.log(`[${tableName}] 👌 Up-to-date.`); } // Silent aja biar log cron gak penuh

            } catch (loopErr) {
                console.error(`[${tableName}] ❌ Error: ${loopErr.message}`);
            }
        }

    } catch (err) {
        console.error('Global Error:', err.message);
    } finally {
        if (sourcePool) await sourcePool.end();
        if (destPool) await destPool.end();
        // console.timeEnd('Total Waktu Sync');
    }
}

runUpsert();