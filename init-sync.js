require('dotenv').config();
const mysql = require('mysql2/promise');

const config = {
    source: {
        host: process.env.SOURCE_DB_HOST,
        user: process.env.SOURCE_DB_USER,
        password: process.env.SOURCE_DB_PASSWORD,
        database: process.env.SOURCE_DB_NAME,
        connectTimeout: 60000 
    },
    dest: {
        host: process.env.DEST_DB_HOST,
        user: process.env.DEST_DB_USER,
        password: process.env.DEST_DB_PASSWORD,
        database: process.env.DEST_DB_NAME,
        multipleStatements: true 
    }
};

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function runInitialSync() {
    console.time('Total Waktu Initial Sync');
    let sourcePool, destPool;

    try {
        sourcePool = mysql.createPool(config.source);
        destPool = mysql.createPool(config.dest);

        console.log(`🚀 MEMULAI INITIAL SYNC (AUTO FIX PREFIX)...`);

        const [tablesRaw] = await sourcePool.query("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'");
        const tablesToSync = tablesRaw.map(row => Object.values(row)[0]);
        
        await destPool.query('SET FOREIGN_KEY_CHECKS=0');

        for (const tableName of tablesToSync) {
            console.log(`\n🔄 Memproses Tabel: [ ${tableName} ]`);
            
            // 1. Cek & Create Tabel
            const [checkTable] = await destPool.query(`SHOW TABLES LIKE ?`, [tableName]);
            
            if (checkTable.length === 0) {
                console.log(`   🏗️  Membuat Tabel Baru (Convert ke InnoDB)...`);
                const [createSyntax] = await sourcePool.query(`SHOW CREATE TABLE ??`, [tableName]);
                let sqlCreate = createSyntax[0]['Create Table'];

                // --- BAGIAN PERBAIKAN (REGEX LEBIH AGRESIF) ---
                
                // 1. Hapus nama database SOURCE secara spesifik (paling aman)
                const dbName = process.env.SOURCE_DB_NAME;
                // Hapus `nama_db`.
                sqlCreate = sqlCreate.split(`\`${dbName}\`.`).join('');
                // Hapus nama_db. (tanpa backtick)
                sqlCreate = sqlCreate.split(`${dbName}.`).join('');
                
                // 2. Hapus sisa-sisa prefix db lain (generic)
                sqlCreate = sqlCreate.replace(/`[^`]+`\./g, ''); 

                // 3. Paksa ganti ENGINE ke InnoDB
                sqlCreate = sqlCreate.replace(/ENGINE=\w+/gi, 'ENGINE=InnoDB');
                
                // 4. Bersihkan Charset Latin1 (Biar gak error Collation)
                sqlCreate = sqlCreate.replace(/latin1_swedish_ci/gi, 'utf8mb4_unicode_ci');
                sqlCreate = sqlCreate.replace(/latin1/gi, 'utf8mb4');
                sqlCreate = sqlCreate.replace(/DEFAULT CHARSET=\w+/gi, 'DEFAULT CHARSET=utf8mb4');
                sqlCreate = sqlCreate.replace(/COLLATE=\w+/gi, 'COLLATE=utf8mb4_unicode_ci');
                sqlCreate = sqlCreate.replace(/ROW_FORMAT=\w+/gi, '');

                // -----------------------------------------------

                // EKSEKUSI DENGAN DEBUGGER
                try {
                    await destPool.query(sqlCreate);
                    console.log(`   ✅ Tabel dibuat dengan engine InnoDB.`);
                    
                    // Tambah Index Updated_at
                    try {
                        await destPool.query(`CREATE INDEX idx_updated_at ON \`${tableName}\` (updated_at)`);
                    } catch (e) {}

                } catch (createErr) {
                    console.error(`   ❌ GAGAL MEMBUAT TABEL!`);
                    console.error(`   Pesan Error: ${createErr.message}`);
                    console.log(`   👇 SQL YANG BERMASALAH (Cek bagian CREATE TABLE):`);
                    console.log(sqlCreate); // <--- INI AKAN MUNCUL DI LAYAR KALAU ERROR
                    process.exit(1); // Stop biar kita bisa baca errornya
                }
            }

            // ... (Kode selanjutnya sama seperti sebelumnya) ...
            // 2. Cek PK
            const [keys] = await sourcePool.query(`SHOW KEYS FROM ?? WHERE Key_name = 'PRIMARY'`, [tableName]);
            if (keys.length === 0) { console.log(`   ⚠️ SKIP: No PK.`); continue; }
            const pk = keys[0].Column_name;

            // 3. Cek Updated_at
            const [cols] = await destPool.query(`SHOW COLUMNS FROM ?? LIKE 'updated_at'`, [tableName]);
            if (cols.length === 0) { console.log(`   ⚠️ SKIP: No updated_at.`); continue; }

            // 4. Looping Sync
            let totalSyncedForTable = 0;
            let isFinished = false;

            while (!isFinished) {
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

                const query = `SELECT * FROM ?? WHERE (updated_at > ?) OR (updated_at = ? AND ?? > ?) ORDER BY updated_at ASC, ?? ASC LIMIT 3000`;
                const [changes] = await sourcePool.query(query, [tableName, lastSync, lastSync, pk, lastId, pk]);

                if (changes.length === 0) {
                    isFinished = true;
                    console.log(`   ✅ Tabel ${tableName} SELESAI.`);
                } else {
                    const columns = Object.keys(changes[0]);
                    const values = changes.map(row => Object.values(row));
                    const placeholders = changes.map(() => `(${columns.map(() => '?').join(', ')})`).join(', ');
                    const flatValues = values.flat();

                    const escapedColumns = columns.map(col => `\`${col}\``).join(', ');
                    const updateOnDuplicate = columns.map(f => `\`${f}\` = VALUES(\`${f}\`)`).join(', ');

                    const sql = `INSERT INTO ?? (${escapedColumns}) VALUES ${placeholders} ON DUPLICATE KEY UPDATE ${updateOnDuplicate}`;
                    await destPool.query(sql, [tableName, ...flatValues]);
                    
                    totalSyncedForTable += changes.length;
                    process.stdout.write(`\r   ⚡ Progress: ${totalSyncedForTable} baris...`);
                    await sleep(200);
                    if (changes.length < 3000) { isFinished = true; console.log(`\n   ✅ Tabel ${tableName} SELESAI.`); }
                }
            }
            await destPool.query(`ANALYZE TABLE \`${tableName}\``);
        }

    } catch (err) {
        console.error('\n❌ Global Error:', err.message);
    } finally {
        if (sourcePool) await sourcePool.end();
        if (destPool) await destPool.end();
        console.timeEnd('Total Waktu Initial Sync');
    }
}

runInitialSync();