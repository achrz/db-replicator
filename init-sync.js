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

        console.log(`🚀 MEMULAI INITIAL SYNC (MODE SMART CURSOR)...`);

        // Ambil Daftar Tabel
        const [tablesRaw] = await sourcePool.query("SHOW TABLES");
        const tablesToSync = tablesRaw.map(row => Object.values(row)[0]);
        
        await destPool.query('SET FOREIGN_KEY_CHECKS=0');

        for (const tableName of tablesToSync) {
            console.log(`\n🔄 Memproses Tabel: [ ${tableName} ]`);
            
            // 1. Cek Primary Key (WAJIB ADA buat Tie-Breaker)
            const [keys] = await sourcePool.query(`SHOW KEYS FROM ?? WHERE Key_name = 'PRIMARY'`, [tableName]);
            if (keys.length === 0) {
                console.log(`   ⚠️ SKIP: Tabel tidak punya Primary Key.`);
                continue;
            }
            const pk = keys[0].Column_name; // Biasanya 'id'

            // 2. Cek Updated_at
            const [cols] = await destPool.query(`SHOW COLUMNS FROM ?? LIKE 'updated_at'`, [tableName]);
            if (cols.length === 0) {
                console.log(`   ⚠️ SKIP: Tidak ada updated_at.`);
                continue;
            }

            // 3. LOOPING CERDAS
            let totalSyncedForTable = 0;
            let isFinished = false;

            while (!isFinished) {
                // A. Cek Posisi Terakhir di Server Fisik
                // Kita ambil updated_at TERAKHIR dan ID TERAKHIR pada waktu tersebut
                const [rows] = await destPool.query(
                    `SELECT updated_at, ?? as last_id FROM ?? ORDER BY updated_at DESC, ?? DESC LIMIT 1`, 
                    [pk, tableName, pk]
                );

                let lastSync = new Date(0); // Default tahun 1970
                let lastId = 0;

                if (rows.length > 0 && rows[0].updated_at) {
                    lastSync = rows[0].updated_at;
                    lastId = rows[0].last_id;
                }

                // B. Query "Tie-Breaker"
                // Logika: (Waktu > WaktuTerakhir) ATAU (Waktu == WaktuTerakhir DAN ID > IDTerakhir)
                const query = `
                    SELECT * FROM ?? 
                    WHERE (updated_at > ?) 
                       OR (updated_at = ? AND ?? > ?) 
                    ORDER BY updated_at ASC, ?? ASC 
                    LIMIT 3000
                `;

                const [changes] = await sourcePool.query(query, [
                    tableName, 
                    lastSync,         // Param 1: updated_at > ...
                    lastSync,         // Param 2: updated_at = ...
                    pk, lastId,       // Param 3: id > ...
                    pk                // Param 4: Order by id ASC
                ]);

                if (changes.length === 0) {
                    isFinished = true;
                    console.log(`   ✅ Tabel ${tableName} SELESAI (Up to date).`);
                } else {
                    // C. Insert Data
                    const columns = Object.keys(changes[0]);
                    const values = changes.map(row => Object.values(row));
                    const placeholders = changes.map(() => `(${columns.map(() => '?').join(', ')})`).join(', ');
                    const flatValues = values.flat();

                    const updateOnDuplicate = columns.map(f => `${f} = VALUES(${f})`).join(', ');
                    const sql = `INSERT INTO ?? (${columns.join(', ')}) VALUES ${placeholders} 
                                 ON DUPLICATE KEY UPDATE ${updateOnDuplicate}`;

                    await destPool.query(sql, [tableName, ...flatValues]);
                    
                    totalSyncedForTable += changes.length;
                    process.stdout.write(`\r   ⚡ Progress: ${totalSyncedForTable} baris...`);
                    
                    await sleep(200); // Jeda dikit

                    if (changes.length < 3000) {
                        isFinished = true;
                        console.log(`\n   ✅ Tabel ${tableName} SELESAI.`);
                    }
                }
            }
        }

    } catch (err) {
        console.error('\n❌ Error:', err.message);
    } finally {
        if (sourcePool) await sourcePool.end();
        if (destPool) await destPool.end();
        console.timeEnd('Total Waktu Initial Sync');
    }
}

runInitialSync();