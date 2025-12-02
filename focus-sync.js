require('dotenv').config();
const mysql = require('mysql2/promise');

// ==========================================
// 🎯 KONFIGURASI TABEL TARGET
// ==========================================
// Tulis nama tabel yang mau di-resync di sini.
// Script hanya akan memproses tabel ini saja.
const TARGET_TABLES = [
    // 'tb_culling',
    // 'tb_feedtime', 
    'tb_konsumsi',
];
// ==========================================

const config = {
    source: {
        host: process.env.SOURCE_DB_HOST,
        port: process.env.SOURCE_DB_PORT || 3306,
        user: process.env.SOURCE_DB_USER,
        password: process.env.SOURCE_DB_PASSWORD,
        database: process.env.SOURCE_DB_NAME,
        connectTimeout: 60000,
        compress: true 
    },
    dest: {
        host: process.env.DEST_DB_HOST,
        port: process.env.DEST_DB_PORT,
        user: process.env.DEST_DB_USER,
        password: process.env.DEST_DB_PASSWORD,
        database: process.env.DEST_DB_NAME,
        multipleStatements: true 
    }
};

async function runTargetResync() {
    console.time('Total Waktu Resync');
    let sourcePool, destPool;

    try {
        sourcePool = mysql.createPool(config.source);
        destPool = mysql.createPool(config.dest);

        console.log(`🚀 MEMULAI FULL RESYNC (TARGET MODE)...`);

        if (TARGET_TABLES.length === 0) {
            console.error(`❌ DAFTAR TARGET KOSONG! Harap isi variable 'TARGET_TABLES' di dalam file.`);
            process.exit(1);
        }

        // HANYA ambil tabel fisik (Base Table)
        const [tablesRaw] = await sourcePool.query("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'");
        
        // LOGIC FILTER (WHITELIST)
        const tablesToSync = tablesRaw
            .map(row => Object.values(row)[0]) // Ambil nama tabel
            .filter(tableName => {
                // Normalisasi (Trim & Lowercase)
                const cleanName = tableName.trim().toLowerCase();
                const cleanTargetList = TARGET_TABLES.map(t => t.trim().toLowerCase());
                
                // HANYA ambil jika ada di daftar target
                return cleanTargetList.includes(cleanName);
            });

        console.log(`📋 Target Sync: ${tablesToSync.length} tabel.`);
        
        if (tablesToSync.length > 0) {
            console.log(`   Akan memproses: [ ${tablesToSync.join(', ')} ]`);
        } else {
            console.log(`   ⚠️ TIDAK ADA TABEL YANG COCOK. Cek ejaan di 'TARGET_TABLES'.`);
        }

        // Config MySQL biar lancar
        await destPool.query('SET FOREIGN_KEY_CHECKS=0');
        await destPool.query(`SET SESSION innodb_strict_mode=0`);

        for (const tableName of tablesToSync) {
            console.log(`\n🔄 Memproses Tabel: [ ${tableName} ]`);
            
            // 1. Cari Primary Key
            const [keys] = await sourcePool.query(`SHOW KEYS FROM ?? WHERE Key_name = 'PRIMARY'`, [tableName]);
            if (keys.length === 0) {
                console.log(`   ⚠️ SKIP: Tabel tidak punya Primary Key.`);
                continue;
            }
            const pk = keys[0].Column_name;

            // 2. LOOPING RESYNC (Pagination by ID)
            let lastId = 0; 
            let totalSynced = 0;
            let isFinished = false;

            while (!isFinished) {
                const query = `
                    SELECT * FROM ?? 
                    WHERE ?? > ? 
                    ORDER BY ?? ASC 
                    LIMIT 5000
                `;

                const [rows] = await sourcePool.query(query, [tableName, pk, lastId, pk]);

                if (rows.length === 0) {
                    isFinished = true;
                    console.log(`   ✅ Tabel ${tableName} SELESAI.`);
                } else {
                    lastId = rows[rows.length - 1][pk];

                    const columns = Object.keys(rows[0]);
                    const values = rows.map(row => Object.values(row));
                    const placeholders = rows.map(() => `(${columns.map(() => '?').join(', ')})`).join(', ');
                    const flatValues = values.flat();

                    const escapedColumns = columns.map(col => `\`${col}\``).join(', ');
                    const updateOnDuplicate = columns.map(f => `\`${f}\` = VALUES(\`${f}\`)`).join(', ');

                    const sql = `INSERT INTO ?? (${escapedColumns}) VALUES ${placeholders} 
                                 ON DUPLICATE KEY UPDATE ${updateOnDuplicate}`;

                    await destPool.query(sql, [tableName, ...flatValues]);
                    
                    totalSynced += rows.length;
                    process.stdout.write(`\r   ⚡ Menyisir ID: ${lastId} (Total: ${totalSynced} baris)...`);
                }
            }
        }

    } catch (err) {
        console.error('\n❌ Global Error:', err.message);
    } finally {
        if (sourcePool) await sourcePool.end();
        if (destPool) await destPool.end();
        console.timeEnd('Total Waktu Resync');
    }
}

runTargetResync();