require('dotenv').config();
const mysql = require('mysql2/promise');

// ==========================================
// 🚀 KONFIGURASI TITIK MULAI (RESUME)
// ==========================================
// Masukkan nama tabel di mana kamu ingin MEMULAI proses.
// Semua tabel SEBELUM tabel ini akan di-SKIP otomatis.
// Tabel INI dan SETELAHNYA akan diproses.
//
// Contoh: Ada tabel A, B, C, D, E.
// Jika diisi 'C', maka A dan B di-skip. C, D, E diproses.
// Kosongkan '' jika ingin memproses dari awal.

const START_FROM_TABLE = '';

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

async function runResyncStartFrom() {
    console.time('Total Waktu Resync');
    let sourcePool, destPool;

    try {
        sourcePool = mysql.createPool(config.source);
        destPool = mysql.createPool(config.dest);

        console.log(`🚀 MEMULAI FULL RESYNC (METODE START FROM)...`);

        if (START_FROM_TABLE) {
            console.log(`   ⏩ Mode Resume Aktif: Mulai dari tabel '${START_FROM_TABLE}'`);
        } else {
            console.log(`   ▶️ Mode Normal: Memproses dari awal.`);
        }

        // HANYA ambil tabel fisik (Base Table)
        const [tablesRaw] = await sourcePool.query("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'");
        const tablesList = tablesRaw.map(row => Object.values(row)[0]);

        // Matikan FK check biar lancar
        await destPool.query('SET FOREIGN_KEY_CHECKS=0');
        await destPool.query(`SET SESSION innodb_strict_mode=0`);

        // --- LOGIC SKIP & RESUME ---
        let isStarted = false; // Flag penanda apakah sudah ketemu tabel start

        // Jika START_FROM_TABLE kosong, anggap sudah start dari awal
        if (!START_FROM_TABLE) isStarted = true;

        for (const tableName of tablesList) {

            // Normalisasi nama untuk perbandingan (biar gak sensitif huruf besar/kecil)
            const currentName = tableName.trim().toLowerCase();
            const targetName = START_FROM_TABLE.trim().toLowerCase();

            // Cek apakah kita sudah boleh mulai?
            if (!isStarted) {
                if (currentName === targetName) {
                    isStarted = true; // KETEMU! Nyalakan flag start
                    console.log(`\n🎯 TARGET DITEMUKAN: [ ${tableName} ] -> Memulai proses...`);
                } else {
                    // Selama belum ketemu target, skip terus
                    console.log(`   ⏭️  Skipping: ${tableName}`);
                    continue;
                }
            }

            // ===========================================
            // MULAI DARI SINI ADALAH LOGIC SYNC (SAMA)
            // ===========================================
            console.log(`\n🔄 Memproses Tabel: [ ${tableName} ]`);

            const [keys] = await sourcePool.query(`SHOW KEYS FROM ?? WHERE Key_name = 'PRIMARY'`, [tableName]);
            if (keys.length === 0) {
                console.log(`   ⚠️ SKIP: Tabel tidak punya Primary Key.`);
                continue;
            }
            const pk = keys[0].Column_name;

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

        if (!isStarted && START_FROM_TABLE) {
            console.log(`\n⚠️ WARNING: Tabel target '${START_FROM_TABLE}' tidak ditemukan dalam database!`);
            console.log(`   Pastikan ejaan nama tabel benar.`);
        }

    } catch (err) {
        console.error('\n❌ Global Error:', err.message);
    } finally {
        if (sourcePool) await sourcePool.end();
        if (destPool) await destPool.end();
        console.timeEnd('Total Waktu Resync');
    }
}

runResyncStartFrom();