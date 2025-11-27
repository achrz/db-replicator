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

async function runUpsert() {
    console.time('Total Waktu Sync');
    let sourcePool, destPool;

    try {
        sourcePool = mysql.createPool(config.source);
        destPool = mysql.createPool(config.dest);

        console.log(`🚀 Connecting to cPanel to fetch table list...`);

        // HANYA ambil tabel fisik (Base Table), abaikan View
        const [tablesRaw] = await sourcePool.query("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'");

        // Ubah hasil query jadi array string nama tabel
        // Object.values(row)[0] mengambil value pertama (nama tabel) tanpa peduli key-nya
        const tablesToSync = tablesRaw.map(row => Object.values(row)[0]);

        console.log(`📋 Ditemukan ${tablesToSync.length} tabel: [ ${tablesToSync.join(', ')} ]`);

        // Matikan FK check biar aman
        await destPool.query('SET FOREIGN_KEY_CHECKS=0');

        for (const tableName of tablesToSync) {
            try {
                // A. Cek/Create Tabel (Skip kalau kamu udah import manual, ini jaga-jaga aja)
                const [checkTable] = await destPool.query(`SHOW TABLES LIKE ?`, [tableName]);

                if (checkTable.length === 0) {
                    console.log(`[${tableName}] Tabel belum ada di server fisik. Mencoba copy structure...`);
                    const [createSyntax] = await sourcePool.query(`SHOW CREATE TABLE ??`, [tableName]);
                    let sqlCreate = createSyntax[0]['Create Table'];
                    sqlCreate = sqlCreate.replace(/`[^`]+`\./g, ''); // Hapus nama DB cPanel
                    await destPool.query(sqlCreate);
                }

                // B. Cek Last Sync (Logic Updated_at)
                // Pastikan tabel punya kolom updated_at, kalau gak punya, kita skip logikanya
                let lastSync = new Date(0); // Default 1970
                let hasUpdatedAt = false;

                // Cek kolom dulu
                const [cols] = await destPool.query(`SHOW COLUMNS FROM ?? LIKE 'updated_at'`, [tableName]);
                if (cols.length > 0) {
                    hasUpdatedAt = true;
                    const [rows] = await destPool.query(`SELECT MAX(updated_at) as last_sync FROM ??`, [tableName]);
                    if (rows[0].last_sync) {
                        lastSync = new Date(new Date(rows[0].last_sync).getTime() - 60000);
                    }
                } else {
                    // Kalau gak ada updated_at, kita asumsikan selalu sync data baru (berdasarkan PK) 
                    // atau skip dulu biar gak error. Disini saya set warning aja.
                    console.log(`[${tableName}] ⚠️  Warning: Tidak ada kolom 'updated_at'. Full scan mungkin berat.`);
                }

                // C. Tarik Data
                let query = '';
                let params = [];

                if (hasUpdatedAt) {
                    query = `SELECT * FROM ?? WHERE updated_at >= ? LIMIT 10000`;
                    params = [tableName, lastSync];
                } else {
                    // Fallback kalau gak ada updated_at (Optional: skip atau ambil semua)
                    // Disini saya limit 10000 aja biar aman
                    query = `SELECT * FROM ?? LIMIT 10000`;
                    params = [tableName];
                }

                const [changes] = await sourcePool.query(query, params);

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
                } else {
                    console.log(`[${tableName}] 👌 Up-to-date.`);
                }

            } catch (loopErr) {
                console.error(`[${tableName}] ❌ Error: ${loopErr.message}`);
            }
        }

    } catch (err) {
        console.error('Global Error:', err.message);
    } finally {
        if (sourcePool) await sourcePool.end();
        if (destPool) await destPool.end();
        console.timeEnd('Total Waktu Sync');
    }
}

runUpsert();