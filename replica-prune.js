require('dotenv').config();
const mysql = require('mysql2/promise');

const config = {
    source: {
        host: process.env.SOURCE_DB_HOST,
        port: process.env.SOURCE_DB_PORT || 3306,
        user: process.env.SOURCE_DB_USER,
        password: process.env.SOURCE_DB_PASSWORD,
        database: process.env.SOURCE_DB_NAME,
        supportBigNumbers: true, // PENTING: Biar ID besar gak error
        bigNumberStrings: true,   // PENTING: Biar ID besar jadi string
        compress: true
    },
    dest: {
        host: process.env.DEST_DB_HOST,
        port: process.env.DEST_DB_PORT, // <--- PORT DOCKER
        user: process.env.DEST_DB_USER,
        password: process.env.DEST_DB_PASSWORD,
        database: process.env.DEST_DB_NAME,
        supportBigNumbers: true,
        bigNumberStrings: true,
        multipleStatements: true
    }
};

async function runPrune() {
    // console.time('Total Waktu Prune'); // Matikan timer biar log cron bersih
    let sourcePool, destPool;

    try {
        sourcePool = mysql.createPool(config.source);
        destPool = mysql.createPool(config.dest);

        // HANYA ambil tabel fisik (Base Table)
        const [tablesRaw] = await sourcePool.query("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'");
        const tablesToSync = tablesRaw.map(row => Object.values(row)[0]);

        for (const tableName of tablesToSync) {
            try {
                // 1. Cari Primary Key
                const [keys] = await sourcePool.query(`SHOW KEYS FROM ?? WHERE Key_name = 'PRIMARY'`, [tableName]);

                if (keys.length === 0) {
                    // console.log(`[${tableName}] Skip Prune (No PK).`);
                    continue;
                }
                const pk = keys[0].Column_name;

                // 2. Ambil SEMUA ID dari Source (cPanel)
                // Kita konversi ke String biar aman (String "123" == String "123")
                const [sourceIds] = await sourcePool.query(`SELECT ?? FROM ??`, [pk, tableName]);
                
                // SAFETY: Kalau source kosong (0 baris), JANGAN HAPUS APAPUN DI DESTINASI.
                // Takutnya koneksi ke cPanel error tapi script lanjut jalan, nanti malah ngapus data server fisik.
                if (sourceIds.length === 0) {
                    // Cek dulu apakah tabel emang kosong beneran?
                    // Tapi untuk safety mending skip aja.
                    // console.log(`[${tableName}] Source kosong. Skip delete safety.`);
                    continue; 
                }

                // Masukkan ke SET (Kamus Cepat)
                const sourceIdSet = new Set(sourceIds.map(r => String(r[pk])));

                // 3. Ambil SEMUA ID dari Server Fisik (Dest)
                const [destIds] = await destPool.query(`SELECT ?? FROM ??`, [pk, tableName]);

                // 4. Bandingkan: Mana ID di Server Fisik yg GAK ADA di cPanel?
                const idsToDelete = [];
                for (const row of destIds) {
                    // Bandingkan sebagai String juga
                    if (!sourceIdSet.has(String(row[pk]))) {
                        idsToDelete.push(row[pk]);
                    }
                }

                if (idsToDelete.length > 0) {
                    // Hapus per batch 1000
                    const chunkSize = 1000;
                    for (let i = 0; i < idsToDelete.length; i += chunkSize) {
                        const chunk = idsToDelete.slice(i, i + chunkSize);
                        await destPool.query(`DELETE FROM ?? WHERE ?? IN (?)`, [tableName, pk, chunk]);
                    }
                    console.log(`[${tableName}] 💀 Dihapus ${idsToDelete.length} data sampah.`);
                } 
                // else { console.log(`[${tableName}] ✨ Bersih.`); }

            } catch (err) {
                console.error(`[${tableName}] ❌ Error Prune: ${err.message}`);
            }
        }

    } catch (err) {
        console.error('Global Error:', err.message);
    } finally {
        if (sourcePool) await sourcePool.end();
        if (destPool) await destPool.end();
        // console.timeEnd('Total Waktu Prune');
    }
}

runPrune();