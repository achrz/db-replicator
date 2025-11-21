require('dotenv').config();
const mysql = require('mysql2/promise');

const config = {
    source: {
        host: process.env.SOURCE_DB_HOST,
        user: process.env.SOURCE_DB_USER,
        password: process.env.SOURCE_DB_PASSWORD,
        database: process.env.SOURCE_DB_NAME
    },
    dest: {
        host: process.env.DEST_DB_HOST,
        user: process.env.DEST_DB_USER,
        password: process.env.DEST_DB_PASSWORD,
        database: process.env.DEST_DB_NAME
    }
};

async function runPrune() {
    console.time('Total Waktu Prune');
    let sourcePool, destPool;

    try {
        sourcePool = mysql.createPool(config.source);
        destPool = mysql.createPool(config.dest);

        console.log(`🗑️  Connecting to cPanel to fetch table list...`);
        
        // 1. AMBIL DAFTAR TABEL DARI CPANEL
        const [tablesRaw] = await sourcePool.query("SHOW TABLES");
        const tablesToSync = tablesRaw.map(row => Object.values(row)[0]);

        for (const tableName of tablesToSync) {
            try {
                // 2. Cari Primary Key (Wajib punya PK buat hapus)
                const [keys] = await sourcePool.query(`SHOW KEYS FROM ?? WHERE Key_name = 'PRIMARY'`, [tableName]);
                
                if (keys.length === 0) {
                    // Skip kalau tabel gak punya ID unik
                    continue; 
                }
                const pk = keys[0].Column_name;

                // 3. Ambil SEMUA ID dari cPanel (Source)
                const [sourceIds] = await sourcePool.query(`SELECT ?? FROM ??`, [pk, tableName]);
                const sourceIdSet = new Set(sourceIds.map(r => r[pk]));

                // 4. Ambil SEMUA ID dari Server Fisik (Dest)
                const [destIds] = await destPool.query(`SELECT ?? FROM ??`, [pk, tableName]);
                
                // 5. Bandingkan: Mana ID di Server Fisik yg GAK ADA di cPanel?
                const idsToDelete = [];
                for (const row of destIds) {
                    if (!sourceIdSet.has(row[pk])) {
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
                } else {
                    console.log(`[${tableName}] ✨ Bersih (Sinkron).`);
                }

            } catch (err) {
                console.log(`[${tableName}] Error Prune: ${err.message}`);
            }
        }

    } catch (err) {
        console.error('Global Error:', err.message);
    } finally {
        if (sourcePool) await sourcePool.end();
        if (destPool) await destPool.end();
        console.timeEnd('Total Waktu Prune');
    }
}

runPrune();