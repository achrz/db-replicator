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
        compress: true,
        // --- TAMBAHAN PENTING ---
        connectionLimit: 1, // Cuma minta 1 slot koneksi (biar gak antre di cPanel)
        waitForConnections: true,
        queueLimit: 0,
        // --- FIX CONNECTION LOST ---
        enableKeepAlive: true,    // Mencegah diputus server secara sepihak
        keepAliveInitialDelay: 0  // Langsung aktifkan keep-alive
    },
    dest: {
        host: process.env.DEST_DB_HOST,
        port: process.env.DEST_DB_PORT,
        user: process.env.DEST_DB_USER,
        password: process.env.DEST_DB_PASSWORD,
        database: process.env.DEST_DB_NAME,
        supportBigNumbers: true,
        bigNumberStrings: true,
        multipleStatements: true,
        connectionLimit: 1, // Hemat koneksi juga di server fisik
        enableKeepAlive: true,
        keepAliveInitialDelay: 0
    }
};

async function runPrune() {
    let sourcePool, destPool;
    let totalDeletedGlobal = 0; // Statistik global

    try {
        console.log('🔌 Inisialisasi Koneksi Database...');

        sourcePool = mysql.createPool(config.source);
        destPool = mysql.createPool(config.dest);

        console.log('🚀 Sedang mengambil daftar tabel dari cPanel (Source)...');

        // Timer timeout manual buat deteksi stuck
        const timeoutCheck = setTimeout(() => {
            console.log('⚠️  WARNING: Koneksi ke cPanel > 10 detik. Mungkin limit koneksi penuh atau firewall nge-block.');
        }, 10000);

        // HANYA ambil tabel fisik (Base Table)
        const [tablesRaw] = await sourcePool.query("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'");

        clearTimeout(timeoutCheck); // Hapus timer kalau sukses

        console.log(`📋 Berhasil connect! Ditemukan ${tablesRaw.length} tabel.`);

        const tablesToSync = tablesRaw.map(row => Object.values(row)[0]);

        for (let i = 0; i < tablesToSync.length; i++) {
            const tableName = tablesToSync[i];
            try {
                // Log progres biar gak dikira stuck
                console.log(`🔍 [${i + 1}/${tablesToSync.length}] Cek tabel: ${tableName}`);

                // 1. Cari Primary Key
                const [keys] = await sourcePool.query(`SHOW KEYS FROM ?? WHERE Key_name = 'PRIMARY'`, [tableName]);

                if (keys.length === 0) {
                    console.log(`   ⏩ Skip (Tidak ada Primary Key)`);
                    continue;
                }
                const pk = keys[0].Column_name;

                // 2. Ambil SEMUA ID dari Source
                const [sourceIds] = await sourcePool.query(`SELECT ?? FROM ??`, [pk, tableName]);

                if (sourceIds.length === 0) {
                    console.log(`   ⚠️ Source kosong (0 baris). Skip safety.`);
                    continue;
                }

                // Masukkan ke SET
                const sourceIdSet = new Set(sourceIds.map(r => String(r[pk])));

                // 3. Ambil SEMUA ID dari Server Fisik
                const [destIds] = await destPool.query(`SELECT ?? FROM ??`, [pk, tableName]);

                // 4. Bandingkan
                const idsToDelete = [];
                for (const row of destIds) {
                    if (!sourceIdSet.has(String(row[pk]))) {
                        idsToDelete.push(row[pk]);
                    }
                }

                if (idsToDelete.length > 0) {
                    const chunkSize = 1000;
                    for (let j = 0; j < idsToDelete.length; j += chunkSize) {
                        const chunk = idsToDelete.slice(j, j + chunkSize);
                        await destPool.query(`DELETE FROM ?? WHERE ?? IN (?)`, [tableName, pk, chunk]);
                    }
                    console.log(`   💀 Dihapus ${idsToDelete.length} data sampah (Ada di Server tapi hilang di cPanel).`);
                    totalDeletedGlobal += idsToDelete.length;
                } else {
                    // Tampilkan info kalau bersih biar tenang
                    console.log(`   ✨ Bersih. (Data Source: ${sourceIds.length} vs Server: ${destIds.length})`);
                }

            } catch (err) {
                console.error(`   ❌ [${tableName}] Error Prune: ${err.message}`);
                // Cek apakah errornya karena putus koneksi?
                if (err.code === 'PROTOCOL_CONNECTION_LOST' || err.message.includes('closed')) {
                    console.log('   🔄 Mencoba reconnect otomatis di putaran berikutnya...');
                }
            }
        }

    } catch (err) {
        console.error('\n❌ Global Fatal Error:', err.message);
        console.error('   Cek koneksi internet, IP Whitelist, atau Database Credentials.');
    } finally {
        if (sourcePool) await sourcePool.end();
        if (destPool) await destPool.end();
        console.log(`✅ Selesai. Total data dibersihkan: ${totalDeletedGlobal}`);
    }
}

runPrune();