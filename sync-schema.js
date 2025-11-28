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
        port: process.env.DEST_DB_PORT, // Pastikan port 3307 (Docker)
        user: process.env.DEST_DB_USER,
        password: process.env.DEST_DB_PASSWORD,
        database: process.env.DEST_DB_NAME,
        multipleStatements: true 
    }
};

async function syncSchema() {
    console.time('Waktu Sync Schema');
    let sourcePool, destPool;

    try {
        sourcePool = mysql.createPool(config.source);
        destPool = mysql.createPool(config.dest);

        console.log(`🚀 MEMULAI SYNC SCHEMA (FUNCTIONS & VIEWS)...`);

        // ==========================================================
        // PART 1: FUNCTIONS & PROCEDURES
        // ==========================================================
        console.log(`\n🔄 1. Mengambil FUNCTIONS & PROCEDURES...`);
        
        // Ambil daftar function
        const [funcs] = await sourcePool.query(`SHOW FUNCTION STATUS WHERE Db = ?`, [config.source.database]);
        
        for (const f of funcs) {
            const funcName = f.Name;
            console.log(`   -> Processing Function: ${funcName}`);

            try {
                // Ambil resep aslinya
                const [createRes] = await sourcePool.query(`SHOW CREATE FUNCTION \`${funcName}\``);
                let sqlCreate = createRes[0]['Create Function'];

                // BERSIH-BERSIH (PENTING!)
                // Hapus DEFINER=`user_cpanel`@`localhost` biar gak error access denied
                sqlCreate = sqlCreate.replace(/DEFINER=`[^`]+`@`[^`]+`/gi, '');
                sqlCreate = sqlCreate.replace(/DEFINER=\w+@\w+/gi, '');

                // Hapus dulu kalau ada
                await destPool.query(`DROP FUNCTION IF EXISTS \`${funcName}\``);
                // Buat ulang
                await destPool.query(sqlCreate);
                console.log(`      ✅ Created.`);
            } catch (e) {
                console.error(`      ❌ Gagal: ${e.message}`);
            }
        }

        // ==========================================================
        // PART 2: VIEWS
        // ==========================================================
        console.log(`\n🔄 2. Mengambil VIEWS...`);

        // Ambil daftar View saja
        const [views] = await sourcePool.query("SHOW FULL TABLES WHERE Table_type = 'VIEW'");
        const viewNames = views.map(row => Object.values(row)[0]);

        // Kita perlu Loop 2x. 
        // Kenapa? Karena kadang View A butuh View B. Kalau View B belum dibuat, View A error.
        // Jadi kita coba create, kalau gagal kita taruh belakang antrian (Retry).
        
        let pendingViews = [...viewNames];
        let retryCount = 0;
        const MAX_RETRIES = 5; // Batas putaran

        while (pendingViews.length > 0 && retryCount < MAX_RETRIES) {
            console.log(`   --- Putaran ke-${retryCount + 1} (Sisa ${pendingViews.length} view) ---`);
            const nextPending = [];

            for (const viewName of pendingViews) {
                try {
                    // Ambil resep View
                    const [createRes] = await sourcePool.query(`SHOW CREATE VIEW \`${viewName}\``);
                    let sqlCreate = createRes[0]['Create View'];

                    // BERSIH-BERSIH
                    // Hapus DEFINER
                    sqlCreate = sqlCreate.replace(/DEFINER=`[^`]+`@`[^`]+`/gi, '');
                    sqlCreate = sqlCreate.replace(/DEFINER=\w+@\w+/gi, '');
                    
                    // Hapus SQL SECURITY DEFINER (Opsional, ubah jadi INVOKER biar aman)
                    sqlCreate = sqlCreate.replace(/SQL SECURITY DEFINER/gi, 'SQL SECURITY INVOKER');

                    // Hapus view lama
                    await destPool.query(`DROP VIEW IF EXISTS \`${viewName}\``);
                    
                    // Buat view baru
                    await destPool.query(sqlCreate);
                    console.log(`      ✅ View '${viewName}' Created.`);

                } catch (e) {
                    // Kalau error "Table doesn't exist", mungkin dia butuh View lain yg belum kebuat
                    // Masukkan ke antrian buat dicoba lagi nanti
                    if (e.message.includes("doesn't exist")) {
                         // console.log(`      ⏳ View '${viewName}' ditunda (tunggu dependency)...`);
                         nextPending.push(viewName);
                    } else {
                        console.error(`      ❌ View '${viewName}' Error Fatal: ${e.message}`);
                    }
                }
            }

            // Cek apakah ada kemajuan? Kalau jumlah pending sama terus, berarti mentok (circular dependency)
            if (nextPending.length === pendingViews.length && nextPending.length > 0) {
                console.warn(`   ⚠️ Warning: Sisa view ini mungkin punya dependency melingkar atau tabel hilang.`);
                nextPending.forEach(v => console.log(`      - ${v}`));
                break; // Stop loop biar gak infinite
            }

            pendingViews = nextPending;
            retryCount++;
        }

    } catch (err) {
        console.error('Global Error:', err.message);
    } finally {
        if (sourcePool) await sourcePool.end();
        if (destPool) await destPool.end();
        console.timeEnd('Waktu Sync Schema');
    }
}

syncSchema();