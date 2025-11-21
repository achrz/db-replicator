require('dotenv').config();
const mysql = require('mysql2/promise');

const config = {
    host: process.env.DEST_DB_HOST,
    user: process.env.DEST_DB_USER,
    password: process.env.DEST_DB_PASSWORD,
    database: process.env.DEST_DB_NAME
};

async function checkTables() {
    const connection = await mysql.createConnection(config);
    console.log(`Connecting to DB: ${config.database} at ${config.host}...`);

    try {
        // 1. Cek Daftar Tabel
        const [rows] = await connection.query("SHOW TABLES");
        console.log("\n📋 DAFTAR TABEL YANG DITEMUKAN SCRIPT:");
        
        if (rows.length === 0) {
            console.log("⚠️ DATABASE KOSONG! Tidak ada tabel sama sekali.");
        } else {
            // Ambil value dari object row (karena key-nya dinamis 'Tables_in_dbname')
            const tableNames = rows.map(row => Object.values(row)[0]);
            tableNames.forEach(t => console.log(` - ${t}`));
            
            console.log("\n------------------------------------------------");
            
            // 2. Cek Spesifik Tabel 'users'
            const target = 'users'; // Ganti sesuai yang kamu tulis di script replica
            if (tableNames.includes(target)) {
                console.log(`✅ Tabel '${target}' DITEMUKAN persis (Case Sensitive Match).`);
            } else {
                console.log(`❌ Tabel '${target}' TIDAK DITEMUKAN!`);
                
                // Cek apakah ada yang mirip (beda huruf besar/kecil)
                const mirip = tableNames.find(t => t.toLowerCase() === target.toLowerCase());
                if (mirip) {
                    console.log(`💡 TAPI ada tabel bernama '${mirip}'. Harap ubah scriptmu menyesuaikan huruf besar/kecilnya.`);
                }
            }
        }

    } catch (err) {
        console.error("❌ KONEKSI GAGAL:", err.message);
    } finally {
        await connection.end();
    }
}

checkTables();