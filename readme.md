Buat replika dari db cpanel ea, db cpanel kan gak full access tuh

NB : harus export import structure db nya dulu biar ada kerangka duplikat nya (biar langsung ada primary key sama relasi nya)

cara kerja : dia ngeselect data dari tabel yang ada updated_at dan kolom id nya, kalau gk ada bakal di skip (ini buat penanda migrasi nya udah sampai mana). Jadi si tabel nya harus ada kolom updated_at sama id.
Dan ini bisa dinamis baca semua tabel, jadi gak perlu nulis satu satu nama tabel nya. Tinggal atur di env db sama auth nya

bisa realtime, tinggal pakein cron job aja, kalau mau sync pakai replica-upsert atau init-sync kalau masih pertama kali. Kalau mau sync yang delete tinggal cron replica-prune

udah sih gitu aja palingan