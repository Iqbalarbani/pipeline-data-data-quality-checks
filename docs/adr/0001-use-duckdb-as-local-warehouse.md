# ADR 0001 â€” Menggunakan DuckDB sebagai data warehouse lokal

- Status: Accepted
- Tanggal: 2026-11-24

## Konteks
Proyek portofolio ini harus bisa dijalankan end-to-end oleh siapa pun yang
meng-clone repo, tanpa biaya cloud dan tanpa menyiapkan server database.
Volumenya kecil (harga harian puluhan komoditas x puluhan provinsi), tetapi
butuh SQL analitik yang nyaman, transaksi, dan integrasi mulus dengan dbt.

## Opsi yang dipertimbangkan
1. **PostgreSQL via Docker** â€” realistis untuk produksi, tetapi menambah
   service, kredensial, dan langkah setup; overkill untuk dataset kecil.
2. **SQLite** â€” nol setup, tetapi tipe data lemah, kurang cocok untuk
   window function berat dan ekosistem dbt yang terbatas.
3. **DuckDB** â€” embedded, columnar, SQL analitik cepat, adapter
dbt-duckdb resmi, satu file untuk seluruh warehouse.

## Keputusan
Menggunakan DuckDB (file tunggal, mis. `data/warehouse/harga_pangan.duckdb`).

## Konsekuensi
- **Positif:** zero-infra, reproducible, `make up` cukup menjalankan Python;
  dbt dan Streamlit membaca file yang sama; mudah di-CI.
- **Negatif:** bukan sistem multi-user; penulisan bersamaan terbatas (single
  writer). Tidak mewakili operasi warehouse terdistribusi.
- **Mitigasi:** lapisan ingestion menulis secara serial dan idempoten, jadi
  batasan single-writer tidak menjadi masalah untuk beban kerja ini.
