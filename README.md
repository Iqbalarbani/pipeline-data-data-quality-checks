# harga-pangan-pipeline

End-to-end data pipeline untuk memantau harga pangan Indonesia: ingestion â†’
validasi â†’ warehouse â†’ transformasi dbt â†’ deteksi anomali â†’ dashboard.

> **Status:** Fase 1 (scaffolding). Bagian fungsional akan diisi bertahap.
> README ini menjelaskan **tujuan**, **alasan pembuatan**, dan **skill yang
ditunjukkan** proyek; detail operasional ditambahkan saat fase terkait selesai.

---

## 1. Masalah yang dipecahkan

Harga pangan di Indonesia sangat fluktuatif dan berdampak langsung pada inflasi
serta daya beli. Data mentah tersedia dari sumber publik (PIHPS Bank Indonesia,
Panel Harga Badan Pangan Nasional, BPS), tetapi tersebar, tidak konsisten formatnya,
dan tidak otomatis terpantau.

Proyek ini membangun pipeline yang:
- mengambil data harga pangan publik secara berkala,
- **menolak data buruk secara eksplisit** (masuk tabel karantina, bukan dibuang),
- menyimpannya di warehouse lokal yang queryable,
- menghasilkan metrik harian/mingguan/bulanan serta volatilitas per komoditas,
- menandai anomali harga, dan
- menyajikannya dalam dashboard dengan satu halaman **Insight** berisi temuan nyata
  (mis. komoditas paling volatil atau pola kenaikan menjelang hari besar keagamaan).

## 2. Mengapa proyek ini dibuat

Tujuan utamanya adalah **portofolio rekayasa data end-to-end**, bukan sekadar
demo grafik. Yang ingin ditunjukkan adalah cara berpikir seorang data engineer:

- **Reproducibility:** siapa pun bisa meng-clone repo dan menjalankannya dengan
  satu perintah tanpa setup server database.
- **Kejujuran data:** data yang gagal validasi tidak hilang diam-diam; ada jejak
  dan alasannya.
- **Idempotency:** menjalankan ulang pipeline tidak membuat duplikat.
- **Kualitas yang teruji:** transformasi dilindungi dbt tests dan unit/integration test.
- **Dokumentasi keputusan:** pilihan teknis dicatat sebagai ADR, termasuk trade-off-nya.
- **Tanpa klaim palsu:** tidak ada angka performa yang tidak diukur.

## 3. Arsitektur (ringkas)

```
Sumber publik (PIHPS/BI, Panel Harga BPN, BPS, data.go.id)
        â”‚  (retry + logging, fallback CSV sampel bila API tidak bisa diakses otomatis)
        â–¼
Ingestion (Python)  â”€â”€â–º  validasi pandera  â”€â”€â”¬â”€â–º  raw (append-only, idempoten)
                                             â””â”€â–º  quarantine (gagal + alasan)
        â–¼
DuckDB warehouse (file tunggal)
        â–¼
dbt-duckdb: staging â†’ intermediate â†’ marts
        â–¼
Dagster (aset + schedule harian + freshness check)
        â–¼
Streamlit dashboard + anomaly_flags + alert (log / Telegram / Discord opsional)
```

Diagram Mermaid lengkap: `docs/architecture.md`.

## 4. Tech stack & alasan

| Komponen | Pilihan | Alasan singkat |
|---|---|---|
| Bahasa | Python 3.11+ | Ekosistem data & tooling yang matang |
| Dependency | uv (fallback pip) | Resolusi cepat, lockfile reproducible |
| Orkestrasi | Dagster | Asset-based, lineage jelas, cocok dengan konsep data sebagai aset |
| Warehouse | DuckDB | Embedded, zero-infra, SQL analitik cepat (lihat ADR 0001) |
| Transformasi | dbt-duckdb | Layer staging/intermediate/marts + testing deklaratif |
| Data quality | pandera + dbt tests | Validasi di batas ingestion + constraint di warehouse |
| Dashboard | Streamlit | Prototipe dashboard cepat, cukup untuk portofolio |
| Testing | pytest | Standar de-facto |
| CI | GitHub Actions | Lint (ruff) + test + dbt build |
| Container | Docker + docker-compose | Menjalankan seluruh stack dengan `make up` |

## 5. Cara menjalankan

> Lengkap akan diisi pada fase berikutnya. Rencana (maksimal 5 langkah):

```bash
cp .env.example .env     # 1. konfigurasi
make install             # 2. install dependensi
make up                  # 3. jalankan pipeline + dashboard (docker-compose)
make test                # 4. jalankan test
# 5. buka dashboard di http://localhost:8501
```

## 6. Struktur repo

```
harga-pangan-pipeline/
â”œâ”€â”€ README.md
â”œâ”€â”€ Makefile
â”œâ”€â”€ pyproject.toml
â”œâ”€â”€ docs/
â”‚   â”œâ”€â”€ adr/                 # Architecture Decision Records
â”‚   â””â”€â”€ architecture.md      # diagram Mermaid
â”œâ”€â”€ src/
â”‚   â”œâ”€â”€ ingestion/           # fetch, validasi, simpan raw + quarantine
â”‚   â”œâ”€â”€ quality/             # skema pandera, util validasi
â”‚   â””â”€â”€ dashboard/           # Streamlit
â”œâ”€â”€ dbt_project/             # models (staging/intermediate/marts), tests, seeds
â”œâ”€â”€ orchestration/           # aset & schedule Dagster
â”œâ”€â”€ tests/
â””â”€â”€ data/sample/             # fallback dataset CSV
```

## 7. Skill yang ditunjukkan proyek ini

- **Desain pipeline data:** pemisahan layer (raw â†’ staging â†’ marts), strategi
  idempotency, penanganan kegagalan dengan retry dan logging terstruktur.
- **Data quality engineering:** pandera untuk validasi saat ingestion, tabel
  quarantine dengan alasan penolakan, serta dbt tests (`unique`, `not_null`,
  `accepted_values`, `relationships`, dan custom test seperti lonjakan harga
  harian > X%).
- **SQL analitik & pemodelan dbt:** normalisasi komoditas/wilayah, penanganan
  data hilang, metrik volatilitas dan perubahan mingguan/bulanan.
- **Deteksi anomali:** z-score / rolling median, penyimpanan flag ke tabel khusus.
- **Orkestrasi:** Dagster asset graph dengan dependensi eksplisit, schedule harian,
  dan data freshness check.
- **Observability:** log terstruktur dan tabel `pipeline_runs` (waktu, jumlah baris,
  status).
- **Rekayasa perangkat lunak:** type hints, docstring, linting ruff, unit & integration
  test dengan pytest, CI hijau di GitHub Actions.
- **DevOps dasar:** Docker + docker-compose, Makefile sebagai antarmuka tunggal.
- **Dokumentasi teknis:** README studi kasus, ADR berisi konteks/opsi/keputusan/
  konsekuensi, dan `docs/insights.md` dengan temuan data berangka.

## 8. Batasan yang diketahui

- Dataset berskala kecil; DuckDB dipilih karena itu (lihat ADR 0001).
- Ketergantungan pada akses sumber publik: bila endpoint tidak dapat diakses
  otomatis (dibatasi rate limit atau butuh sesi), digunakan fallback CSV di
  `data/sample/` dan hal ini didokumentasikan.
- Bukan sistem multi-user; penulisan warehouse bersifat serial.

## 9. Roadmap fase

1. **Scaffolding & tooling** (selesai) â€” struktur repo, ruff, pytest, CI hijau.
2. Ingestion + validasi pandera + quarantine + tes.
3. Model dbt + tes kualitas data.
4. Deteksi anomali + orkestrasi Dagster.
5. Dashboard Streamlit + halaman Insight.
6. Docker, README final, ADR lengkap, polish.

## 10. Lisensi

MIT.
