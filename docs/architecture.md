# Arsitektur â€” harga-pangan-pipeline

Diagram ini menggambarkan alur data dari sumber publik hingga dashboard dan
alert. Sebagian komponen baru akan diimplementasikan pada fase berikutnya;
diagram ini menjadi acuan target.

```mermaid
flowchart TD
    subgraph Sources["Sumber data publik"]
        A1["PIHPS Bank Indonesia /\nPanel Harga BPN"]
        A2["BPS"]
        A3["data.go.id /\nSatu Data Indonesia"]
        A4["Fallback: CSV sampel\n(data/sample)"]
    end

    subgraph Ingestion["Ingestion (Python)"]
        B1["fetch dengan retry\n+ logging terstruktur"]
        B2["validasi pandera\n(tipe, rentang harga, null di kolom kunci)"]
        B3["raw layer\n(append-only, idempoten)"]
        B4["quarantine\n(gagal + alasan)"]
        B5["pipeline_runs\n(waktu, jumlah baris, status)"]
    end

    subgraph Warehouse["DuckDB warehouse"]
        C1["raw"]
        C2["staging"]
        C3["intermediate"]
        C4["marts\n(harga harian, perubahan\nmingguan/bulanan, volatilitas)"]
        C5["anomaly_flags"]
    end

    subgraph Transform["dbt-duckdb"]
        D1["staging: rapikan nama & tipe"]
        D2["intermediate: normalisasi\nkomoditas & wilayah"]
        D3["marts + dbt tests"]
    end

    subgraph Orchestration["Dagster"]
        E1["asset graph\n+ schedule harian"]
        E2["data freshness check"]
    end

    subgraph Output["Output"]
        F1["Streamlit dashboard"]
        F2["Insight page"]
        F3["Alert: log /\nTelegram / Discord (opsional)"]
    end

    A1 --> B1
    A2 --> B1
    A3 --> B1
    A4 --> B1
    B1 --> B2
    B2 -->|lolos| B3
    B2 -->|gagal| B4
    B3 --> C1
    B3 --> B5
    C1 --> D1 --> D2 --> D3 --> C2
    C2 --> C3 --> C4
    C4 --> C5
    E1 -.mengorkestrasi.-> B1
    E1 -.mengorkestrasi.-> D3
    E1 -.mengorkestrasi.-> C5
    E2 -.cek.-> C4
    C4 --> F1
    C4 --> F2
    C5 --> F1
    C5 --> F3
    B4 --> F1
    B5 --> F1
```

## Alur ringkas

1. **Ingestion** mengambil data dari sumber publik (dengan retry). Jika tidak
   bisa diakses otomatis, gunakan CSV sampel dari `data/sample/`.
2. **Validasi pandera** memisahkan data bersih (masuk `raw`) dari data buruk
   (masuk `quarantine` beserta alasannya). Setiap run dicatat di `pipeline_runs`.
3. **dbt** mengubah `raw` menjadi `staging` â†’ `intermediate` â†’ `marts`, dengan
   dbt tests sebagai gerbang kualitas.
4. **Deteksi anomali** menulis flag ke `anomaly_flags`.
5. **Dagster** mengorkestrasi seluruh langkah dengan schedule harian dan
   freshness check.
6. **Streamlit** menampilkan tren, anomali, ringkasan kualitas data, dan insight.
7. **Alert** (opsional) dikirim via log/Telegram/Discord.
