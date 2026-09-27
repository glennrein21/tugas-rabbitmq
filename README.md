# Tugas Messaging Queue (RabbitMQ) - Alur Pembayaran
# Glenn Reinard Stanis - 27125029

Tugas ini mengimplementasikan komunikasi asynchronous menggunakan Topic Exchange, lengkap dengan fitur simulasi sukses, gagal, mekanisme Retry (TTL), dan penanganan Dead Letter Queue (DLX).

## Cara Menjalankan Proyek (macOS / Apple Silicon)

1. Pastikan Docker Desktop sudah menyala di Mac Anda.
2. Buka terminal di folder proyek ini, lalu jalankan RabbitMQ via Docker:
   ```bash
   docker-compose up -d