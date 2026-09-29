up:      ## Jalankan Kafka, producer, dan streaming job
	docker compose up -d --build
logs:    ## Lihat log streaming job
	docker compose logs -f streaming
query:   ## Jalankan demo fitur Iceberg
	docker compose --profile tools run --rm query
count:   ## Cek jumlah pesan di topic
	docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic orders
down:    ## Stop semua
	docker compose down
clean:   ## Stop + hapus data lab
	docker compose down -v && rm -rf warehouse checkpoints
