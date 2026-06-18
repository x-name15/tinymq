build:
	go build -o bin/tinymq ./cmd/tinymq

run:
	go run ./cmd/tinymq ./data

clean:
	rm -rf bin/ data/