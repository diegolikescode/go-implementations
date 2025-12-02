lets:
	go run ./cmd/main.go

test:
	# go clean -testcache
	go test ./... -v

test-nc:
	go clean -testcache
	# go test ./... -v
	go test ./internal/aws -v
