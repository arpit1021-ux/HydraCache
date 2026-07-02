# Contributing to HydraCache

## Development Setup

```bash
git clone https://github.com/hydracache/hydracache.git
cd hydracache
go mod tidy
```

## Running Tests

```bash
# Unit tests
go test ./...

# With race detector
go test -race ./...

# With coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

## Code Style

- Follow Go conventions (gofmt, go vet)
- Use `golangci-lint` for linting
- Write tests for all new functionality
- Keep functions focused and small
- Use meaningful variable names

## Pull Request Process

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Architecture Decisions

If your change involves a design decision, please create an ADR (Architecture Decision Record) in `docs/adr/`.

## Reporting Issues

Use GitHub Issues to report bugs or request features. Include:
- Go version
- OS
- Steps to reproduce
- Expected vs actual behavior
