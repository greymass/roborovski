# Roborovski

High-performance blockchain history infrastructure for Antelope networks.

Roborovski provides:
- Efficient block/transaction storage with sliced architecture
- APIs for querying blockchain history
- Framework for building custom blockchain indexes

## Build

```bash
make build    # Build all services
make test     # Run tests
make verify   # Full validation
```

## License

The services (everything under `services/`, and this repository as a whole) are
licensed under AGPL-3.0 - see [LICENSE](LICENSE).

The reusable Go modules under `libraries/` are licensed under MPL-2.0 so they
can be integrated into any application, open or closed source - see the
LICENSE file in each library module.
