# Test


## Mocks

For each module, a mock implementation is generated. Mocks were generated using:
```bash
mockgen -source=./internal/campaign/campaign_proto/campaign.pb.proto -destination=./test/mock/campaign/Mock_Proto.go -package=mock
mockgen -source=api/replication.proto -destination=test/mock/replication/Mock_Proto.go -package=mock
mockgen -source=api/snapshot.proto -destination=test/mock/snapshot/Mock_Proto.go -package=mock
```