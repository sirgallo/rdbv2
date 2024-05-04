# Test


## Mocks

For each module, a mock implementation is generated. Mocks were generated using:
```bash
mockgen -source=./internal/campaign/campaign_proto/campaign.pb.go -destination=./test/mock/campaign/mock.go -package=mock
mockgen -source=./internal/replication/replication_proto/replication.pb.go -destination=test/mock/replication/mock.go -package=mock
mockgen -source=./internal/snapshot/snapshot_proto/snapshot.pb.go -destination=test/mock/snapshot/mock.go -package=mock
```