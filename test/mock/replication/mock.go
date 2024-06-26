// Code generated by MockGen. DO NOT EDIT.
// Source: ./internal/replication/replication_proto/replication.pb.go

// Package mock is a generated GoMock package.
package mock

import (
	"github.com/sirgallo/rdbv2/internal/replication/replication_proto"
)


// create mock implementation for gRPC client interface
type MockReplicationServiceClient struct {
	// implement mock behavior here
}

func (m *MockReplicationServiceClient) AppendEntryRPC(req *replication_proto.AppendEntry) (*replication_proto.AppendEntryResponse, error) {
	// simulate behavior of AppendEntryRPC

	// for example, always return success
	response := &replication_proto.AppendEntryResponse{
		Term:         1,
		NextLogIndex: 5,
		Success:      true,
	}

	return response, nil
}