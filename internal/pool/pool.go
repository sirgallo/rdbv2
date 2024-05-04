package pool

import (
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
)


//=========================================== Pool


//	NewPool:
//		initialize the connection pool.
//		the purpose of the connection pool is to reuse connections once they have been made, minimizing overhead for reconnecting to a host every time an rpc is made.
//		the pool has the following structure:
//			{
//				[key: address/host]: Array<connections>
//			}
func NewPool(opts PoolOpts) *Pool {
	return &Pool{ maxConn: opts.MaxConn }
}

//	GetConnection:
//		1.) load connections for the particular host/address
//		2.) if the address was loaded from the thread safe map:
//			if the total connections in the map is greater than max connections specified throw max connections error
//			otherwise for each connection in the array of connections, if the connection is not null and the connection is ready for work, return the connection
//		3.) if the address was not loaded, create a new grpc connection and store the new connection at the key associated with the address/host and return the new connection
//	
//		for grpc connection opts, we will automatically compress the rpc on the wire
func (cp *Pool) GetConnection(addr string, port string) (*grpc.ClientConn, error) {
	connections, loaded := cp.connections.Load(addr)
	if loaded {
		if len(connections.([]*grpc.ClientConn)) >= cp.maxConn { return nil, errors.New(MAX_CONNECTIONS) }
		for _, conn := range connections.([]*grpc.ClientConn) {
			if conn != nil && conn.GetState() == connectivity.Ready { return conn, nil }
		}
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)),
	}

	newConn, connErr := grpc.Dial(addr + port, opts...)
	if connErr != nil { 
		cp.connections.Delete(addr)
		return nil, connErr 
	}

	emptyConns, loaded := cp.connections.LoadOrStore(addr, []*grpc.ClientConn{newConn})
	if loaded {
		connections := emptyConns.([]*grpc.ClientConn)
		cp.connections.Store(addr, append(connections, newConn))
	}
	
	return newConn, nil
}

//	PutConnection:
//		1.) load connections for the particular host/address
//		2.) if the address was loaded from the thread safe map:
//			if the connection already exists in the map, return otherwise, close the connection and return
func (cp *Pool) PutConnection(addr string, connection *grpc.ClientConn) (bool, error) {
	connections, loaded := cp.connections.Load(addr)
	if loaded {
		for _, conn := range connections.([]*grpc.ClientConn) {
			if conn == connection { return true, nil }
		}
	}

	closeErr := connection.Close()
	if closeErr != nil { return false, closeErr }
	return false, nil
}

//	CloseConnections:
//		1.) load connections for the particular host/address
//		2.) if the address was loaded from the thread safe map:
//			if the connection already exists in the map, close the connection
//		3.) remove the key from the map
func (cp *Pool) CloseConnections(addr string) (bool, error) {
	connections, loaded := cp.connections.Load(addr)
	if loaded {
		for _, conn := range connections.([]*grpc.ClientConn) {
			closeErr := conn.Close()
			if closeErr != nil { return false, closeErr }
		}
	}

	cp.connections.Delete(addr)
	return true, nil
}