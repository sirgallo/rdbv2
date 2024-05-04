package request

import (
	"net/http"
	"sync"
	"github.com/sirgallo/logger"
	
	"github.com/sirgallo/rdbv2/internal/state"
	rdbUtils "github.com/sirgallo/rdbv2/internal/utils"
)


//=========================================== Request Service


//	NewRequestService:
//		create a new service instance with passable options.
//		initialize the mux server and register route handlers on it, in this case the command route for sending operations to perform on the state machine.
func NewRequestService(opts *RequestServiceOpts) *RequestService {
	reqService := &RequestService{
		Mux: http.NewServeMux(),
		Port: rdbUtils.NormalizePort(opts.Port),
		CurrentSystem: opts.CurrentSystem,
		RequestChannel: make(chan *state.StateOperation, RequestChannelSize),
		ResponseChannel: make(chan *state.StateResponse, ResponseChannelSize),
		ClientMappedResponseChannels: sync.Map{},
		Log: *logger.NewCustomLog(NAME),
	}

	reqService.RegisterCommandRoute()
	return reqService
}

//	StartRequestService:
//		separate go routines:
//			1.) http server starts the server to begin listening for client requests
//			2.) handle response channel:
//				for incoming respones:
//					check the request id against the mapping of client response channels
//					if the channel exists for the response, pass the response back to the route so it can be returned to the client
func (reqService *RequestService) StartRequestService() {
	go func() {
		reqService.Log.Info(SERVICE_LISTENING, "port:", reqService.Port)
		srvErr := http.ListenAndServe(reqService.Port, reqService.Mux)
		if srvErr != nil { reqService.Log.Fatal(SERVICE_START_ERROR) }
	}()

	go func() {
		for response := range reqService.ResponseChannel {
			go func(response *state.StateResponse) {
				c, ok := reqService.ClientMappedResponseChannels.Load(response.RequestId)
				if ok {
					clientChannel := c.(chan *state.StateResponse)
					clientChannel <- response
				} else { reqService.Log.Warn(CLIENT_CHANNEL_ERROR, response.RequestId) }
			}(response)
		}
	}()
}