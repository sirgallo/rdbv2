package request

import (
	"net/http"
	"sync"
	"time"
	"github.com/sirgallo/logger"
	
	"github.com/sirgallo/rdbv2/internal/state"
	"github.com/sirgallo/rdbv2/internal/system"
)


type RequestServiceOpts struct {
	Port int
	CurrentSystem *system.System
}

type RequestService struct {
	Mux *http.ServeMux
	Port string
	Mutex sync.Mutex
	CurrentSystem *system.System
	RequestChannel chan *state.StateOperation
	ResponseChannel chan *state.StateResponse
	ClientMappedResponseChannels sync.Map
	Log logger.CustomLog
}

type RequestInfo string
type RequestError string


const NAME = "HTTP Service"
const CommandRoute = "/command"
const RequestChannelSize = 100000
const ResponseChannelSize = 10000
const HTTPTimeout = 2 * time.Second

const (
	SERVICE_LISTENING RequestInfo = "http service starting"
)

const (
	CLIENT_CHANNEL_ERROR RequestError = "client channel error"
	ENCODING_ERROR string = "encoding error"
	JSON_PARSE_ERROR string = "unable to parse JSON object"
	METHOD_NOT_ALLOWED string = "http method not allowed"
	NO_LEADER_ERROR string = "no leader available"
	REQUEST_HASH_ERROR string = "unable to produce hash"
	SERVICE_START_ERROR RequestError = "http service error"
)