package request

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"github.com/sirgallo/utils"
	
	"github.com/sirgallo/rdbv2/internal/state"
	"github.com/sirgallo/rdbv2/internal/system"
	rdbUtils "github.com/sirgallo/rdbv2/internal/utils"
)


//=========================================== Snapshot Service Handlers

/*
	Register Command Route
		path: /command
		method: POST

		request body:
			{
				action: "string",
				payload: {
					collection: "string",
					value: "string"
				}
			}

		response body:
			{
				collection: "string",
				key: "string" | nil,
				value: "string" | nil
			}

	ingest requests and pass from the HTTP Service to the replicated log service if leader,
	or the relay service if a follower.
		1.) append a both a unique identifier for the request as well as the current node that the request was sent to.
		2.) a channel for the request to be returned is created and mapped to the request id in the mapping of response channels
		2.) A context with timeout is initialized and the route either receives the response back and returns to the client,
			or the timeout is exceeded and failure is retuned to the client
*/

func (reqService *RequestService) RegisterCommandRoute() {
	handler := func(w http.ResponseWriter, r *http.Request) {
		var handlerErr error

		if r.Method == http.MethodPost { 
			if reqService.CurrentSystem.SysState == system.Leader {
				var requestData *state.StateOperation

				handlerErr = json.NewDecoder(r.Body).Decode(&requestData)
				if handlerErr != nil {
					http.Error(w, JSON_PARSE_ERROR, http.StatusBadRequest)
					return
				}

				var hash string
				hash, handlerErr = rdbUtils.GenerateRandomSHA256Hash()
				if handlerErr != nil {
					http.Error(w, REQUEST_HASH_ERROR, http.StatusBadRequest)
					return
				}

				clientResponseChannel := make(chan *state.StateResponse)
				reqService.ClientMappedResponseChannels.Store(hash, clientResponseChannel)
				requestData.RequestId = hash

				reqService.RequestChannel <- requestData
				responseData :=<- clientResponseChannel
				reqService.ClientMappedResponseChannels.Delete(hash)

				var responseJSON []byte
				responseJSON, handlerErr = json.Marshal(&state.StateResponse{ Collection: responseData.Collection, Key: responseData.Key, Value: responseData.Value })
				if handlerErr != nil {
					http.Error(w, ENCODING_ERROR, http.StatusInternalServerError)
					return
				}

				w.Header().Set("Content-Type", "application/json")
				w.Write(responseJSON)
			} else {
				redirectRequest := func() (bool, error) {
					if reqService.CurrentSystem.CurrentLeader != utils.GetZero[string]() {
						location := func () string { 
							return "http://" + reqService.CurrentSystem.CurrentLeader + reqService.Port + CommandRoute
						}()

						var parsedURL *url.URL
						parsedURL, handlerErr = url.Parse(location)
						if handlerErr != nil { return false, handlerErr }

						newReq := &http.Request{ Method: r.Method, URL: parsedURL, Header: r.Header.Clone(), Body: r.Body }
						client := &http.Client{}

						var resp *http.Response
						resp, handlerErr = client.Do(newReq)
						if handlerErr != nil { return false, handlerErr }
						
						defer resp.Body.Close()

						var responseBody []byte
						responseBody, handlerErr = io.ReadAll(resp.Body)
						if handlerErr != nil { return false, handlerErr }

						w.Header().Set("Content-Type", "application/json")
						w.Write(responseBody)
						return true, nil
					} else { return false, errors.New(NO_LEADER_ERROR) }
				}

				maxRetries := 5
				expOpts := utils.ExpBackoffOpts{ MaxRetries: &maxRetries, TimeoutInNanosecs: 50000000 }
				expBackoff := utils.NewExponentialBackoffStrat[bool](expOpts)
				_, handlerErr = expBackoff.PerformBackoff(redirectRequest)
				if handlerErr != nil { 
					http.Error(w, NO_LEADER_ERROR, http.StatusInternalServerError)
					return
				}
			}
		} else { http.Error(w, METHOD_NOT_ALLOWED, http.StatusMethodNotAllowed) }
	}

	reqService.Mux.HandleFunc(CommandRoute, handler)
}