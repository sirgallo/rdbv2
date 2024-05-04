package utils

import "strconv"


//=========================================== Server Utils


/*
	normal port --> take a port and map the string to start net service
*/

func NormalizePort(port int) string {
	return ":" + strconv.Itoa(port)
}