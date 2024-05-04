package stats

import "github.com/sirgallo/utils"


func EncodeStatObjectToBytes(statObj Stats) ([]byte, error) {
	value, encErr := utils.EncodeStructToBytes[Stats](statObj)
	if encErr != nil { return nil, encErr }
	return value, nil
}

func DecodeBytesToStatObject(statAsBytes []byte) (*Stats, error) {
	stats, decodeErr := utils.DecodeBytesToStruct[Stats](statAsBytes)
	if decodeErr != nil { return nil, decodeErr }
	return stats, nil
}