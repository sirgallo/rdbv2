package utils

import "bytes"
import "encoding/base64"
import "encoding/gob"
import "encoding/json"


//============================================= Encode/Decode JSON Utils


// EncodeStructToString
//	Encode a struct of type T to a string (json stringify)
//
// Parameters:
//	data: the data to encode
//
// Returns:
//	The encoded data
func EncodeStructToString [T comparable](data T) (string, error) {
	var buf bytes.Buffer
  enc := gob.NewEncoder(&buf)
	
	err := enc.Encode(data)
	if err != nil { return GetZero[string](), err }
	
	binaryData := buf.Bytes()
	base64String := base64.StdEncoding.EncodeToString(binaryData)
	return base64String, nil
}

// EncodeStructsToBytes
//	Encode a struct of type T to a string (json stringify)
//
// Parameters:
//	data: the data to encode
//
// Returns:
//	The encoded data
func EncodeStructToBytes [T comparable](data T) ([]byte, error) {
	var buf bytes.Buffer
  enc := gob.NewEncoder(&buf)
	
	err := enc.Encode(data)
	if err != nil { return nil, err }
	
	binaryData := buf.Bytes()

	return binaryData, nil
}

// DecodeStringToStruct
//	Decode a string to a struct of type T
//
// Parameters:
//	encoded: the encoded data
//
// Returns
//	The decoded data
func DecodeStringToStruct [T comparable](encoded string) (*T, error) {
	decodedBinaryData, binaryErr := base64.StdEncoding.DecodeString(encoded)
	if binaryErr != nil { return nil, binaryErr }
	
	decodedObj := new(T)
  dec := gob.NewDecoder(bytes.NewReader(decodedBinaryData))
  decErr := dec.Decode(&decodedObj)
	if decErr != nil { return nil, decErr }

	return decodedObj, nil
}

// DecodeBytesToStruct
//	Decode a byte array to a struct of type T
//
// Parameters:
//	encoded: the encoded data
//
// Returns:
//	The decoded data
func DecodeBytesToStruct [T comparable](encoded []byte) (*T, error) {
	decodedObj := new(T)
  dec := gob.NewDecoder(bytes.NewReader(encoded))
  decErr := dec.Decode(&decodedObj)
	if decErr != nil { return nil, decErr }

	return decodedObj, nil
}


// EncodeStructToJSONString
//	JSON stringify the incoming data
//
// Parameters:
//	data: the data to be stringified
//
// Returns:
//	The stringified representation of the data
func EncodeStructToJSONString [T comparable](data T) (string, error) {
	encoded, err := json.Marshal(data)
	if err != nil { return GetZero[string](), err }
	
	return string(encoded), nil
}

// DecodeJSONStringToStruct
//	Decode a json string back into a struct
//
// Parameters:
//	encoded: the json stringified representation of the data
//
// Returns:
//	The decoded struct from the json string
func DecodeJSONStringToStruct [T comparable](encoded string) (*T, error) {
	data := new(T)
	err := json.Unmarshal([]byte(encoded), data)
	if err != nil { return nil, err }
	
	return data, nil
}