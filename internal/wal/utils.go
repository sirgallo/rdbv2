package wal

import "encoding/binary"


//=========================================== Write Ahead Log Utils


/*
	Convert Int To Bytes
		used for creating the byte array keys from the entry index
*/

func ConvertIntToBytes(val int64) []byte {
	byteArray := make([]byte, 8) // 8 bytes for int64
	binary.BigEndian.PutUint64(byteArray, uint64(val))
	return byteArray
}

func ConvertBytesToInt(byteArray []byte) int64 {
	if len(byteArray) != 8 { return 0 }
	uintVal := binary.BigEndian.Uint64(byteArray)
	return int64(uintVal)
}