package array

import "errors"


//============================================= Array Utils


// Chunk 
//	Pass in an array of type T with a specified chunk size, and return a nested array.
//	Each entry is an array of elements for the the chunk size, or the remaining elements.
//
// Parameters:
//	array: the array to be chunked
//	chunkSize: the size each chunk should be 
//
// Returns:
//	The newly chunked array
func Chunk [T comparable](array[]T, chunkSize int) ([][]T, error) {
	if chunkSize <= 0 { return nil, errors.New("chunk size needs to be greater than 0") }
	
	var chunks [][]T
	
	if (len(array) <= chunkSize) { 
		return append(chunks, array), nil
	} else {
		totalChunks := (len(array) / chunkSize) + 1

		for idx := range make([]int, totalChunks - 1) {
			start := idx * chunkSize
			end := (idx + 1) * chunkSize
			chunks = append(chunks, array[start:end])
		}

		startOfRemainder := (totalChunks - 1) * chunkSize
		
		if startOfRemainder < len(array) { return append(chunks, array[startOfRemainder:]), nil }
		return chunks, nil
	} 
}

// Filter 
//	Pass in an array of type T and a condition. 
//	If the element passes the condition, return it in the new array slice
// 
// Parameters:
//	array: the array to be filtered
//	condition: the condition the array should be filtered on
//
// Returns:
//	The filtered array
func Filter [T comparable](array []T, condition func(T) bool) []T {
	var filtered []T
	for _, elem := range array {
		if condition(elem) { filtered = append(filtered, elem) }
	}

	return filtered
}

// Map
//	pass in an array of type T and a tranformer function. 
//	For each element of the array, apply the transformer and return an array of type V, or all of the transformed elements.
//
// Parameters:
//	array: the array to be mapped
//	transform: the map function for transforming elements
//
// Returns:
//	The mapped array
func Map [T any, V any](array []T, transform func(T) V) []V {
	var mapped []V
	for _, elem := range array {
		mapped = append(mapped, transform(elem))
	}

	return mapped
}