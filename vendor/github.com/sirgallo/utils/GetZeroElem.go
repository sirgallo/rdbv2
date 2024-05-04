package utils



//============================================= Get Zero Elem


// GetZero
//	Get the null type for any type T
//
// Returns:
//	Get the null element
func GetZero [T comparable]() T {
	var result T
	return result
}