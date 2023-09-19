package utils

func Pointer[T any](val T) *T {
	return &val
}

func Value[T any](val *T) T {
	return *val
}
