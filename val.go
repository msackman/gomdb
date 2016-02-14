package mdb

/*
#include <memory.h>
#include <stdlib.h>
#include <lmdb.h>
*/
import "C"

import (
	"reflect"
	"unsafe"
)

// MDB_val
type Val C.MDB_val

// Create a Val that points to p's data. the Val's data must not be freed
// manually and C references must not survive the garbage collection of p (and
// the returned Val).
func Wrap(p []byte) *Val {
	l := C.size_t(len(p))
	ptr := C.malloc(C.sizeof_MDB_val + l)
	val := (*C.MDB_val)(ptr)
	val.mv_size = l

	if l != 0 {
		bitesPtr := unsafe.Pointer(uintptr(ptr) + uintptr(C.sizeof_MDB_val))
		C.memcpy(bitesPtr, unsafe.Pointer(&p[0]), l)
		val.mv_data = bitesPtr
	}
	return (*Val)(val)
}

func (v *Val) Free() {
	C.free(unsafe.Pointer(v))
}

// If val is nil, a empty slice is retured.
func (v *Val) Bytes() []byte {
	return C.GoBytes(v.mv_data, C.int(v.mv_size))
}

func (v *Val) BytesNoCopy() []byte {
	hdr := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(v.mv_data)),
		Len:  int(v.mv_size),
		Cap:  int(v.mv_size),
	}
	return *(*[]byte)(unsafe.Pointer(&hdr))
}

// If val is nil, an empty string is returned.
func (v *Val) String() string {
	return C.GoStringN((*C.char)(v.mv_data), C.int(v.mv_size))
}
