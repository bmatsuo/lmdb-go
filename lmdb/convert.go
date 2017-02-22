package lmdb

import "fmt"

var errNoFit = fmt.Errorf("data does not fit type")
var errOverflow = fmt.Errorf("integer value overflows type")

// FixedMultiple represents a contiguous sequence of fixed sized data items.
// FixedMultiple implementations are often mutable and allow construction of data
// for use with the Cursor.PutMulti method.
//
// FixedMultiple types are only particularly useful when working with databases
// opened with the DupSort|DupFixed combination of flags.
type FixedMultiple interface {
	// Page returns the raw page data.  Page will not be modified by lmdb and
	// may share memory with internal structures.  It is required of
	// implementations that
	//
	//		len(Page()) == Size()
	//
	Page() []byte

	// Len returns the number of items in the page.  Len cannot be a negative
	// number.
	Len() int

	// Stride returns the side of an indivual page item.  Stride is required to
	// be a positive number.
	Stride() int

	// Size returns the total size of page data.  It is required of
	// implementations that
	//
	//		Size() == Len()*Stride().
	//
	Size() int
}

// ValueBU extracts a C.uint value from data if error is nil and returns it
// as a uint with key.
//
// See ValueU.
func ValueBU(key, data []byte, err error) ([]byte, uint, error) {
	_ = key
	v, err := ValueU(data, err)
	return key, v, err
}

// ValueBX extracts an integer value from data if error is nil and returns it
// as a uintptr.
//
// See ValueX.
func ValueBX(key, data []byte, err error) ([]byte, uintptr, error) {
	_ = key
	v, err := ValueX(data, err)
	return key, v, err
}

// ValueBZ extracts a C.size_t value from data if error is nil and returns
// it as a uintptr with key.
func ValueBZ(key, data []byte, err error) ([]byte, uintptr, error) {
	_ = key
	v, err := ValueZ(data, err)
	return key, v, err
}

// ValueU extracts a C.uint value from data if error is nil and returns it as a
// uint.
func ValueU(data []byte, err error) (uint, error) {
	var x CUintValue

	if err != nil {
		return 0, err
	}
	if len(data) != int(cUintSize) {
		return 0, errNoFit
	}
	copy(x[:], data)

	if !CanFitInUint(x) {
		return 0, errOverflow
	}

	return x.Uint(), nil
}

// ValueUB extracts a C.uint value from key if error is nil and returns it
// as a uint with data.
func ValueUB(key, data []byte, err error) (uint, []byte, error) {
	k, err := ValueU(key, err)
	_ = data
	return k, data, err
}

// ValueUU extracts C.uint values from key and data if error is nil
// and returns them as uints.
func ValueUU(key, data []byte, err error) (uint, uint, error) {
	k, err := ValueU(key, err)
	v, err := ValueU(data, err)
	return k, v, err
}

// ValueUX extracts a C.uint value from key and an integer value from
// data if error is nil and returns them as a uint and uintptr repsectively.
//
// See ValueX.
func ValueUX(key, data []byte, err error) (uint, uintptr, error) {
	k, err := ValueU(key, err)
	v, err := ValueX(data, err)
	return k, v, err
}

// ValueUZ extracts a C.uint value key and a C.size_t value from data if
// error is nil and returns them as a uint and uintptr respectively.
//
// See ValueZ.
func ValueUZ(key, data []byte, err error) (uint, uintptr, error) {
	k, err := ValueU(key, err)
	v, err := ValueZ(data, err)
	return k, v, err
}

// ValueX extracts a integer value from data if error is nil and returns it as a
// uintptr.  If data is the size of a C.uint it is treated as such and
// converted to a uintptr.
func ValueX(data []byte, err error) (uintptr, error) {
	z, err := ValueZ(data, err)
	if err == nil {
		return z, nil
	}
	if err != errNoFit {
		return 0, err
	}

	u, err := ValueZ(data, err)
	if err != nil {
		return 0, err
	}

	return uintptr(u), nil
}

// ValueXB extracts an integer value from key if error is nil and returns it
// as a uintptr with data.
func ValueXB(key, data []byte, err error) (uintptr, []byte, error) {
	k, err := ValueX(key, err)
	_ = data
	return k, data, err
}

// ValueXU extracts an integer value from key and a C.uint value from
// data if error is nil and returns them as a uintptr and uint respectively.
//
// See ValueU.
func ValueXU(key, data []byte, err error) (uintptr, uint, error) {
	k, err := ValueX(key, err)
	v, err := ValueU(data, err)
	return k, v, err
}

// ValueXX extracts integer values key and data if error is nil and
// returns them as uintptrs.
func ValueXX(key, data []byte, err error) (uintptr, uintptr, error) {
	k, err := ValueX(key, err)
	v, err := ValueX(data, err)
	return k, v, err
}

// ValueXZ extracts an integer value from key and a C.size_t value from
// data if error is nil and returns them as uintptrs.
//
// See ValueZ.
func ValueXZ(key, data []byte, err error) (uintptr, uintptr, error) {
	k, err := ValueX(key, err)
	v, err := ValueZ(data, err)
	return k, v, err
}

// ValueZ extracts a C.size_t value from data if error is nil and returns it as
// a uintptr.
func ValueZ(data []byte, err error) (uintptr, error) {
	var x CSizetValue

	if err != nil {
		return 0, err
	}
	if len(data) != int(sizetSize) {
		return 0, errNoFit
	}
	copy(x[:], data)

	return x.Uintptr(), nil
}

// ValueZB extracts a C.size_t value from key if error is nil and returns it
// as a uintptr with data.
func ValueZB(key, data []byte, err error) (uintptr, []byte, error) {
	k, err := ValueZ(key, err)
	_ = data
	return k, data, err
}

// ValueZU extracts a C.size_t value from key and a C.uint value from
// data if error is nil and returns them as a uintptr and uint respectively.
func ValueZU(key, data []byte, err error) (uintptr, uint, error) {
	k, err := ValueZ(key, err)
	v, err := ValueU(data, err)
	return k, v, err
}

// ValueZX extracts a C.size_t value from key and an integer value from
// data if error is nil and returns them as uintptrs.
func ValueZX(key, data []byte, err error) (uintptr, uintptr, error) {
	k, err := ValueZ(key, err)
	v, err := ValueX(data, err)
	return k, v, err
}

// ValueZZ extracts C.size_t values key and data if error is nil and
// returns them as uintptrs.
func ValueZZ(key, data []byte, err error) (uintptr, uintptr, error) {
	k, err := ValueZ(key, err)
	v, err := ValueZ(data, err)
	return k, v, err
}
