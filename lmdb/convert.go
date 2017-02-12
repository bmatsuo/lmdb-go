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

// ValueBU extracts a C.uint value from valdata if error is nil and returns it
// as a uint with keydata.
//
// See ValueU.
func ValueBU(keydata, valdata []byte, err error) ([]byte, uint, error) {
	_ = keydata
	v, err := ValueU(valdata, err)
	return keydata, v, err
}

// ValueBX extracts an integer value from valdata if error is nil and returns it
// as a uintptr.
//
// See ValueX.
func ValueBX(keydata, valdata []byte, err error) ([]byte, uintptr, error) {
	_ = keydata
	v, err := ValueX(valdata, err)
	return keydata, v, err
}

// ValueBZ extracts a C.size_t value from valdata if error is nil and returns
// it as a uintptr with keydata.
func ValueBZ(keydata, valdata []byte, err error) ([]byte, uintptr, error) {
	_ = keydata
	v, err := ValueZ(valdata, err)
	return keydata, v, err
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

// ValueUB extracts a C.uint value from keydata if error is nil and returns it
// as a uint with valdata.
func ValueUB(keydata, valdata []byte, err error) (uint, []byte, error) {
	k, err := ValueU(keydata, err)
	_ = valdata
	return k, valdata, err
}

// ValueUU extracts C.uint values from keydata and valdata if error is nil
// and returns them as uints.
func ValueUU(keydata, valdata []byte, err error) (uint, uint, error) {
	k, err := ValueU(keydata, err)
	v, err := ValueU(valdata, err)
	return k, v, err
}

// ValueUX extracts a C.uint value from keydata and an integer value from
// valdata if error is nil and returns them as a uint and uintptr repsectively.
//
// See ValueX.
func ValueUX(keydata, valdata []byte, err error) (uint, uintptr, error) {
	k, err := ValueU(keydata, err)
	v, err := ValueX(valdata, err)
	return k, v, err
}

// ValueUZ extracts a C.uint value keydata and a C.size_t value from valdata if
// error is nil and returns them as a uint and uintptr respectively.
//
// See ValueZ.
func ValueUZ(keydata, valdata []byte, err error) (uint, uintptr, error) {
	k, err := ValueU(keydata, err)
	v, err := ValueZ(valdata, err)
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

// ValueXB extracts an integer value from keydata if error is nil and returns it
// as a uintptr with valdata.
func ValueXB(keydata, valdata []byte, err error) (uintptr, []byte, error) {
	k, err := ValueX(keydata, err)
	_ = valdata
	return k, valdata, err
}

// ValueXU extracts an integer value from keydata and a C.uint value from
// valdata if error is nil and returns them as a uintptr and uint respectively.
//
// See ValueU.
func ValueXU(keydata, valdata []byte, err error) (uintptr, uint, error) {
	k, err := ValueX(keydata, err)
	v, err := ValueU(valdata, err)
	return k, v, err
}

// ValueXX extracts integer values keydata and valdata if error is nil and
// returns them as uintptrs.
func ValueXX(keydata, valdata []byte, err error) (uintptr, uintptr, error) {
	k, err := ValueX(keydata, err)
	v, err := ValueX(valdata, err)
	return k, v, err
}

// ValueXZ extracts an integer value from keydata and a C.size_t value from
// valdata if error is nil and returns them as uintptrs.
//
// See ValueZ.
func ValueXZ(keydata, valdata []byte, err error) (uintptr, uintptr, error) {
	k, err := ValueX(keydata, err)
	v, err := ValueZ(valdata, err)
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

// ValueZB extracts a C.size_t value from keydata if error is nil and returns it
// as a uintptr with valdata.
func ValueZB(keydata, valdata []byte, err error) (uintptr, []byte, error) {
	k, err := ValueZ(keydata, err)
	_ = valdata
	return k, valdata, err
}

// ValueZU extracts a C.size_t value from keydata and a C.uint value from
// valdata if error is nil and returns them as a uintptr and uint respectively.
func ValueZU(keydata, valdata []byte, err error) (uintptr, uint, error) {
	k, err := ValueZ(keydata, err)
	v, err := ValueU(valdata, err)
	return k, v, err
}

// ValueZX extracts a C.size_t value from keydata and an integer value from
// valdata if error is nil and returns them as uintptrs.
func ValueZX(keydata, valdata []byte, err error) (uintptr, uintptr, error) {
	k, err := ValueZ(keydata, err)
	v, err := ValueX(valdata, err)
	return k, v, err
}

// ValueZZ extracts C.size_t values keydata and valdata if error is nil and
// returns them as uintptrs.
func ValueZZ(keydata, valdata []byte, err error) (uintptr, uintptr, error) {
	k, err := ValueZ(keydata, err)
	v, err := ValueZ(valdata, err)
	return k, v, err
}
