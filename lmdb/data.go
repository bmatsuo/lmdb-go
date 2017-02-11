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

// Data is a container for data that can be written to an LMDB environment.
//
// Data types are only required when working with databases opened with the
// IntegerKey or IntegerDup flags.  Using Data types in other situations will
// only hurt the performance of your application.
//
// Data is a controlled interface that cannot be implemented by external
// types.  The only implementations of Data are BytesData, UintData, and
// UintptrData.
type Data interface {
	// tobytes is currently used to create a value which can be written to the
	// database.  But in the future this may not always be the case. In
	// particular, the github issue golang/go#6907 should allow the
	// implementation of a StringData type and it may require special handling
	// all the way to the cgo call site.  If that were the case a StringData
	// type would still provide a tobytes implementation but it would
	// potentially need to be bypassed using type assertions depending on the
	// end implementation of #6907.
	tobytes() []byte
}

func dataToBytes(v Data) []byte {
	if v == nil {
		return nil
	}
	return v.tobytes()
}

// DataBU extracts a C.uint value from valdata if error is nil and returns it
// as a uint with keydata.
//
// See DataU.
func DataBU(keydata, valdata []byte, err error) ([]byte, uint, error) {
	_ = keydata
	v, err := DataU(valdata, err)
	return keydata, v, err
}

// DataBX extracts an integer value from valdata if error is nil and returns it
// as a uintptr.
//
// See DataX.
func DataBX(keydata, valdata []byte, err error) ([]byte, uintptr, error) {
	_ = keydata
	v, err := DataX(valdata, err)
	return keydata, v, err
}

// DataBZ extracts a C.size_t value from valdata if error is nil and returns
// it as a uintptr with keydata.
func DataBZ(keydata, valdata []byte, err error) ([]byte, uintptr, error) {
	_ = keydata
	v, err := DataZ(valdata, err)
	return keydata, v, err
}

// DataU extracts a C.uint value from data if error is nil and returns it as a
// uint.
func DataU(data []byte, err error) (uint, error) {
	if err != nil {
		return 0, err
	}
	if len(data) != int(uintSize) {
		return 0, errNoFit
	}
	x, ok := getUint(data)
	if !ok {
		return 0, errOverflow
	}
	return x, nil
}

// DataUB extracts a C.uint value from keydata if error is nil and returns it
// as a uint with valdata.
func DataUB(keydata, valdata []byte, err error) (uint, []byte, error) {
	k, err := DataU(keydata, err)
	_ = valdata
	return k, valdata, err
}

// DataUU extracts C.uint values from keydata and valdata if error is nil
// and returns them as uints.
func DataUU(keydata, valdata []byte, err error) (uint, uint, error) {
	k, err := DataU(keydata, err)
	v, err := DataU(valdata, err)
	return k, v, err
}

// DataUX extracts a C.uint value from keydata and an integer value from
// valdata if error is nil and returns them as a uint and uintptr repsectively.
//
// See DataX.
func DataUX(keydata, valdata []byte, err error) (uint, uintptr, error) {
	k, err := DataU(keydata, err)
	v, err := DataX(valdata, err)
	return k, v, err
}

// DataUZ extracts a C.uint value keydata and a C.size_t value from valdata if
// error is nil and returns them as a uint and uintptr respectively.
//
// See DataZ.
func DataUZ(keydata, valdata []byte, err error) (uint, uintptr, error) {
	k, err := DataU(keydata, err)
	v, err := DataZ(valdata, err)
	return k, v, err
}

// DataX extracts a integer value from data if error is nil and returns it as a
// uintptr.  If data is the size of a C.uint it is treated as such and
// converted to a uintptr.
func DataX(data []byte, err error) (uintptr, error) {
	if err != nil {
		return 0, err
	}
	if len(data) == int(uintSize) {
		x, ok := getUint(data)
		if !ok {
			return 0, errOverflow
		}
		return uintptr(x), nil
	}
	if len(data) == int(sizetSize) {
		x, ok := getUintptr(data)
		if !ok {
			return 0, errOverflow
		}
		return x, nil
	}
	return 0, errNoFit
}

// DataXB extracts an integer value from keydata if error is nil and returns it
// as a uintptr with valdata.
func DataXB(keydata, valdata []byte, err error) (uintptr, []byte, error) {
	k, err := DataX(keydata, err)
	_ = valdata
	return k, valdata, err
}

// DataXU extracts an integer value from keydata and a C.uint value from
// valdata if error is nil and returns them as a uintptr and uint respectively.
//
// See DataU.
func DataXU(keydata, valdata []byte, err error) (uintptr, uint, error) {
	k, err := DataX(keydata, err)
	v, err := DataU(valdata, err)
	return k, v, err
}

// DataXX extracts integer values keydata and valdata if error is nil and
// returns them as uintptrs.
func DataXX(keydata, valdata []byte, err error) (uintptr, uintptr, error) {
	k, err := DataX(keydata, err)
	v, err := DataX(valdata, err)
	return k, v, err
}

// DataXZ extracts an integer value from keydata and a C.size_t value from
// valdata if error is nil and returns them as uintptrs.
//
// See DataZ.
func DataXZ(keydata, valdata []byte, err error) (uintptr, uintptr, error) {
	k, err := DataX(keydata, err)
	v, err := DataZ(valdata, err)
	return k, v, err
}

// DataZ extracts a C.size_t value from data if error is nil and returns it as
// a uintptr.
func DataZ(data []byte, err error) (uintptr, error) {
	if err != nil {
		return 0, err
	}
	if len(data) != int(sizetSize) {
		return 0, errNoFit
	}
	x, ok := getUintptr(data)
	if !ok {
		return 0, errOverflow
	}
	return x, nil
}

// DataZB extracts a C.size_t value from keydata if error is nil and returns it
// as a uintptr with valdata.
func DataZB(keydata, valdata []byte, err error) (uintptr, []byte, error) {
	k, err := DataZ(keydata, err)
	_ = valdata
	return k, valdata, err
}

// DataZU extracts a C.size_t value from keydata and a C.uint value from
// valdata if error is nil and returns them as a uintptr and uint respectively.
func DataZU(keydata, valdata []byte, err error) (uintptr, uint, error) {
	k, err := DataZ(keydata, err)
	v, err := DataU(valdata, err)
	return k, v, err
}

// DataZX extracts a C.size_t value from keydata and an integer value from
// valdata if error is nil and returns them as uintptrs.
func DataZX(keydata, valdata []byte, err error) (uintptr, uintptr, error) {
	k, err := DataZ(keydata, err)
	v, err := DataX(valdata, err)
	return k, v, err
}

// DataZZ extracts C.size_t values keydata and valdata if error is nil and
// returns them as uintptrs.
func DataZZ(keydata, valdata []byte, err error) (uintptr, uintptr, error) {
	k, err := DataZ(keydata, err)
	v, err := DataZ(valdata, err)
	return k, v, err
}

// Bytes returns a Data containg b.  The returned value shares is memory with
// b and b must not be modified while it is use.
func Bytes(b []byte) BytesData {
	return BytesData(b)
}

// BytesData is a Data that contains arbitrary data.
type BytesData []byte

var _ Data = BytesData(nil)

func (v BytesData) tobytes() []byte {
	return []byte(v)
}
