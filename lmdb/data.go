package lmdb

// FixedPage represents a contiguous sequence of fixed sized data items.
// FixedPage implementations are often mutable and allow construction of data
// for use with the Cursor.PutMulti method.
//
// FixedPage types are only particularly useful when working with databases
// opened with the DupSort|DupFixed combination of flags.
type FixedPage interface {
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
	tobytes() []byte
}

func dataToBytes(v Data) []byte {
	if v == nil {
		return nil
	}
	return v.tobytes()
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
