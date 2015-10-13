package lmdbcmd

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

var flagPrintVersion bool
var flagOpenNoSubDir bool

func init() {
	flag.BoolVar(&flagPrintVersion, "V", false, "Write the library version number to the standard output, and exit.")
	flag.BoolVar(&flagOpenNoSubDir, "n", false, "Open LDMB environment(s) which do not use subdirectories.")
}

func printVersion(w io.Writer) {
	_, _, _, version := lmdb.Version()
	fmt.Fprintln(w, version)
}

func PrintVersion() {
	if flagPrintVersion {
		printVersion(os.Stdout)
		os.Exit(0)
	}
}

func OpenFlag() uint {
	var flag uint
	if flagOpenNoSubDir {
		flag |= lmdb.NoSubdir
	}
	return flag
}
