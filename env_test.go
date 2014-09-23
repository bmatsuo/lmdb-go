package lmdb

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestEnvPathNoOpen(t *testing.T) {
	env, err := NewEnv()
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// before Open the Path method returns "" and a non-nil error.
	path, err := env.Path()
	if err == nil {
		t.Errorf("no error returned before Open")
	}
	if path != "" {
		t.Errorf("non-zero path returned before Open")
	}
}

func TestEnvPath(t *testing.T) {
	env, err := NewEnv()
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// open an environment
	dir, err := ioutil.TempDir("", "mdb_test")
	if err != nil {
		t.Fatalf("tempdir: %v", err)
	}
	defer os.RemoveAll(dir)

	err = env.Open(dir, 0, 0644)
	if err != nil {
		env.Close()
		t.Fatalf("open: %v", err)
	}
	path, err := env.Path()
	if err != nil {
		t.Errorf("path: %v", err)
	}
	if path != dir {
		t.Errorf("path: %q (!= %q)", path, dir)
	}

	err = env.Close()
	if err != nil {
		t.Errorf("close: %v", err)
	}
}

func TestEnvOpen(t *testing.T) {
	env, err := NewEnv()
	if err != nil {
		t.Fatalf("create: %s", err)
	}

	// ensure that opening a non-existent path fails.
	err = env.Open("adsjgfadsfjg", 0, 0664)
	if err == nil {
		t.Errorf("open: must exist")
	}
	if !os.IsNotExist(err) {
		t.Errorf("open: %v", err)
	}

	// open an environment at a temporary path.
	path, err := ioutil.TempDir("/tmp", "mdb_test")
	if err != nil {
		t.Fatalf("tempdir: %v", err)
	}
	err = os.MkdirAll(path, 0770)
	if err != nil {
		t.Fatalf("mkdir: %s", path)
	}
	err = env.Open(path, 0, 0664)
	if err != nil {
		t.Errorf("open: %s", err)
	}

	// close the environment and remove the environment.
	err = env.Close()
	if err != nil {
		t.Errorf("close: %s", err)
	}
	err = os.RemoveAll(path)
	if err != nil {
		t.Errorf("remove: %v", err)
	}
}

func setup(t *testing.T) *Env {
	env, err := NewEnv()
	if err != nil {
		t.Errorf("Cannot create enviroment: %s", err)
	}
	path, err := ioutil.TempDir("/tmp", "mdb_test")
	if err != nil {
		t.Errorf("Cannot create temporary directory")
	}
	err = os.MkdirAll(path, 0770)
	if err != nil {
		t.Errorf("Cannot create directory: %s", path)
	}
	err = env.Open(path, 0, 0664)
	if err != nil {
		t.Errorf("Cannot open environment: %s", err)
	}

	return env
}

func clean(env *Env, t *testing.T) {
	path, err := env.Path()
	if err != nil {
		t.Errorf("path: %v", err)
	}
	err = env.Close()
	if err != nil {
		t.Errorf("close: %s", err)
	}
	if path != "" {
		err = os.RemoveAll(path)
		if err != nil {
			t.Errorf("remove: %v", err)
		}
	}
}

func TestEnvCopy(t *testing.T) {
	env := setup(t)
	defer clean(env, t)
}
