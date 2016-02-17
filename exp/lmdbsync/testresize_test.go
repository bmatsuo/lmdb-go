package lmdbsync

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/bmatsuo/lmdb-go/lmdb"
)

func TestResize(t *testing.T) {
	tempdir, err := ioutil.TempDir("", "lmdbsync_testresize")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempdir)

	dbpath := filepath.Join(tempdir, "db")
	err = os.Mkdir(dbpath, 0755)
	if err != nil {
		t.Error(err)
		return
	}

	env, err := NewEnv(nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer env.Close()
	err = env.Open(dbpath, 0, 0644)
	if err != nil {
		t.Error(err)
		return
	}

	var root lmdb.DBI
	env.Update(func(txn *lmdb.Txn) (err error) {
		root, err = txn.OpenRoot(0)
		if err != nil {
			return err
		}
		return txn.Put(root, []byte("_start"), []byte(time.Now().String()), 0)
	})

	before, err := env.Info()
	if err != nil {
		t.Error(err)
		return
	}

	bin := filepath.Join(tempdir, "testresize")
	build := exec.Command("go", "build", "-o", bin, "./testresize")
	build.Stderr = os.Stderr
	err = build.Run()
	if err != nil {
		t.Error(err)
		return
	}

	r1, w1, err := os.Pipe()
	if err != nil {
		t.Error(err)
		return
	}
	r2, w2, err := os.Pipe()
	if err != nil {
		t.Error(err)
		return
	}
	closePipes := func() {
		w1.Close()
		w2.Close()
		r1.Close()
		r2.Close()
	}

	cmd1 := exec.Command(bin)
	cmd1.Dir = tempdir
	cmd2 := exec.Command(bin)
	cmd2.Dir = tempdir

	cmd1.Stdin = r1
	cmd2.Stdout = w1

	cmd2.Stdin = r2
	cmd1.Stdout = w2

	stderr1, err := cmd1.StderrPipe()
	if err != nil {
		t.Error(err)
		closePipes()
		return
	}
	stderr2, err := cmd2.StderrPipe()
	if err != nil {
		t.Error(err)
		closePipes()
		return
	}
	stderr := io.MultiReader(stderr1, stderr2)

	err = cmd1.Start()
	if err != nil {
		t.Errorf("start 1: %v", err)
		closePipes()
		return
	}
	err = cmd2.Start()
	if err != nil {
		t.Errorf("start 2: %v", err)
		closePipes()
		return
	}
	go fmt.Fprintln(w1, "start")

	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		t.Log(scanner.Text())
	}
	closePipes()

	err1 := cmd1.Wait()
	if err1 != nil {
		t.Errorf("err 1: %v", err1)
	}
	err2 := cmd2.Wait()
	if err2 != nil {
		t.Errorf("err 2: %v", err2)
	}

	trace := &resizeTracer{}
	runner := env.WithHandler(HandlerChain{
		trace,
		MapResizedHandler(2, func(retry int) time.Duration {
			if retry > 0 {
				t.Errorf("failed to reopen at %d times", retry)
			}
			return time.Millisecond
		}),
	})
	err = runner.Update(func(txn *lmdb.Txn) (err error) {
		return txn.Put(root, []byte("_finish"), []byte(time.Now().String()), 0)
	})
	if err != nil {
		t.Error(err)
	}

	if trace.resized == 0 {
		t.Errorf("no resize detected")
	}

	after, err := env.Info()
	if err != nil {
		t.Error(err)
		return
	}
	if after.MapSize <= before.MapSize {
		t.Errorf("mapsize: %d (<= %d)", after.MapSize, before.MapSize)
	}
}

type resizeTracer struct {
	resized int
}

func (t *resizeTracer) HandleTxnErr(c context.Context, env *Env, err error) (context.Context, error) {
	if lmdb.IsMapResized(err) {
		t.resized++
	}
	return c, err
}
