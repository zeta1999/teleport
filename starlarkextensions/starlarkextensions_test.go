package starlarkextensions

import (
	"testing"

	"github.com/hundredwatt/teleport/starlarkextensions"
	"github.com/qri-io/starlib/testdata"
	"go.starlark.net/resolve"
	"go.starlark.net/starlark"
	"go.starlark.net/starlarktest"
)

func TestFile(t *testing.T) {
	resolve.AllowFloat = true
	nullLoader := func() (starlark.StringDict, error) { return starlark.StringDict{}, nil }
	thread := &starlark.Thread{Load: testdata.NewLoader(nullLoader, "null.star")}
	starlarktest.SetReporter(thread, t)

	// Execute test files
	_, err := starlark.ExecFile(thread, "testdata/dig.star", nil, starlarkextensions.GetExtensions())
	if err != nil {
		t.Error(err)
	}

	_, err = starlark.ExecFile(thread, "testdata/time.star", nil, starlarkextensions.GetExtensions())
	if err != nil {
		t.Error(err)
  }
}
