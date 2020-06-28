package starlarkextensions

import (
	"strconv"

	"go.starlark.net/starlark"
	"go.starlark.net/starlarkstruct"
	time "github.com/qri-io/starlib/time"
	gotime "time"
)


// GetExtensions returns predeclared Starlark modules and functions to pass to configuration scripts
func GetExtensions() starlark.StringDict { 
	timeLoaded, _ := time.LoadModule()
	timeModule := timeLoaded["time"].(*starlarkstruct.Module)
	timeModule.Members["fromtimestamp"] = starlark.NewBuiltin("fromtimestamp", fromtimestamp)

	return starlark.StringDict{
		"dig": starlark.NewBuiltin("dig", dig),
		"time": timeModule,
	}
}

func dig(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (value starlark.Value, err error) {
	var found bool
	value = args[0]

	for _, arg := range args[1:] {
		switch value.(type) {
		case *starlark.Dict:
			value, found, err = value.(*starlark.Dict).Get(arg)
			if err != nil {
				return starlark.None, err
			}
			if !found {
				return starlark.None, nil
			}
		case *starlark.List:
			if arg.Type() != "int" {
				return starlark.None, nil
			}

			if idx, err := strconv.Atoi(arg.String()); err != nil {
				return starlark.None, err
			} else if idx >= value.(*starlark.List).Len() {
				return starlark.None, nil
			} else {
				value = value.(*starlark.List).Index(idx)
			}
		}
	}
	return
}

func fromtimestamp(thread *starlark.Thread, _ *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
	var (
		x starlark.Int
	)
	if err := starlark.UnpackArgs("time", args, kwargs, "x", &x); err != nil {
		return nil, err
	}

	i, err := strconv.ParseInt(x.String(), 10, 64)
	if err != nil {
		return nil, err
	}

	t := gotime.Unix(i, 0)

	return time.Time(t), nil
}