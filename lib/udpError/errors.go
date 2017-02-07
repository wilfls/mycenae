package udpError

import (
	"errors"
	"net/http"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/tserr"
)

func errBasic(f, s string, code int, e error) gobol.Error {
	if e != nil {
		return tserr.New(
			e,
			s,
			code,
			map[string]interface{}{
				"package": "udpError",
				"func":    f,
			},
		)
	}
	return nil
}

func errNotFound(f string) gobol.Error {
	return errBasic(f, "", http.StatusNotFound, errors.New(""))
}

func errPersist(f string, e error) gobol.Error {
	return errBasic(f, e.Error(), http.StatusInternalServerError, e)
}

func errParamSize(f string, e error) gobol.Error {
	return errBasic(f, `query param "size" should be an integer number greater than zero`, http.StatusBadRequest, e)
}

func errParamFrom(f string, e error) gobol.Error {
	return errBasic(f, `query param "from" should be an integer number greater or equals zero`, http.StatusBadRequest, e)
}

func errNoContent(f string) gobol.Error {
	return errBasic(f, "", http.StatusNoContent, errors.New(""))
}
