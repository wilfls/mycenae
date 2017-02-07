package bcache

import (
	"net/http"

	"github.com/uol/gobol"

	"github.com/uol/mycenae/lib/tserr"
)

func errBasic(f, s string, e error) gobol.Error {
	if e != nil {
		return tserr.New(
			e,
			s,
			http.StatusInternalServerError,
			map[string]interface{}{
				"package": "bcache/persistence",
				"func":    f,
			},
		)
	}
	return nil
}

func errPersist(f string, e error) gobol.Error {
	return errBasic(f, e.Error(), e)
}
