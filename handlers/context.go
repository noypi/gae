package handlers

import (
	"net/http"

	"github.com/noypi/gae"
	"github.com/noypi/router"
	"google.golang.org/appengine"
)

func AddGAEContext(w http.ResponseWriter, r *http.Request) {
	c := router.ContextW(w)
	gae.PutGAEContext(c, appengine.NewContext(r))
}
