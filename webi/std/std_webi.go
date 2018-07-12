package webi

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/cookiejar"
	"net/url"

	"context"

	"github.com/noypi/gae"
	"github.com/noypi/gae/registry"
	"google.golang.org/appengine/urlfetch"
)

const Name = "standard"

const cGetRepeat = 5
const cTimeout = 60

type Webi struct {
	client      *http.Client
	cloudClient *http.Client
	c           context.Context
	logger      gae.LogInt
}

// required params:
//    c context.Context
//   logger gae.LogInt
func New(params map[string]interface{}) (gae.WebInt, error) {
	c, has := params[gae.WebiContext].(context.Context)
	if !has {
		return nil, fmt.Errorf("Error: invalid params, context needed")
	}
	logger, has := params[gae.WebiLogger].(gae.LogInt)
	if !has {
		return nil, fmt.Errorf("Error: invalid params, logger needed")
	}
	jar, _ := params[gae.WebiJar].(*cookiejar.Jar)
	return newWebi(c, logger, jar), nil
}

func newWebi(c context.Context, logger gae.LogInt, jar *cookiejar.Jar) (w *Webi) {
	w = new(Webi)
	w.c = c
	w.logger = logger

	w.client = urlfetch.Client(c)
	w.client.Transport = &urlfetch.Transport{Context: c}
	if nil == jar {
		jar, _ = cookiejar.New(nil)
	}
	w.client.Jar = jar

	return
}

func (this *Webi) Client() *http.Client {
	return this.client
}

func (this Webi) Get(s string, v *url.Values) (bb []byte, err error) {
	for i := 0; i < cGetRepeat; i++ {
		if bb, err = this.get(s, v); nil == err {
			break
		}
	}

	return
}

func (this Webi) get(s string, v *url.Values) (bb []byte, err error) {

	if nil != v {
		s = s + "?" + v.Encode()
	}
	req, _ := http.NewRequest("GET", s, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36")
	this.logger.Infof("Webi Get url=%s\n", s)

	res, err := this.client.Do(req)
	if nil != err {
		this.logger.Debugf("Webi Get err=%v\n", err)
		return
	}
	defer res.Body.Close()

	if http.StatusOK != res.StatusCode {
		err = errors.New(res.Status)
		return
	}

	bb, err = ioutil.ReadAll(res.Body)

	return
}

func (this Webi) Post(s string, v *url.Values) (bb []byte, err error) {

	//req, _ := http.NewRequest("POST", s, nil)
	//req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36")

	res, err := this.client.PostForm(s, *v)
	if nil != err {
		return
	}
	defer res.Body.Close()

	bb, err = ioutil.ReadAll(res.Body)

	return
}

func init() {
	registry.RegisterWebi(Name, New)
}
