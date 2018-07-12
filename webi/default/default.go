package pse

import (
	"errors"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"time"

	"github.com/noypi/gae"
)

const cWebGetTimeout = time.Duration(2 * time.Minute)
const cWebGetRepeat = 3

func dialTimeout(network, addr string) (net.Conn, error) {
	return net.DialTimeout(network, addr, cWebGetTimeout)
}

type WebIntDefault struct {
	client *http.Client
}

func NewWebiDefault() gae.WebInt {
	w := new(WebIntDefault)
	jar, _ := cookiejar.New(nil)
	transport := http.Transport{
		Dial: dialTimeout,
	}
	w.client = &http.Client{
		Jar:       jar,
		Transport: &transport,
	}
	return w
}

func (this WebIntDefault) Client() *http.Client {
	return this.client
}

func (this WebIntDefault) Get(s string, v *url.Values) (bb []byte, err error) {
	for i := 0; i < cWebGetRepeat; i++ {
		if bb, err = this.get(s, v); nil == err {
			break
		}
	}

	return
}

func (this WebIntDefault) get(s string, v *url.Values) (bb []byte, err error) {
	if nil != v {
		s = s + "?" + v.Encode()
	}
	req, _ := http.NewRequest("GET", s, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36")
	//log.Println("Get url=", s)

	res, err := this.client.Do(req)
	if nil != err {
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

func (this WebIntDefault) Post(s string, v *url.Values) (bb []byte, err error) {

	//req, _ := http.NewRequest("POST", s, nil)
	//req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36")

	log.Println("Post url=", s, "(params omitted)")
	res, err := this.client.PostForm(s, *v)
	if nil != err {
		return
	}
	defer res.Body.Close()

	bb, err = ioutil.ReadAll(res.Body)

	return
}
