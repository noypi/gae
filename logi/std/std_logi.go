package logi

import (
	"fmt"
	"strings"

	"context"

	nutil "github.com/noypi/util"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/mail"

	"github.com/noypi/gae"
	"github.com/noypi/gae/registry"
)

const Name = "standard"

type Logi struct {
	c           context.Context
	calldepth   int
	emailsender string
}

// required params:
//     context context.Context
func New(params map[string]interface{}) (gae.LogInt, error) {
	c, has := params["context"]
	if !has {
		return nil, fmt.Errorf("invalid params, context not defined.")
	}
	o := newLogi(c.(context.Context))
	if sender, has := params["email-sender"]; has {
		o.emailsender = sender.(string)
	} else {
		o.emailsender = "adrian.migraso@gmail.com"
	}
	return o, nil
}

func newLogi(c context.Context) *Logi {
	return &Logi{
		c:         c,
		calldepth: 4,
	}
}

func (this *Logi) SetCallDepth(n int) {
	this.calldepth = n
}

func (this Logi) Output(calldepth int, logFn func(format string, args ...interface{}), s string) {
	logFn("%s %s", nutil.Lfmt(calldepth), s)
}

func (this Logi) Debugf(format string, args ...interface{}) {
	log.Debugf(this.c, "%s "+format, append([]interface{}{nutil.Lfmt(this.calldepth)}, args...)...)
}

func (this Logi) Infof(format string, args ...interface{}) {
	log.Infof(this.c, "%s "+format, append([]interface{}{nutil.Lfmt(this.calldepth)}, args...)...)
}

func (this Logi) Warningf(format string, args ...interface{}) {
	log.Warningf(this.c, "%s "+format, append([]interface{}{nutil.Lfmt(this.calldepth)}, args...)...)
}

func (this Logi) Errorf(format string, args ...interface{}) {
	log.Errorf(this.c, "%s "+format, append([]interface{}{nutil.Lfmt(this.calldepth)}, args...)...)
}

func (this Logi) Criticalf(format string, args ...interface{}) {
	log.Criticalf(this.c, "%s "+format, append([]interface{}{nutil.Lfmt(this.calldepth)}, args...)...)
}

func (this Logi) EmailTo(receivers []string, subject, body string, bHtml bool) {
	msg := &mail.Message{
		To:      receivers,
		Sender:  this.emailsender,
		Subject: subject,
	}

	if bHtml {
		msg.HTMLBody = body
	} else {
		msg.Body = body
	}

	err := mail.Send(this.c, msg)
	if nil != err {
		log.Errorf(this.c, "Email err=%v", err)
	}

}

func (this Logi) Email(subject, body string) {
	msg := &mail.Message{
		Sender:  "adrian.migraso@gmail.com",
		Subject: subject,
	}
	if strings.HasSuffix(strings.TrimSpace(strings.ToLower(body)), "</html>") {
		msg.HTMLBody = body
	} else {
		msg.Body = body
	}

	var err error = mail.SendToAdmins(this.c, msg)
	if nil != err {
		log.Errorf(this.c, "Email err=%v", err)
	}
}

func init() {
	registry.RegisterLogi(Name, New)
}
