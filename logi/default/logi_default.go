package logi

import (
	"fmt"
	"log" //don't use logrus here (will fail with appengine because of syscall)

	"github.com/noypi/gae"
	"github.com/noypi/gae/registry"
	nutil "github.com/noypi/util"
)

const Name = "default"

type LogIntDefault struct {
	LogLevel  int
	calldepth int
}

var g_bSilence bool

func Silence() {
	g_bSilence = true
}

func New(_ map[string]interface{}) (gae.LogInt, error) {
	return new(LogIntDefault), nil
}

func (this LogIntDefault) SetCallDepth(n int) {
	this.calldepth = n
}

func (this LogIntDefault) Output(calldepth int, logFn func(format string, args ...interface{}), s string) {
	logFn("%s %s", nutil.Lfmt(calldepth), s)

}

func (this LogIntDefault) Infof(format string, args ...interface{}) {
	if g_bSilence {
		return
	}
	log.Output(this.calldepth, fmt.Sprintf(format, args...))
}

func (this LogIntDefault) Debugf(format string, args ...interface{}) {
	if g_bSilence {
		return
	}
	log.Output(this.calldepth, fmt.Sprintf(format, args...))
}

func (this LogIntDefault) Warningf(format string, args ...interface{}) {
	log.Output(this.calldepth, fmt.Sprintf(format, args...))
}

func (this LogIntDefault) Errorf(format string, args ...interface{}) {
	log.Output(this.calldepth, fmt.Sprintf(format, args...))
}

func (this LogIntDefault) Criticalf(format string, args ...interface{}) {
	log.Output(this.calldepth, fmt.Sprintf(format, args...))
}

func (this LogIntDefault) EmailTo(ss []string, subject, body string, bHtml bool) {
}

func (this LogIntDefault) Email(subject, body string) {
	s := `Sending Email:
		subject: ` + subject + `
		body: ` + body

	log.Output(this.calldepth, fmt.Sprintf("%s", s))
}

func init() {
	registry.RegisterLogi(Name, New)
}
