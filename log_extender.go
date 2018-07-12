package gae

import (
	"fmt"
)

type LogEx struct {
	LogInt
}

type LogExtended interface {
	LogInt
	Println(...interface{})
	Errorln(...interface{})
	Criticalln(...interface{})
	Fatalln(...interface{})
}

func ExtendLogger(l LogInt) LogExtended {
	o := new(LogEx)
	o.LogInt = l
	return o
}

func (this LogEx) Println(as ...interface{}) {
	this.Infof("%s", fmt.Sprintln(as...))
}

func (this LogEx) Errorln(as ...interface{}) {
	this.Errorf("%s", fmt.Sprintln(as...))
}

func (this LogEx) Criticalln(as ...interface{}) {
	this.Criticalf("%s", fmt.Sprintln(as...))
}

func (this LogEx) Fatalln(as ...interface{}) {
	this.Criticalln(as)
	panic(fmt.Sprintln(as...))
}
