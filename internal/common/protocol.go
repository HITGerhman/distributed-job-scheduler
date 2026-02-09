package common

import (
	"encoding/json"
	"fmt"
)

type Job struct {
	Name     string `json:"name"`
	Command  string `json:"command"`
	CronExpr string `json:"cronExpr"`
}
type Response struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

func BuildJobEvent(eventType int, job *Job) {

}
func BuildResponse(errno int, msg string, data interface{}) []byte {
	var response Response
	response.Errno = errno
	response.Msg = msg
	response.Data = data

	resp, err := json.Marshal(response)
	if err != nil {
		fmt.Printf("json marshal error:%v", err)
	}
	return resp
}
