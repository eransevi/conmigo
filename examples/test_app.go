package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/eransevi/conmigo/providers"
	"github.com/eransevi/conmigo"
)

type Service struct {
	name string
	tm   *conmigo.TransactionManager
}

type ActionDetails struct {
	serviceName string
	actionName  string
}

func main() {

	serviceName := flag.String("service", "a", "service name to start. Can be 'a', 'b', 'c'")
	//actionName := flag.String("action", "create", "action name for service")

	flag.Parse()

	service := NewService(*serviceName)

	details := GetActionDetails(service.name)

	if *serviceName == "a" {
		err := service.tm.Start(details)
		if err == nil {
			fmt.Printf("Action Succeeded")
		} else {
			fmt.Printf("Action Failed with error: %s", err)
		}
	}

	bufio.NewReader(os.Stdin).ReadBytes('\n')
}

func GetActionData(serviceName string) *conmigo.ActionData {

	var parentActionName string
	var dependencies []string

	switch serviceName {
	case "a":
		{
			dependencies = make([]string, 2)
			dependencies[0] = "a"
			dependencies[1] = "b"
		}

	case "b":
		parentActionName = "a"
	case "c":
		parentActionName = "a"
	}

	data := conmigo.NewActionData(
		         "action_" + serviceName,
		   parentActionName,
		       dependencies,
		  performActionFunc,
		 rollbackActionFunc)

	return data
}

func NewService(serviceName string) *Service {
	//set up providers
	cp := providers.NewRMQProvider("amqp://127.0.0.1")//&providers.RMQProvider{}
	sp := providers.NewRedisProvider("127.0.0.1:6379","",0)//&providers.RedisProvider{}

	data := GetActionData(serviceName)

	//set up transaction manager
	tm := conmigo.NewTransactionManager(serviceName, cp, sp)

	tm.Initialize(*data)

	service := &Service{
		name: serviceName,
		tm:   tm,
	}

	return service
}

func GetActionDetails(serviceName string) []byte {
	details := ActionDetails{
		serviceName: serviceName,
		actionName:  "TestAction",
	}
	fmt.Printf("details: %v\n", details)
	d, err := json.Marshal(details)

	if err != nil {
		fmt.Printf("err: %v", err)
	}
	os.Stdout.Write(d)
	return d
}

func performActionFunc(actionDetails interface{}) (interface{}, error) {
	var details ActionDetails
	fmt.Printf("actionDetails: %v\n", actionDetails)
	err := json.Unmarshal(actionDetails.([]byte), &details)
	if err == nil {
		msg := fmt.Sprintf("service %s: action %s performed", details.serviceName, details.actionName)
		fmt.Printf(msg)
	}

	return nil, err
}

func rollbackActionFunc(actionDetails interface{}) (interface{}, error) {
	var details ActionDetails
	err := json.Unmarshal(actionDetails.([]byte), details)
	if err == nil {
		msg := fmt.Sprintf("service %s: action %s rolledback", details.serviceName, details.actionName)
		fmt.Printf(msg)
	}

	return nil, err
}
