package conmigo

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/eransevi/conmigo/providers"
	"strconv"
)

type Conmigo interface {
	Initialize(data ActionData)
	Close()
	Start(actionDetails interface{}) error
}

type TransactionManager struct {
	name            string
	commProvider    providers.CommunicationProvider
	storageProvider providers.StorageProvider

	storageKey      string
	actionStatusKey string
	actionData      ActionData
	commData        CommunicationData
	depChannel      chan bool
}

type ActionData struct {
	actionName         string
	parentActionName   string
	dependencies       []string
	performActionFunc  func(actionDetails interface{}) (interface{}, error)
	rollbackActionFunc func(actionDetails interface{}) (interface{}, error)
}

type CommunicationData struct {
	startedChannelName   string
	abortedChannelName   string
	succeededChannelName string
	failedChannelName    string
}

const status_failed = "fail"

func NewActionData(actionName string, parentActionName string, dependencies []string, performActionFunc func(actionDetails interface{}) (interface{}, error), rollbackActionFunc func(actionDetails interface{}) (interface{}, error)) *ActionData {
	ad := &ActionData{
		actionName: actionName,
		parentActionName:parentActionName,
		dependencies:dependencies,
		performActionFunc:performActionFunc,
		rollbackActionFunc:rollbackActionFunc,
	}

	return ad
}

func NewTransactionManager(managerName string, commProv providers.CommunicationProvider, storageProv providers.StorageProvider) *TransactionManager {
	tm := &TransactionManager{
		name:            managerName,
		commProvider:    commProv,
		storageProvider: storageProv,
	}

	return tm
}

func (tm *TransactionManager) Initialize(data ActionData) {
	tm.actionData = data

	tm.initializeDependencies()
	tm.initializeCommunication()
}

func (tm *TransactionManager) Close() {

}

func (tm *TransactionManager) Start(actionDetails interface{}) error {
	err := tm.performAction(actionDetails)
	fmt.Println(err)
	if err == nil {
		tm.depChannel = make(chan bool)
		actionErr := <-tm.depChannel

		if !actionErr {
			return nil
		}
	}

	return errors.New("action failed")
}

func (tm *TransactionManager) initializeDependencies() {
	tm.storageKey = fmt.Sprintf("%s_%s", tm.name, tm.actionData.actionName)
	tm.storageProvider.Initialize(tm.storageKey)
}

func (tm *TransactionManager) initializeCommunication() {
	tm.commProvider.Initialize(tm.actionData.actionName)

	commData := CommunicationData{
		startedChannelName:   fmt.Sprintf("%s_Started", tm.actionData.parentActionName),
		abortedChannelName:   fmt.Sprintf("%s_Aborted", tm.actionData.parentActionName),
		succeededChannelName: fmt.Sprintf("%s_%s_Succeeded", tm.actionData.actionName),
		failedChannelName:    fmt.Sprintf("%s_%s_Failed", tm.actionData.actionName)}

	// Subscribe to events from parent
	if tm.actionData.parentActionName != "" {
		tm.commProvider.Subscribe(commData.startedChannelName, tm.onActionStarted)
		tm.commProvider.Subscribe(commData.abortedChannelName, tm.onActionAborted)
	}

	// Subscribe to events from dependencies
	for _, dep := range tm.actionData.dependencies {
		tm.commProvider.Subscribe(fmt.Sprintf(commData.succeededChannelName, dep), tm.onActionSucceeded)
		tm.commProvider.Subscribe(fmt.Sprintf(commData.failedChannelName, dep), tm.onActionFailed)
	}
}

func (tm *TransactionManager) performAction(actionDetails interface{}) error {
	actionID, err := tm.storageProvider.Increment(tm.storageKey)
	if err != nil {
		fmt.Println(err)
		return err
	}
	tm.actionStatusKey = fmt.Sprintf("%s_%s_status", tm.storageKey, actionID)

	result, err := tm.actionData.performActionFunc(actionDetails)

	if err == nil {
		byteResult, _ := json.Marshal(result)
		tm.storageProvider.Set(fmt.Sprintf("%s_%s", tm.storageKey, actionID), string(byteResult))
		err = tm.commProvider.Publish(tm.commData.startedChannelName, byteResult)

		if err != nil {
			fmt.Println(err)
			return err
		}

		if len(tm.actionData.dependencies) == 0 {
			err = tm.commProvider.Publish(tm.commData.succeededChannelName, byteResult)
		}
	}

	return err
}

func (tm *TransactionManager) rollbackAction(actionID string) error {
	result, _ := tm.storageProvider.Get(fmt.Sprintf("%s_%s", tm.storageKey, actionID))
	_, err := tm.actionData.rollbackActionFunc(result)

	tm.commProvider.Publish(tm.commData.failedChannelName, []byte(actionID))
	tm.commProvider.Publish(tm.commData.abortedChannelName, []byte(actionID))

	tm.depChannel <- false
	return err
}

func (tm *TransactionManager) onActionStarted(actionID string) {
	tm.performAction(actionID)
}

func (tm *TransactionManager) onActionAborted(actionID string) {
	tm.storageProvider.Set(fmt.Sprintf("%s_%s_status", tm.storageKey, actionID), status_failed)
	count, err := tm.storageProvider.Get(fmt.Sprintf("%s_%s_deps", tm.storageKey, actionID))
	if count == strconv.Itoa(len(tm.actionData.dependencies)) || err != nil {
		tm.rollbackAction(actionID)
	}
}

func (tm *TransactionManager) onActionSucceeded(actionID string) {
	count, err := tm.storageProvider.Increment(fmt.Sprintf("%s_%s_d eps", tm.storageKey, actionID))
	if err != nil {
		tm.rollbackAction(actionID)
	} else if count == len(tm.actionData.dependencies) {
		status, err := tm.storageProvider.Get(fmt.Sprintf("%s_%s_status", tm.storageKey, actionID))
		if status == status_failed || err != nil {
			tm.rollbackAction(actionID)
		} else {
			tm.commProvider.Publish(tm.commData.succeededChannelName, []byte(actionID))
			tm.depChannel <- true
		}
	}
}

func (tm *TransactionManager) onActionFailed(actionID string) {
	tm.storageProvider.Set(fmt.Sprintf("%s_%s_status", tm.storageKey, actionID), status_failed)
	count, err := tm.storageProvider.Increment(fmt.Sprintf("%s_%s_deps", tm.storageKey, actionID))
	if count == len(tm.actionData.dependencies) || err != nil {
		tm.rollbackAction(actionID)
	}
}
