package providers

type CommunicationProvider interface {
	Initialize(actionName string)
	Close()
	Subscribe(channel string, callback func(string)) error
	Publish(channel string, eventData []byte) error
}
