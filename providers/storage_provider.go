package providers

type StorageProvider interface {
	Initialize(actionName string)
	Close()
	Increment(key string) (int, error)
	Set(key string, value string) error
	Get(key string) (string, error)
}
