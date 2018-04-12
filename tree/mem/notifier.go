package mem

// Notifier describes an optional entity to each node for it to notify certain
// events. The user of this library can implement its own notifier to capture
// and process those events.
type Notifier interface {
	NodeChanged(id string)
}
