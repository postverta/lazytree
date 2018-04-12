package driver

import (
	"fmt"
)

type DriverManager struct {
	drivers map[string]Driver
}

var manager *DriverManager

func NewDriverManager() *DriverManager {
	return &DriverManager{
		drivers: make(map[string]Driver),
	}
}

func (dm *DriverManager) RegisterDriver(driver Driver) {
	dm.drivers[driver.Name()] = driver
}

func (dm *DriverManager) GetDriver(name string) (Driver, error) {
	d, found := dm.drivers[name]
	if !found {
		return nil, fmt.Errorf("Cannot find driver %s", name)
	} else {
		return d, nil
	}
}

func RegisterDriver(driver Driver) {
	if manager == nil {
		manager = NewDriverManager()
	}
	manager.RegisterDriver(driver)
}

func GetDriver(name string) (Driver, error) {
	if manager == nil {
		manager = NewDriverManager()
	}
	return manager.GetDriver(name)
}
