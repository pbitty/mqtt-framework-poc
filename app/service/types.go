package service

import "app/router"

type (
	DeviceNamespace struct {
		Region string
		Zone   string
		ID     string
	}

	TemperatureMessage struct {
		TemperatureCelcius float64
	}

	TemperatureTopic = router.TopicDef[DeviceNamespace, TemperatureMessage]
)
