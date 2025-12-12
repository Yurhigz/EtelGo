package main

import "time"

func ProcessDuration(duration string) (time.Duration, error) {
	return time.ParseDuration(duration)
}
