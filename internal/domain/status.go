package domain

func IsInstanceTerminalStatus(status string) bool {
	switch status {
	case InstanceStatusSucceeded, InstanceStatusFailed:
		return true
	default:
		return false
	}
}

func IsAttemptTerminalStatus(status string) bool {
	switch status {
	case AttemptStatusSucceeded, AttemptStatusFailed, AttemptStatusTimeout, AttemptStatusKilled:
		return true
	default:
		return false
	}
}
