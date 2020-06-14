package protocol

import (
	"regexp"
)

/**
	检查topic和channel名称是否合规,
	长度必须在1 - 64之间,字符只能是大小写字母加数字以及.-_  另外允许#ephemeral结尾，这个结尾的表示是一次性的
 */
var validTopicChannelNameRegex = regexp.MustCompile(`^[\.a-zA-Z0-9_-]+(#ephemeral)?$`)

// IsValidTopicName checks a topic name for correctness
func IsValidTopicName(name string) bool {
	return isValidName(name)
}

// IsValidChannelName checks a channel name for correctness
func IsValidChannelName(name string) bool {
	return isValidName(name)
}

func isValidName(name string) bool {
	if len(name) > 64 || len(name) < 1 {
		return false
	}
	return validTopicChannelNameRegex.MatchString(name)
}
