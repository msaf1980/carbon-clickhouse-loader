package tags

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTagsParse(t *testing.T) {
	assert := assert.New(t)

	// make metric name as receiver
	metric := "cpu_util;fqdn=asd;dc=qwe;instance=10.33.10.10_9100;job=node"

	path, tags, err := TagsParse(metric)
	if err != nil {
		t.Errorf("tagParse: %s", err.Error())
	}
	assert.Equal("cpu_util?dc=qwe&fqdn=asd&instance=10.33.10.10_9100&job=node", path)
	assert.Equal([]string{"__name__=cpu_util", "dc=qwe", "fqdn=asd", "instance=10.33.10.10_9100", "job=node"}, tags)
}
