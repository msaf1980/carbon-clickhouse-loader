package tags

import (
	"fmt"
	"sort"
	"strings"

	"github.com/maruel/natural"
	"github.com/msaf1980/go-stringutils"
)

func TagsParse(path string) (string, []string, error) {
	name, args, n := stringutils.Split2(path, ";")
	if n == 1 || args == "" {
		return name, nil, fmt.Errorf("incomplete tags in '%s'", path)
	}
	if strings.Contains(name, ";") {
		return name, nil, fmt.Errorf("name contain ';' in '%s'", path)
	}
	tags := make([]string, 1, 12)
	tags[0] = "__name__=" + name
	for {
		if delim := strings.Index(args, ";"); delim == -1 {
			if !strings.Contains(args, "=") {
				return name, nil, fmt.Errorf("incomplete tags in '%s'", path)
			}
			tags = append(tags, args)
			break
		} else {
			tagValue := args[0:delim]
			if !strings.Contains(tagValue, "=") {
				return name, nil, fmt.Errorf("incomplete tags in '%s'", path)
			}
			tags = append(tags, tagValue)
			if delim >= len(args)-1 {
				break
			}
			args = args[delim+1:]
		}
	}
	sort.Sort(natural.StringSlice(tags))
	var sb strings.Builder
	sb.WriteString(name)
	sb.WriteByte('?')
	for i, tag := range tags[1:] {
		if i > 0 {
			sb.WriteByte('&')
		}
		sb.WriteString(tag)
	}
	return sb.String(), tags, nil
}
