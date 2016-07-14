package yatz

import "strings"
import (
	"fmt"
	"strconv"
)

func (e experiment) dump() string {
	strs := make([]string, 3)
	strs[0] = e.Name
	strs[1] = e.Type

	variations := make([]string, 0, len(e.Variations))
	var sum uint = 0
	for _, variation := range e.Variations {
		sum += variation.Weight
	}
	for i := range e.Variations {
		v := e.Variations[i]
		variations = append(variations, fmt.Sprintf("%s@%d/%d", v.Name, v.Weight, sum))
	}
	strs[2] = strings.Join(variations, ",")
	return strings.Join(strs, ":")
}

func (e experiment) dumpShort(userid string) string {
	return e.Name + ":" + e.Roll(userid)
}

func dumpActiveExperiments() string {
	return dumpExperiments(activeExperiments)
}

func dumpExperiments(experiments []*experiment) string {
	strs := make([]string, 1, 1+len(experiments))
	strs[0] = "YATZ_DUMP"
	for i := range experiments {
		strs = append(strs, experiments[i].dump())
	}
	return strings.Join(strs, "|")
}

func (e event) dump(userid string) string {
	strs := make([]string, 6)
	strs[0] = "YATZ_EVENT"
	if userid == "" {
		strs[1] = "_"
	} else {
		strs[1] = userid
	}
	strs[2] = cleanString(e.Name)
	if e.Identifier == "" {
		strs[3] = "_"
	} else {
		strs[3] = cleanString(e.Identifier)
	}
	strs[4] = strconv.FormatFloat(e.Count, 'f', 5, 64)
	exprs := make([]string, len(activeExperiments))
	for i := range activeExperiments {
		exprs[i] = activeExperiments[i].dumpShort(userid)
	}
	strs[5] = strings.Join(exprs, ",")
	return strings.Join(strs, "|")
}

var replacer = strings.NewReplacer(":", "_", "|", "_", ",", "_")

func cleanString(input string) string {
	return replacer.Replace(input)
}
