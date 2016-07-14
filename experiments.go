package yatz

import (
	"math/rand"
	"sync"
)

const (
	Control = "Control"
)

var once sync.Once
var activeBins, experimentMap = preloadExperiments()

type variation struct {
	Name      string
	Weight    uint
	Whitelist []string
}

const (
	abTestType      = "abtest"
	phoneAbTestType = "phoneAbtest"
)

type experiment struct {
	Name         string
	Type         string
	Variations   []variation
	whitelistMap map[string]string
}

type event struct {
	Name       string
	Identifier string
	Count      float64
}

// PUBLIC

type Loggable interface {
	Infof(string, ...interface{})
	Warningf(string, ...interface{})
}

// Fire an event with a unique identifier
func FireWithId(log Loggable, userid, eventName, id string) {
	FireEvent(log, userid, event{Name: eventName, Identifier: id, Count: 1})
}

// Fire an event with a count
func Fire(log Loggable, userid, eventName string, count float64) {
	FireEvent(log, userid, event{Name: eventName, Count: count})
}

func FireEvent(log Loggable, userid string, event event) {
	log.Infof(event.dump(userid))
}

// Returns the experiment variant the user is under
func Roll(log Loggable, userid, expName string) string {
	FireWithId(log, userid, "rolled__"+expName, userid)
	if exp, ok := experimentMap[expName]; ok {
		return exp.Roll(userid)
	} else {
		log.Warningf("Unable to find experiment named: %s", expName)
		return ""
	}
}

func GetAll(userid string) map[string]string {
	toRet := make(map[string]string)
	for _, exp := range activeExperiments {
		toRet[exp.Name] = exp.Roll(userid)
	}
	return toRet
}

func GetAllByUseridAndContactKeyId(userid, contactKeyStringId string) map[string]string {
	toRet := make(map[string]string)
	for _, exp := range activeExperiments {
		if exp.Type == phoneAbTestType {
			toRet[exp.Name] = exp.Roll(contactKeyStringId)
		} else {
			toRet[exp.Name] = exp.Roll(userid)
		}
	}
	return toRet
}

// This method should be called by the appengine warmup script. This lets the query tool know what are the currently active experiments and variants
func Warmup(log Loggable) {
	once.Do(func() {
		log.Infof(dumpActiveExperiments())
	})
}

// from
// 	 http://www.cse.yorku.ca/~oz/hash.html
func hash(s string) int64 {
	var hash int64
	hash = 5381

	for i := range s {
		hash = ((hash << 5) + hash) + int64(s[i])
	}

	return hash
}

func preloadExperiments() (bins map[string][]int, experiments map[string]*experiment) {
	bins = make(map[string][]int)
	experiments = make(map[string]*experiment)
	for i := range activeExperiments {
		bins[activeExperiments[i].Name] = binVarWeights(activeExperiments[i].Variations)
		exp := activeExperiments[i]
		exp.preload()
		experiments[activeExperiments[i].Name] = exp
	}
	return
}

func binVarWeights(variations []variation) []int {
	sum := 0
	for i := range variations {
		sum += int(variations[i].Weight)
	}

	bin := make([]int, sum)
	j := 0
	w := int(variations[j].Weight)
	for i := range bin {
		if i >= w {
			j++
			w += int(variations[j].Weight)
		}
		bin[i] = j
	}
	return bin
}

func (e *experiment) Roll(userid string) string {
	if e.whitelistMap != nil {
		if v := e.whitelistMap[userid]; v != "" {
			return v
		}
	}
	bin := activeBins[e.Name]
	pick := rand.New(rand.NewSource(hash(e.Name + userid))).Intn(len(bin))
	return e.Variations[bin[pick]].Name
}

func (e *experiment) preload() {
	for _, variant := range e.Variations {
		for _, uid := range variant.Whitelist {
			if e.whitelistMap == nil {
				e.whitelistMap = make(map[string]string)
			}
			e.whitelistMap[uid] = variant.Name
		}
	}
}
