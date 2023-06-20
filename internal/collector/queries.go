package collector

import "fmt"

func prepareCollectQuery(tname string, data TablesConfig) string {
	collectQ := fmt.Sprintf(data.Collect, tname)
	return collectQ
}

func prepareCompareQuery(tname string, data TablesConfig, idStart, idStop int) string {
	compareQ := fmt.Sprintf(data.Compare, tname, tname, idStart, idStop)
	return compareQ
}
