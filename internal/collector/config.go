package collector

type Config struct {
	Tables map[string]TablesConfig `yaml:"tables"`
}

type TablesConfig struct {
	Collect string `yaml:"collect"`
	Compare string `yaml:"compare"`
}

type collectResult struct {
	StartID int `json:"startId"`
	StopID  int `json:"stopId"`
}
