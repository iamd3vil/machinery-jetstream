package broker

type Config struct {
	URL         string `json:"url"`
	EnabledAuth bool   `json:"enabled_auth"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	GroupName   string `json:"group_name"`
}
