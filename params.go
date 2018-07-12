package gae

const (
	ContextParam  = "context"
	LoggerParam   = "logger"
	OptsParam     = "opts"
	UseCloudParam = "use_cloud"
)

const (
	WebiContext = ContextParam
	WebiLogger  = LoggerParam
	WebiJar     = "jar"
)

const (
	StoriContext   = ContextParam
	StoriLogger    = LoggerParam
	StoriOpts      = OptsParam
	StoriProjectID = "projectID"
	StoriBucket    = "bucket"
)

const (
	DbiContext    = ContextParam
	DbiLogger     = LoggerParam
	DbiAppID      = "appid"
	DbiName       = "name" // dbi Kind
	DbiOpts       = OptsParam
	DbiUseUpdated = "use_updated"
	DbiUseCloud   = UseCloudParam
)
