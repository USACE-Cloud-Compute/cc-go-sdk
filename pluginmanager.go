package cc

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

const (
	CcPayloadId         = "CC_PAYLOAD_ID"
	CcManifestId        = "CC_MANIFEST_ID"
	CcEventIdentifier   = "CC_EVENT_IDENTIFIER"
	CcEventNumber       = "CC_EVENT_NUMBER"
	CcPluginDefinition  = "CC_PLUGIN_DEFINITION"
	CcProfile           = "CC"
	CcPayloadFormatted  = "CC_PAYLOAD_FORMATTED"
	CcRootPath          = "CC_ROOT"
	CcLogIdentifier     = "CC_LOG"
	AwsAccessKeyId      = "AWS_ACCESS_KEY_ID"
	AwsSecretAccessKey  = "AWS_SECRET_ACCESS_KEY"
	AwsDefaultRegion    = "AWS_DEFAULT_REGION"
	AwsS3Bucket         = "AWS_S3_BUCKET"
	AwsS3Mock           = "S3_MOCK"
	AwsS3ForcePathStyle = "S3_FORCE_PATH_STYLE"
	AwsS3DisableSSL     = "S3_DISABLE_SSL"
	AwsS3Endpoint       = "AWS_ENDPOINT"
	FsbRootPath         = "FSB_ROOT_PATH"
)

//var substitutionRegexPattern string = `{([^{}]*)}`
// var substitutionRegex *regexp.Regexp

// Note: group numbers:
// 1 = TYPE
// 2 = VARNAME
// 3 = empty [] (captures the literal "[]")
// 4 = numeric index
// 5 = single-quoted key
// 6 = double-quoted key
var substitutionRegex = regexp.MustCompile(
	`\{(ATTR|VAR|ENV)::([a-zA-Z_][a-zA-Z0-9_]*)(?:` +
		`(\[\s*\])` + // group 3: captures "[]" when present
		`|\[\s*([0-9]+)\s*\]` + // group 4: numeric index
		`|\[\s*'([^']*)'\s*\]` + // group 5: single-quoted key
		`|\[\s*"([^"]*)"\s*\]` + // group 6: double-quoted key
		`)?\}`,
)

var maxretry int = 100

type NamedAction interface {
	GetName() string
}

type ActionRunnerBase struct {
	ActionName      string
	ContinueOnError bool
	PluginManager   *PluginManager
	Action          Action
}

func (arb ActionRunnerBase) GetName() string {
	return arb.ActionName
}

func (arb *ActionRunnerBase) SetName(name string) {
	arb.ActionName = name
}

func (arb *ActionRunnerBase) Log(msg string, args ...any) {
	if args == nil {
		args = []any{}
	}
	args = append(args, "action", arb.ActionName)
	arb.PluginManager.Logger.Action(msg, args...)
}

type ActionRunner interface {
	Run() error
}

var ActionRegistry ActionRunnerRegistry = make(map[string]ActionRunner)

func (arr *ActionRunnerRegistry) RegisterAction(actionName string, runner ActionRunner) {
	(*arr)[actionName] = runner
}

type ActionRunnerRegistry map[string]ActionRunner

//var ActionRegistry ActionRunnerRegistry = []ActionRunner{}

//type ActionRunnerRegistry []ActionRunner

// func (arr *ActionRunnerRegistry) RegisterAction(runner ActionRunner) {
// 	*arr = append(*arr, runner)
// }

// PluginManager is a Manager designed to simplify access to stores and usage of plugin api calls
type PluginManager struct {
	EventIdentifier string
	ccStore         CcStore
	Logger          *CcLogger
	Payload
}

type PluginManagerConfig struct {
	MaxRetry int
}

func InitPluginManagerWithConfig(config PluginManagerConfig) (*PluginManager, error) {
	maxretry = config.MaxRetry
	return InitPluginManager()
}

func connectStores(stores *[]DataStore) error {
	for i, ds := range *stores {
		newInstance, err := DataStoreTypeRegistry.New(ds.StoreType)
		if err != nil {
			return err
		}
		if cds, ok := newInstance.(ConnectionDataStore); ok {
			conn, err := cds.Connect(ds)
			if err != nil {
				return err
			}
			(*stores)[i].Session = conn
		}
	}
	return nil
}

func InitPluginManager() (*PluginManager, error) {
	manifestId := os.Getenv(CcManifestId)
	payloadId := os.Getenv(CcPayloadId)
	registerStoreTypes()
	//substitutionRegex, _ = regexp.Compile(substitutionRegexPattern)
	var manager PluginManager
	manager.EventIdentifier = os.Getenv(CcEventIdentifier)
	manager.Logger = NewCcLogger(CcLoggerInput{manifestId, payloadId, nil})

	// Create the store based on configuration
	store, err := NewCcStore()
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}

	manager.ccStore = store
	payload, err := store.GetPayload()
	if err != nil {
		return nil, fmt.Errorf("failed to get payload: %w", err)
	}

	manager.IOManager = payload.IOManager //@TODO do I absolutely need these two lines?
	manager.Actions = payload.Actions

	//make connections to the plugin manager stores
	err = connectStores(&manager.Stores)
	if err != nil {
		return nil, err
	}

	for i := range manager.Actions {
		//add the pm manager IOManager as a parent to the action IOManager
		//so that the action IOManager can recursively search through parent
		//IOManager elements
		manager.Actions[i].IOManager.SetParent(&manager.IOManager)

		//make connection to the action stores
		err = connectStores(&manager.Actions[i].Stores)
		if err != nil {
			return nil, err
		}
	}

	err = manager.substituteVariables()
	return &manager, err
}

// RunActions iterates through the registered actions and executes them.
//
// It iterates over the `Actions` slice in the `PluginManager`, and for each action,
// it searches the `ActionRegistry` for a matching runner implementation. If a match is found
// (identified by the action name), the runner is instantiated using reflection,
// its `PluginManager`, `Action`, and `ActionName` fields are set, and its `Run` method is called.
//
// This method relies heavily on reflection to dynamically instantiate and invoke the action runners,
// making it flexible but also potentially less performant than statically typed calls.
// It assumes that all action runner structs have fields named "PluginManager", "Action", and "ActionName".
//
// @TODO review error handling here.....
func (pm *PluginManager) RunActions() error {
	for _, action := range pm.Actions {
		for runnerName, runner := range ActionRegistry {
			if action.Name == runnerName {
				pm.Logger.Info("Running " + action.Name)
				t := reflect.TypeOf(runner).Elem() //runner is a pointer, so take the value of it
				pointerVal := reflect.New(t)       //create a new struct instance from type t
				structType := pointerVal.Elem()
				structType.FieldByName("PluginManager").Set(reflect.ValueOf(pm))
				structType.FieldByName("Action").Set(reflect.ValueOf(action))
				structType.FieldByName("ActionName").Set(reflect.ValueOf(runnerName))
				runMethod := pointerVal.MethodByName("Run") //must call method on the pointer receiver
				if runMethod.IsValid() {
					results := runMethod.Call(nil)
					//only a single error should be returned as results
					if len(results) > 0 {
						if err, ok := results[0].Interface().(error); ok {
							if !(structType.FieldByName("ContinueOnError").Bool()) {
								return fmt.Errorf("error running %s: %s", runnerName, err)
							}
						}
					}
				}
				pm.Logger.Info("Completed " + action.Name)
			}
			//}
		}
	}
	return nil
}

// -----------------------------------------------
// Wrapped IOManager functions
// -----------------------------------------------

func (pm PluginManager) GetStore(name string) (*DataStore, error) {
	return pm.IOManager.GetStore(name)
}

func (pm PluginManager) GetDataSource(input GetDsInput) (DataSource, error) {
	return pm.IOManager.GetDataSource(input)
}

func (pm PluginManager) GetInputDataSource(name string) (DataSource, error) {
	return pm.IOManager.GetInputDataSource(name)
}

func (pm PluginManager) GetOutputDataSource(name string) (DataSource, error) {
	return pm.IOManager.GetOutputDataSource(name)
}

func (pm PluginManager) GetReader(input DataSourceOpInput) (io.ReadCloser, error) {
	return pm.IOManager.GetReader(input)
}

func (pm PluginManager) Get(input DataSourceOpInput) ([]byte, error) {
	return pm.IOManager.Get(input)
}

func (pm PluginManager) Put(input PutOpInput) (int, error) {
	return pm.IOManager.Put(input)
}

func (pm PluginManager) Copy(src DataSourceOpInput, dest DataSourceOpInput) error {
	return pm.IOManager.Copy(src, dest)
}

func (pm PluginManager) CopyFileToLocal(dsName string, pathkey string, dataPathKey string, localPath string) error {
	return pm.IOManager.CopyFileToLocal(dsName, pathkey, dataPathKey, localPath)
}

func (pm PluginManager) CopyFileToRemote(input CopyFileToRemoteInput) error {
	return pm.IOManager.CopyFileToRemote(input)
}

// -----------------------------------------------
// Private utility functions
// -----------------------------------------------
func (pm *PluginManager) substituteVariables() error {

	//allow env substitution within payload attributes
	pm.substituteMapVariables(pm.Attributes, false)

	for i, ds := range pm.Inputs {
		err := pathsSubstitute(&ds, pm.Attributes)
		if err != nil {
			return err
		}
		pm.Inputs[i] = ds
	}
	for i, ds := range pm.Outputs {
		err := pathsSubstitute(&ds, pm.Attributes)
		if err != nil {
			return err
		}
		pm.Outputs[i] = ds
	}

	for _, action := range pm.Actions {

		//allow env and payload attribute substition within action attributes
		pm.substituteMapVariables(action.Attributes, true)

		//create a map for a combined action parameter and payload parameter list
		combinedParams := maps.Clone(pm.Attributes)
		if combinedParams == nil {
			combinedParams = make(map[string]any)
		}
		maps.Copy(combinedParams, action.Attributes)

		for i, ds := range action.Inputs {
			err := pathsSubstitute(&ds, combinedParams)
			if err != nil {
				return err
			}
			action.Inputs[i] = ds
		}

		for i, ds := range action.Outputs {
			err := pathsSubstitute(&ds, combinedParams)
			if err != nil {
				return err
			}
			action.Outputs[i] = ds
		}
	}

	return nil
}

// substitutes map (i.e. payload or action attributes)
// takes the set of attributes as a param argument to support recursing into attribute maps and arrays
func (pm *PluginManager) substituteMapVariables(params map[string]any, attrSub bool) {
	for param, valAny := range params {
		switch val := valAny.(type) {
		case string:
			newval, err := parameterSubstitute(val, pm.Attributes, attrSub)
			if err == nil {
				params[param] = newval
			}
		case map[string]any:
			pm.substituteMapVariables(val, attrSub)
		case []string:
			for i, v := range val {
				newval, err := parameterSubstitute(v, pm.Attributes, attrSub)
				if err == nil {
					val[i] = newval
				}
			}
		}

	}
}

func pathsSubstitute(ds *DataSource, payloadAttr map[string]any) error {
	name, err := parameterSubstitute(ds.Name, payloadAttr, true)
	if err != nil {
		return err
	}
	ds.Name = name

	for i, p := range ds.Paths {
		path, err := parameterSubstitute(p, payloadAttr, true)
		if err != nil {
			return err
		}
		ds.Paths[i] = path
	}

	for i, p := range ds.DataPaths {
		path, err := parameterSubstitute(p, payloadAttr, true)
		if err != nil {
			return err
		}
		ds.DataPaths[i] = path
	}

	return nil
}

//---------------------------------------

type EmbeddedVar struct {
	Type         string
	Varname      string
	IsArrayOrMap bool
	ArrayIndex   int
	MapIndex     string
}

// func parameterSubstitute3(paramkey string, param any, payloadAttr map[string]any, attrSub bool) ([]string, error) {
// 	ouptut := []string{}
// 	switch template := param.(type) {
// 	case string:
// 		result := substitutionRegex.FindAllStringSubmatch(template, -1)
// 		for _, match := range result {
// 			eVars := matchToEmbeddedVars(match)

// 			valstring := fmt.Sprintf("%v", val)
// 			output = append(output, strings.Replace(template, match[0], valstring, 1))
// 		}
// 		return output, nil
// 	default:
// 		return nil, errors.New("invalid parameter type")
// 	}
// }

// func getSubstitutionVal(evar EmbeddedVar) (any, error) {
// 	var returnval any
// 	switch evar.Type {
// 	case "ENV":
// 		//get the env var then try and split it with a comma separator
// 		//supported env values are single vals "1" or csv vals "one,two,three"
// 		returnval = strings.Split(os.Getenv(evar.Varname), ",")
// 	case "ATTR":
// 		val2, ok := payloadAttr[evar.Varname]
// 		if !ok{
// 			return nil,fmt.Errorf("invalid attribute name: %s",evar.Varname)
// 		}

// 	}
// }

func parseEmbeddedVars(s string) []EmbeddedVar {
	matches := substitutionRegex.FindAllStringSubmatch(s, -1)
	out := make([]EmbeddedVar, 0, len(matches))

	for _, m := range matches {
		ev := EmbeddedVar{
			Type:       m[1],
			Varname:    m[2],
			ArrayIndex: -1,
		}

		// If any of groups 3..6 matched, it's an array/map reference.
		switch {
		case m[3] != "":
			// matched [] (empty index)
			ev.IsArrayOrMap = true
		case m[4] != "":
			// matched [0] numeric array index
			ev.IsArrayOrMap = true
			i, err := strconv.Atoi(m[4])
			if err == nil {
				ev.ArrayIndex = i
			}
		case m[5] != "":
			// matched ['key'] single-quoted map key
			ev.IsArrayOrMap = true
			ev.MapIndex = m[5]
		case m[6] != "":
			// matched ["key"] double-quoted map key
			ev.IsArrayOrMap = true
			ev.MapIndex = m[6]
		}

		out = append(out, ev)
	}
	return out
}

//-----------------------------

// func parameterSubstitute2(paramkey string, param any, payloadAttr map[string]any, attrSub bool) ([]string, error) {
// 	output := []string{}
// 	switch template := param.(type) {
// 	case string:
// 		result := substitutionRegex.FindAllStringSubmatch(template, -1)
// 		for _, match := range result {
// 			eVars := matchToEmbeddedVars(match)
// 			var val any
// 			var ok bool
// 			switch eVars.Type {
// 			case "ENV":
// 				val = os.Getenv(eVars.Varname)
// 			case "ATTR":
// 				if attrSub {
// 					val, ok = payloadAttr[eVars.Varname]
// 					if !ok {
// 						return nil, fmt.Errorf("variable substitution name '%s' not found in the payload", eVars.Varname)
// 					}
// 				}
// 			default:
// 				continue //if its not ENV or ATTR, skip the substitution
// 			}
// 			valstring := fmt.Sprintf("%v", val)
// 			output = append(output, strings.Replace(template, match[0], valstring, 1))
// 		}
// 		return output, nil
// 	default:
// 		return nil, errors.New("invalid parameter type")
// 	}
// }

func parameterSubstitute(param any, payloadAttr map[string]any, attrSub bool) (string, error) {
	switch template := param.(type) {
	case string:
		result := substitutionRegex.FindAllStringSubmatch(template, -1)
		for _, match := range result {
			sub := strings.Split(match[1], "::")
			if len(sub) != 2 {
				return "", fmt.Errorf("invalid data source substitution: %s", match[0])
			}
			val := ""
			switch {
			case sub[0] == "ENV":
				val = os.Getenv(sub[1])
				if val == "" {
					return "", fmt.Errorf("invalid data source substitution.  missing environment parameter: %s", match[0])
				}
			case sub[0] == "ATTR" && attrSub:
				val2, ok := payloadAttr[sub[1]]
				if !ok {
					return "", fmt.Errorf("invalid data source substitution.  missing payload parameter: %s", match[0])
				}
				val = fmt.Sprintf("%v", val2) //need to coerce non-string values into strings.  for example ints might be perfectly valid for parameter substitution in a url
			default:
				continue //if its not ENV or ATTR, skip the substitution
			}
			template = strings.Replace(template, match[0], val, 1)
		}
		return template, nil
	default:
		return "", errors.New("invalid parameter type")
	}
}

func templateVarSubstitution(template string, templateVars map[string]string) string {
	result := substitutionRegex.FindAllStringSubmatch(template, -1)
	for _, match := range result {
		sub := strings.Split(match[1], "::")
		if len(sub) != 2 {
			continue
		}
		if sub[0] == "VAR" {
			if val, ok := templateVars[sub[1]]; ok {
				template = strings.Replace(template, match[0], val, 1)
			}
		}
	}
	return template
}

// func matchToEmbeddedVars(match []string) EmbeddedVar {

// 	ev := EmbeddedVar{
// 		Type:       match[1],
// 		Varname:    match[2],
// 		ArrayIndex: -1,
// 	}

// 	// If any of groups 3..6 matched, it's an array/map reference.
// 	switch {
// 	case match[3] != "":
// 		// matched [] (empty index)
// 		ev.IsArrayOrMap = true
// 	case match[4] != "":
// 		// matched [0] numeric array index
// 		ev.IsArrayOrMap = true
// 		i, err := strconv.Atoi(match[4])
// 		if err == nil {
// 			ev.ArrayIndex = i
// 		}
// 	case match[5] != "":
// 		// matched ['key'] single-quoted map key
// 		ev.IsArrayOrMap = true
// 		ev.MapIndex = match[5]
// 	case match[6] != "":
// 		// matched ["key"] double-quoted map key
// 		ev.IsArrayOrMap = true
// 		ev.MapIndex = match[6]
// 	}
// 	return ev
// }
