package cc

import (
	"fmt"
	"io"
	"maps"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// Environment variable keys, profile identifiers, and parameter substitution prefixes
// used to configure the Cloud Compute SDK, AWS/S3 connections, and file store roots.
const (
	// CcPayloadId is the environment variable name for the Cloud Compute payload identifier.
	CcPayloadId         = "CC_PAYLOAD_ID"
	// CcManifestId is the environment variable name for the Cloud Compute manifest identifier.
	CcManifestId        = "CC_MANIFEST_ID"
	// CcEventIdentifier is the environment variable name for the current event identifier.
	CcEventIdentifier   = "CC_EVENT_IDENTIFIER"
	// CcEventNumber is the environment variable name for the current event number.
	CcEventNumber       = "CC_EVENT_NUMBER"
	// CcPluginDefinition is the environment variable name for the plugin definition.
	CcPluginDefinition  = "CC_PLUGIN_DEFINITION"
	// CcProfile is the default credential profile prefix used for Cloud Compute environment variables.
	CcProfile           = "CC"
	// CcPayloadFormatted is the environment variable name indicating whether the payload is formatted.
	CcPayloadFormatted  = "CC_PAYLOAD_FORMATTED"
	// CcRootPath is the environment variable name for the Cloud Compute root storage path.
	CcRootPath          = "CC_ROOT"
	// CcLogIdentifier is the environment variable name for the Cloud Compute logging configuration.
	CcLogIdentifier     = "CC_LOG"
	// AwsAccessKeyId is the environment variable suffix/name for the AWS access key ID.
	AwsAccessKeyId      = "AWS_ACCESS_KEY_ID"
	// AwsCredProfile is the environment variable suffix/name for the AWS credentials profile.
	AwsCredProfile      = "AWS_PROFILE"
	// AwsSecretAccessKey is the environment variable suffix/name for the AWS secret access key.
	AwsSecretAccessKey  = "AWS_SECRET_ACCESS_KEY"
	// AwsDefaultRegion is the environment variable suffix/name for the AWS default region.
	AwsDefaultRegion    = "AWS_DEFAULT_REGION"
	// AwsS3Bucket is the environment variable suffix/name for the target AWS S3 bucket.
	AwsS3Bucket         = "AWS_S3_BUCKET"
	// AwsS3Mock is the environment variable suffix/name used to enable S3 mocking.
	AwsS3Mock           = "S3_MOCK"
	// AwsS3ForcePathStyle is the environment variable suffix/name used to force path-style S3 addressing.
	AwsS3ForcePathStyle = "S3_FORCE_PATH_STYLE"
	// AwsS3DisableSSL is the environment variable suffix/name used to disable SSL for S3 connections.
	AwsS3DisableSSL     = "S3_DISABLE_SSL"
	// AwsS3Endpoint is the environment variable suffix/name for a custom AWS S3 endpoint URL.
	AwsS3Endpoint       = "AWS_ENDPOINT"
	// AwsS3AssumedRole is the environment variable suffix/name for an AWS IAM role ARN to assume for S3 access.
	AwsS3AssumedRole    = "AWS_S3_ASSUMED_ROLE"
	// FsbRootPath is the environment variable suffix/name for the block file store (FSB) root path.
	FsbRootPath         = "FSB_ROOT_PATH"
	// ParmamSubEnv is the substitution type identifier for environment variable lookups ("ENV").
	ParmamSubEnv        = "ENV"
	// ParamSubAttr is the substitution type identifier for payload or action attribute lookups ("ATTR").
	ParamSubAttr        = "ATTR"
)

//var substitutionRegexPattern string = `{([^{}]*)}`
// var substitutionRegex *regexp.Regexp

// substitutionRegex matches parameter substitution placeholders of the form
// {TYPE::VARNAME}, with optional array or map indexing syntax such as
// {TYPE::VARNAME[]}, {TYPE::VARNAME[0]}, {TYPE::VARNAME['key']}, or {TYPE::VARNAME["key"]}.
//
// Note: group numbers:
// 1 = TYPE (ATTR, VAR, or ENV)
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

// validAutoSubstitution defines the set of substitution variable types ("ATTR" and "ENV")
// that are automatically resolved when the PluginManager is initialized.
var validAutoSubstitution map[string]struct{} = map[string]struct{}{
	"ATTR": {},
	"ENV":  {},
}

// maxretry specifies the maximum number of retry attempts for plugin manager operations.
var maxretry int = 100

// NamedAction represents an action type that exposes its registered name.
type NamedAction interface {
	// GetName returns the name of the action.
	GetName() string
}

// ActionRunnerBase provides a reusable base struct to be embedded in concrete ActionRunner
// implementations. It holds the action's name, error-handling behavior, a reference to the
// parent PluginManager, and the Action configuration from the payload.
type ActionRunnerBase struct {
	// ActionName is the registered name of the action being executed.
	ActionName      string
	// ContinueOnError indicates whether PluginManager.RunActions should continue
	// executing subsequent actions if this action's Run method returns an error.
	ContinueOnError bool
	// PluginManager is a pointer to the active PluginManager orchestrating the execution.
	PluginManager   *PluginManager
	// Action contains the specific Action configuration and scoped IOManager from the payload.
	Action          Action
}

// GetName returns the name of the action associated with the ActionRunnerBase.
func (arb ActionRunnerBase) GetName() string {
	return arb.ActionName
}

// SetName sets the action name on the ActionRunnerBase.
func (arb *ActionRunnerBase) SetName(name string) {
	arb.ActionName = name
}

// Log writes an action-scoped log message via the PluginManager's Logger,
// automatically appending the "action" key-value pair with the runner's ActionName.
func (arb *ActionRunnerBase) Log(msg string, args ...any) {
	if args == nil {
		args = []any{}
	}
	args = append(args, "action", arb.ActionName)
	arb.PluginManager.Logger.Action(msg, args...)
}

// ActionRunner defines the interface that all executable plugin actions must implement.
type ActionRunner interface {
	// Run executes the action's business logic and returns an error if execution fails.
	Run() error
}

// ActionRegistry is the package-level registry mapping action names to their corresponding
// ActionRunner implementations. Plugins register their runners here prior to calling RunActions.
var ActionRegistry ActionRunnerRegistry = make(map[string]ActionRunner)

// RegisterAction registers an ActionRunner implementation in the registry under the specified actionName.
func (arr *ActionRunnerRegistry) RegisterAction(actionName string, runner ActionRunner) {
	(*arr)[actionName] = runner
}

// ActionRunnerRegistry maps action names to prototype ActionRunner instances used to instantiate
// and execute actions defined in a Payload.
type ActionRunnerRegistry map[string]ActionRunner

//var ActionRegistry ActionRunnerRegistry = []ActionRunner{}

//type ActionRunnerRegistry []ActionRunner

// func (arr *ActionRunnerRegistry) RegisterAction(runner ActionRunner) {
// 	*arr = append(*arr, runner)
// }

// PluginManager is a central manager designed to simplify access to data stores,
// payload attributes, logging, and action execution within Cloud Compute plugins.
// It embeds Payload (and transitively IOManager) to provide direct access to payload-level
// stores, inputs, outputs, and attributes.
type PluginManager struct {
	// EventIdentifier holds the event identifier read from the CC_EVENT_IDENTIFIER environment variable.
	EventIdentifier string
	// ccStore is the underlying backing store used to fetch the execution Payload.
	ccStore         CcStore
	// Logger is the structured logger configured for the current manifest and payload.
	Logger          *CcLogger
	Payload
}

// PluginManagerConfig holds custom configuration options for initializing a PluginManager.
type PluginManagerConfig struct {
	// MaxRetry specifies the maximum number of retries for operations.
	MaxRetry int
}

// InitPluginManagerWithConfig sets package-level configuration options (such as maxretry)
// from config and initializes a new PluginManager via InitPluginManager.
func InitPluginManagerWithConfig(config PluginManagerConfig) (*PluginManager, error) {
	maxretry = config.MaxRetry
	return InitPluginManager()
}

// connectStores iterates over the provided slice of DataStore configurations, instantiates
// each store type from DataStoreTypeRegistry, and establishes a connection for any store
// implementing ConnectionDataStore, storing the resulting session in the DataStore's Session field.
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

// InitPluginManager initializes and returns a fully configured PluginManager based on
// environment variables and the remote or local CcStore payload.
//
// During initialization, it:
//  1. Reads manifest, payload, and event identifiers from the environment and initializes a CcLogger.
//  2. Creates a CcStore from environment settings and retrieves the execution Payload.
//  3. Performs automatic variable substitution ({ENV::...} and {ATTR::...}) across payload and action attributes, store parameters, and data source paths.
//  4. Connects all payload-level and action-level DataStores and links each Action's IOManager parent to the PluginManager's IOManager.
func InitPluginManager() (*PluginManager, error) {
	manifestId := os.Getenv(CcManifestId)
	payloadId := os.Getenv(CcPayloadId)
	registerStoreTypes()
	//substitutionRegex, _ = regexp.Compile(substitutionRegexPattern)
	var manager PluginManager
	manager.EventIdentifier = os.Getenv(CcEventIdentifier)
	manager.Logger = NewCcLogger(CcLoggerInput{manifestId, payloadId, nil})

	// use a nil input to create the store based on configuration in the env
	store, err := NewCcStore(nil)
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

	//perform payload variable substitution
	err = manager.substituteVariables()
	if err != nil {
		return nil, err
	}

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

// GetStore retrieves a DataStore by name from the PluginManager's embedded IOManager.
func (pm PluginManager) GetStore(name string) (*DataStore, error) {
	return pm.IOManager.GetStore(name)
}

// GetDataSource retrieves a DataSource matching the specified name and I/O direction
// from the PluginManager's embedded IOManager.
func (pm PluginManager) GetDataSource(input GetDsInput) (DataSource, error) {
	return pm.IOManager.GetDataSource(input)
}

// GetInputDataSource retrieves an input DataSource by name from the PluginManager's embedded IOManager.
func (pm PluginManager) GetInputDataSource(name string) (DataSource, error) {
	return pm.IOManager.GetInputDataSource(name)
}

// GetOutputDataSource retrieves an output DataSource by name from the PluginManager's embedded IOManager.
func (pm PluginManager) GetOutputDataSource(name string) (DataSource, error) {
	return pm.IOManager.GetOutputDataSource(name)
}

// GetReader opens and returns an io.ReadCloser for reading data from an input DataSource
// via the PluginManager's embedded IOManager. The caller is responsible for closing the returned reader.
func (pm PluginManager) GetReader(input DataSourceOpInput) (io.ReadCloser, error) {
	return pm.IOManager.GetReader(input)
}

// Get reads and returns the entire contents of an input DataSource resource as a byte slice
// via the PluginManager's embedded IOManager.
func (pm PluginManager) Get(input DataSourceOpInput) ([]byte, error) {
	return pm.IOManager.Get(input)
}

// Put writes data from an io.Reader to an output DataSource resource via the PluginManager's
// embedded IOManager, returning the number of bytes written.
func (pm PluginManager) Put(input PutOpInput) (int, error) {
	return pm.IOManager.Put(input)
}

// Copy transfers data from a source DataSource to a destination DataSource
// via the PluginManager's embedded IOManager.
func (pm PluginManager) Copy(src DataSourceOpInput, dest DataSourceOpInput) error {
	return pm.IOManager.Copy(src, dest)
}

// CopyFileToLocal copies a file or directory from a remote input FileDataStore
// to the local filesystem via the PluginManager's embedded IOManager.
func (pm PluginManager) CopyFileToLocal(input CopyToLocalInput) error {
	return pm.IOManager.CopyFileToLocal(input)
}

// CopyFileToRemote copies a local file or directory to a remote FileDataStore
// via the PluginManager's embedded IOManager.
func (pm PluginManager) CopyFileToRemote(input CopyFileToRemoteInput) error {
	return pm.IOManager.CopyFileToRemote(input)
}

// -----------------------------------------------
// Private utility functions
// -----------------------------------------------

// substituteVariables performs automatic variable substitution across the PluginManager's
// payload attributes, store parameters, input/output data sources, and all action attributes
// and action-level input/output data sources.
//
// Payload attributes only allow environment variable ({ENV::...}) substitutions, whereas
// store parameters, data sources, and action attributes allow both environment variable
// and attribute ({ATTR::...}) substitutions.
func (pm *PluginManager) substituteVariables() error {

	//allow env substitution within payload attributes
	pm.substituteMapVariables(pm.Attributes, false)

	//allow substitution on data store params
	for _, store := range pm.Stores {
		pm.substituteMapVariables(store.Parameters, true)
	}

	//allow substitution on input data source paths and data paths
	for i, ds := range pm.Inputs {
		err := pathsSubstitute(&ds, pm.Attributes)
		if err != nil {
			return err
		}
		pm.Inputs[i] = ds
	}

	//allow substitution on input data source paths and data paths
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

// ----------------------------------------
// substituteMapVariables recursively traverses a map of attributes or parameters (including nested
// maps and slices) and replaces template placeholders with resolved environment or attribute values.
// When attrSub is true, {ATTR::...} placeholders are resolved against pm.Attributes in addition to {ENV::...} placeholders.
func (pm *PluginManager) substituteMapVariables(params map[string]any, attrSub bool) {
	for param, valAny := range params {
		switch val := valAny.(type) {
		case string:
			newvals, err := parameterSubstitute(paramSubInput{
				TemplateKey:                param,
				Template:                   val,
				Attributes:                 pm.Attributes,
				AllowAttributeSubstitution: attrSub,
			})
			if err == nil {
				delete(params, param)
				for k, v := range newvals {
					params[k] = v
				}
			}
		case map[string]any:
			pm.substituteMapVariables(val, attrSub)
		case []string:
			newslice := handleSliceSub(val, pm, attrSub)
			params[param] = newslice
		case []any:
			newslice := handleSliceSub(val, pm, attrSub)
			params[param] = newslice
		}
	}
}

// pathsSubstitute performs environment and attribute variable substitution on a DataSource's
// Name, Paths, and DataPaths maps using the provided attribute map.
// If an unindexed array or map placeholder (e.g., {ATTR::items[]}) is used in Paths or DataPaths,
// the single path entry is expanded into multiple entries in the resulting map.
func pathsSubstitute(ds *DataSource, attr map[string]any) error {
	//handle data source name substitution
	nameResult, err := parameterSubstitute(paramSubInput{
		TemplateKey:                "name", //this is the data source name, so we will not allow inflating into multiple paths.  key doesn't matter here
		Template:                   ds.Name,
		Attributes:                 attr,
		AllowAttributeSubstitution: true,
	})
	if err != nil {
		return err
	}
	if nameout, ok := nameResult["name"]; ok {
		ds.Name = nameout
	} else {
		return fmt.Errorf("invalid data source name substitution for %s", ds.Name)
	}

	if len(ds.Paths) > 0 {
		newPaths := make(map[string]string, len(ds.Paths))
		for k, p := range ds.Paths {
			paths, err := parameterSubstitute(paramSubInput{
				TemplateKey:                k,
				Template:                   p,
				Attributes:                 attr,
				AllowAttributeSubstitution: true,
			})
			if err != nil {
				return err
			}
			maps.Copy(newPaths, paths)
		}
		ds.Paths = newPaths
	}

	if len(ds.DataPaths) > 0 {
		newDataPaths := make(map[string]string, len(ds.DataPaths))
		for k, p := range ds.DataPaths {
			datapaths, err := parameterSubstitute(paramSubInput{
				TemplateKey:                k,
				Template:                   p,
				Attributes:                 attr,
				AllowAttributeSubstitution: true,
			})
			if err != nil {
				return err
			}
			maps.Copy(newDataPaths, datapaths)
		}
		ds.DataPaths = newDataPaths
	}
	return nil
}

// func pathsSubstitute(ds *DataSource, attr map[string]any) error {
// 	//handle data source name substitution
// 	nameResult, err := parameterSubstitute(paramSubInput{
// 		TemplateKey:                "name", //this is the data source name, so we will not allow inflating into multiple paths.  key doesn't matter here
// 		Template:                   ds.Name,
// 		Attributes:                 attr,
// 		AllowAttributeSubstitution: true,
// 	})
// 	if err != nil {
// 		return err
// 	}
// 	if nameout, ok := nameResult["name"]; ok {
// 		ds.Name = nameout
// 	} else {
// 		return fmt.Errorf("invalid data source name substitution for %s", ds.Name)
// 	}

// 	//handle data source paths substitution
// 	for k, p := range ds.Paths {
// 		paths, err := parameterSubstitute(paramSubInput{
// 			TemplateKey:                k,
// 			Template:                   p,
// 			Attributes:                 attr,
// 			AllowAttributeSubstitution: true,
// 		})
// 		if err != nil {
// 			return err
// 		}
// 		delete(ds.Paths, k)
// 		maps.Copy(ds.Paths, paths)
// 	}

// 	//handle data source data paths substitution
// 	for k, p := range ds.DataPaths {
// 		datapaths, err := parameterSubstitute(paramSubInput{
// 			TemplateKey:                k,
// 			Template:                   p,
// 			Attributes:                 attr,
// 			AllowAttributeSubstitution: true,
// 		})
// 		if err != nil {
// 			return err
// 		}
// 		delete(ds.DataPaths, k)
// 		maps.Copy(ds.DataPaths, datapaths)
// 	}

// 	return nil
// }

// handleSliceSub iterates through a slice of values and performs parameter substitution
// on any string elements, returning a new []any slice containing the resolved values.
func handleSliceSub[T any](val []T, pm *PluginManager, attrSub bool) []any {
	newslice := []any{}
	for _, v := range val {
		if stringv, ok := any(v).(string); ok {
			newvals, err := parameterSubstitute(paramSubInput{
				TemplateKey:                "",
				Template:                   stringv,
				Attributes:                 pm.Attributes,
				AllowAttributeSubstitution: attrSub,
			})
			if err == nil {
				for _, v := range newvals {
					newslice = append(newslice, v)
				}
			}
		} else {
			newslice = append(newslice, v)
		}
	}
	return newslice
}

// EmbeddedVar represents a parsed substitution variable token extracted from a template string
// (for example, {ATTR::my_var[0]} or {ENV::MY_ENV}).
type EmbeddedVar struct {
	// Type is the variable namespace, such as "ATTR", "ENV", or "VAR".
	Type         string
	// Varname is the identifier of the variable within the given namespace.
	Varname      string
	// IsArrayOrMap indicates whether array or map indexing syntax (e.g., [], [0], ['key'], ["key"]) was specified.
	IsArrayOrMap bool
	// ArrayIndex is the non-negative array index when a numeric index is specified, or -1 if unindexed or a map key.
	ArrayIndex   int
	// MapIndex is the key string when single- or double-quoted map key indexing is specified.
	MapIndex     string
}

//func handleSubstitution()

// paramSubInput defines the input parameters for resolving template substitutions via parameterSubstitute.
type paramSubInput struct {
	// TemplateKey is the initial key associated with the template string in the output map.
	TemplateKey                string
	// Template is the raw string containing zero or more {TYPE::VARNAME...} substitution tokens.
	Template                   string
	// Attributes is the map of available attributes used to resolve {ATTR::...} tokens.
	Attributes                 map[string]any
	// AllowAttributeSubstitution controls whether {ATTR::...} tokens are evaluated or left unchanged.
	AllowAttributeSubstitution bool
}

// parameterSubstitute evaluates all {ENV::...} and (when permitted) {ATTR::...} placeholders
// in input.Template and returns a map of resolved key-value string pairs.
//
// When a placeholder references a slice or map using empty bracket syntax ([]), the output map
// is inflated into multiple entries whose keys are suffixed with "-<element_value>".
// When a slice is referenced with a specific numeric index ([i]), only that element is substituted.
// When a slice is referenced without bracket syntax, its elements are concatenated into a comma-separated string.
//
// @TODO how to handle case when array values are not annotated as arrays?  should concat!
func parameterSubstitute(input paramSubInput) (map[string]string, error) {

	//create a map for expanding the template into our output
	output := map[string]string{
		input.TemplateKey: input.Template,
	}

	result := substitutionRegex.FindAllStringSubmatch(input.Template, -1)
	for _, match := range result {
		eVars := matchToEmbeddedVars(match)

		//if this is not an auto sub value, then skip
		if _, ok := validAutoSubstitution[eVars.Type]; !ok {
			//skip
			continue
		}

		//if this is a disallowed ATTR substitution then skip (e.g. ATTR sub in the payload attributes)
		if eVars.Type == ParamSubAttr && !input.AllowAttributeSubstitution {
			//skip
			continue
		}
		val, err := getSubstitutionVal(eVars, input.Attributes)
		if err != nil {
			return nil, err
		}
		vof := reflect.ValueOf(val)
		switch vof.Kind() {
		case reflect.String:
			for outputkey, outputline := range output {
				output[outputkey] = strings.Replace(outputline, match[0], val.(string), -1)
			}
		case reflect.Slice:
			//-1 -> inflate the entire array into the parameter
			if eVars.ArrayIndex == -1 && eVars.IsArrayOrMap {
				newoutput := make(map[string]string)
				for outputkey, outputline := range output {
					for i := 0; i < vof.Len(); i++ {
						element := vof.Index(i)
						strval := fmt.Sprintf("%v", element.Interface())
						newoutput[outputkey+"-"+strval] = strings.Replace(outputline, match[0], strval, -1)
					}
				}
				output = newoutput

				//have an index.  substitute on the index
			} else if eVars.ArrayIndex > -1 && eVars.IsArrayOrMap {
				for outputkey, outputline := range output {
					element := vof.Index(eVars.ArrayIndex)
					strval := fmt.Sprintf("%v", element.Interface())
					output[outputkey] = strings.Replace(outputline, match[0], strval, -1)
				}

				//have a slice but the user referenced the var without array semantics
				//concatonate the slice into csv and substitute the csv string
			} else {
				//concat slice into csv and treat as string
				for outputkey, outputline := range output {
					builder := strings.Builder{}
					for i := 0; i < vof.Len(); i++ {
						if i > 0 {
							builder.WriteString(",")
						}
						element := vof.Index(i)
						strval := fmt.Sprintf("%v", element.Interface())
						builder.WriteString(strval)
					}
					output[outputkey] = strings.Replace(outputline, match[0], builder.String(), -1)
				}
			}
		case reflect.Map:
			//no map index, so inflate the entire map into the parameter
			if eVars.MapIndex == "" && eVars.IsArrayOrMap {
				newoutput := make(map[string]string)
				for outputkey, outputline := range output {
					for _, key := range vof.MapKeys() {
						element := vof.MapIndex(key)
						strval := fmt.Sprintf("%v", element.Interface())
						newoutput[outputkey+"-"+strval] = strings.Replace(outputline, match[0], strval, -1)
					}
				}
				output = newoutput

				//have a map index, so get the map value and substitute
			} else {
				for outputkey, outputline := range output {
					element := vof.MapIndex(reflect.ValueOf(eVars.MapIndex))
					strval := fmt.Sprintf("%v", element.Interface())
					output[outputkey] = strings.Replace(outputline, match[0], strval, -1)
				}
			}
		default:
			//handle same as a string, but coerce values to a string
			for outputkey, outputline := range output {
				output[outputkey] = strings.Replace(outputline, match[0], fmt.Sprintf("%v", val), -1)
			}
		}
		if err != nil {
			return nil, err
		}
	}
	return output, nil
}

// getSubstitutionVal retrieves the raw value for an EmbeddedVar from either the process environment
// (for type "ENV", split on commas into a string slice) or the provided payloadAttr map (for type "ATTR").
// It returns an error if an "ATTR" variable name does not exist in payloadAttr.
func getSubstitutionVal(evar EmbeddedVar, payloadAttr map[string]any) (any, error) {
	var returnval any
	switch evar.Type {
	case "ENV":
		//get the env var then try and split it with a comma separator
		//supported env values are single vals "1" or csv vals "one,two,three"
		returnval = strings.Split(os.Getenv(evar.Varname), ",")
	case "ATTR":
		val, ok := payloadAttr[evar.Varname]
		if !ok {
			return nil, fmt.Errorf("invalid attribute name: %s", evar.Varname)
		}
		returnval = val
	}
	return returnval, nil
}

// matchToEmbeddedVars converts a regex submatch slice produced by substitutionRegex
// into an EmbeddedVar struct with parsed type, variable name, and array/map index metadata.
func matchToEmbeddedVars(match []string) EmbeddedVar {

	ev := EmbeddedVar{
		Type:       match[1],
		Varname:    match[2],
		ArrayIndex: -1,
	}

	// If any of groups 3..6 matched, it's an array/map reference.
	switch {
	case match[3] != "":
		// matched [] (empty index)
		ev.IsArrayOrMap = true
	case match[4] != "":
		// matched [0] numeric array index
		ev.IsArrayOrMap = true
		i, err := strconv.Atoi(match[4])
		if err == nil {
			ev.ArrayIndex = i
		}
	case match[5] != "":
		// matched ['key'] single-quoted map key
		ev.IsArrayOrMap = true
		ev.MapIndex = match[5]
	case match[6] != "":
		// matched ["key"] double-quoted map key
		ev.IsArrayOrMap = true
		ev.MapIndex = match[6]
	}
	return ev
}

// templateVarSubstitution replaces {VAR::varname} placeholders in template with their
// corresponding string values from templateVars. Placeholders not found in templateVars are left unchanged.
func templateVarSubstitution(template string, templateVars map[string]string) string {
	result := substitutionRegex.FindAllStringSubmatch(template, -1)
	for _, match := range result {
		if match[1] == "VAR" {
			if val, ok := templateVars[match[2]]; ok {
				template = strings.Replace(template, match[0], val, 1)
			}
		}
	}
	return template
}
