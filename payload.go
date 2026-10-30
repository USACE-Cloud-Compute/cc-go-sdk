package cc

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/usace-cloud-compute/filesapi"
)

// DataSourceIoType specifies the I/O direction of a DataSource (input, output, or all).
type DataSourceIoType string

const (
	// DataSourceInput indicates an input data source.
	DataSourceInput DataSourceIoType = "INPUT"

	// DataSourceOutput indicates an output data source.
	DataSourceOutput DataSourceIoType = "OUTPUT"

	// DataSourceAll indicates both input and output data sources (the zero value).
	DataSourceAll DataSourceIoType = "" //zero value == all

	// mbconversion is the conversion factor from bytes to megabytes (1024 * 1024).
	mbconversion int64 = 1024 * 1024

	// multipartThreshold is the file size threshold in megabytes (4000 MB / ~4 GB)
	// above which uploads switch to multipart mode for cloud stores such as S3.
	multipartThreshold int64 = 4000 //4GB...used for switching to multiport uploads for cloud stores like s3
)

// Payload represents the top-level Cloud Compute execution configuration.
// It embeds an IOManager holding global attributes, data stores, inputs, and outputs,
// along with a sequence of Actions to be executed by a plugin.
type Payload struct {
	IOManager
	Actions []Action `json:"actions"`
}

// Action represents an individual unit of work or task defined within a Payload.
// It embeds its own IOManager to provide action-scoped attributes, stores, inputs, and outputs,
// which can fall back to the parent Payload's IOManager when linked via SetParent.
type Action struct {
	IOManager
	Type        string `json:"type,omitempty"`
	Description string `json:"description,omitempty"`
	Name        string `json:"name,omitempty"`
}

// -----------------------------------------------
// Wrapped IOManager functions
// -----------------------------------------------

// GetStore retrieves a DataStore by name from the Action's embedded IOManager
// or its parent IOManager if not found locally.
func (a Action) GetStore(name string) (*DataStore, error) {
	return a.IOManager.GetStore(name)
}

// GetDataSource retrieves a DataSource matching the specified name and I/O type
// from the Action's embedded IOManager or its parent IOManager.
func (a Action) GetDataSource(input GetDsInput) (DataSource, error) {
	return a.IOManager.GetDataSource(input)
}

// GetInputDataSource retrieves an input DataSource by name from the Action's
// embedded IOManager or its parent IOManager.
func (a Action) GetInputDataSource(name string) (DataSource, error) {
	return a.IOManager.GetInputDataSource(name)
}

// GetOutputDataSource retrieves an output DataSource by name from the Action's
// embedded IOManager or its parent IOManager.
func (a Action) GetOutputDataSource(name string) (DataSource, error) {
	return a.IOManager.GetOutputDataSource(name)
}

// GetReader opens and returns an io.ReadCloser for reading data from an input DataSource
// via the Action's embedded IOManager. The caller is responsible for closing the returned reader.
func (a Action) GetReader(input DataSourceOpInput) (io.ReadCloser, error) {
	return a.IOManager.GetReader(input)
}

// Get reads and returns the entire contents of an input DataSource resource as a byte slice
// via the Action's embedded IOManager.
func (a Action) Get(input DataSourceOpInput) ([]byte, error) {
	return a.IOManager.Get(input)
}

// Put writes data from an io.Reader to an output DataSource resource via the Action's
// embedded IOManager, returning the number of bytes written.
func (a Action) Put(input PutOpInput) (int, error) {
	return a.IOManager.Put(input)
}

// Copy transfers data from a source DataSource to a destination DataSource
// via the Action's embedded IOManager.
func (a Action) Copy(src DataSourceOpInput, dest DataSourceOpInput) error {
	return a.IOManager.Copy(src, dest)
}

// CopyFileToLocal copies a file or directory from a remote input FileDataStore
// to the local filesystem via the Action's embedded IOManager.
func (a Action) CopyFileToLocal(input CopyToLocalInput) error {
	return a.IOManager.CopyFileToLocal(input)
}

// CopyFileToRemote copies a local file or directory to a remote FileDataStore
// via the Action's embedded IOManager.
func (a Action) CopyFileToRemote(input CopyFileToRemoteInput) error {
	return a.IOManager.CopyFileToRemote(input)
}

// -----------------------------------------------
// IOManager
// -----------------------------------------------

// IOManager manages attributes, data stores, and input/output data sources for a Payload or Action.
// It supports hierarchical lookup of stores and data sources through an optional parent IOManager.
type IOManager struct {
	Attributes PayloadAttributes `json:"attributes,omitempty"`
	Stores     []DataStore       `json:"stores"`
	Inputs     []DataSource      `json:"inputs"`
	Outputs    []DataSource      `json:"outputs"`
	parent     *IOManager
}

// GetDsInput defines the lookup criteria for retrieving a DataSource from an IOManager.
type GetDsInput struct {
	// DsIoType filters the search by data source direction (DataSourceInput, DataSourceOutput, or DataSourceAll).
	DsIoType DataSourceIoType
	// DsName is the name of the DataSource to retrieve.
	DsName string
}

// DataSourceOpInput specifies parameters for performing read, write, or copy operations on a DataSource.
type DataSourceOpInput struct {
	// DataSource is an optional direct pointer to a DataSource. If nil, DataSourceName is used to look it up.
	DataSource *DataSource
	// DataSourceName is the name of the DataSource to resolve when DataSource is nil.
	DataSourceName string
	// PathKey is the map key used to look up the target path within the DataSource's Paths map.
	PathKey string
	// DataPathKey is the optional map key used to look up an internal data path within the DataSource's DataPaths map.
	DataPathKey string
	// TemplateVars provides key-value pairs for substituting {VAR::key} placeholders in the resolved path.
	TemplateVars map[string]string
}

// KeyValue represents a generic name-value pair used for template variable substitution.
type KeyValue struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// PutOpInput specifies parameters for writing data from an io.Reader to a DataSource.
type PutOpInput struct {
	// SrcReader is the source stream containing the data to be written.
	SrcReader io.Reader
	DataSourceOpInput
}

// SetParent assigns a parent IOManager to enable hierarchical fallback lookups
// for data stores and data sources when they are not found in the current IOManager.
func (im *IOManager) SetParent(iom *IOManager) {
	im.parent = iom
}

// GetStore searches for a DataStore with the given name in the current IOManager.
// If not found locally and a parent IOManager is set, the search delegates recursively to the parent.
// It returns an error if no matching store is found in the hierarchy.
func (im *IOManager) GetStore(name string) (*DataStore, error) {
	for i, store := range im.Stores {
		if store.Name == name {
			return &im.Stores[i], nil
		}
	}
	if im.parent != nil {
		return im.parent.GetStore(name)
	}

	return nil, fmt.Errorf("invalid store name: %s", name)
}

// GetDataSource searches for a DataSource matching the name and DataSourceIoType specified in input.
// Depending on DsIoType, it searches Inputs, Outputs, or both. If not found locally and a parent
// IOManager is configured, it recursively searches the parent.
func (im *IOManager) GetDataSource(input GetDsInput) (DataSource, error) {
	sources := []DataSource{}
	switch input.DsIoType {
	case DataSourceInput:
		sources = im.Inputs
	case DataSourceOutput:
		sources = im.Outputs
	case DataSourceAll:
		sources = append(sources, im.Inputs...)
		sources = append(sources, im.Outputs...)
	}
	for _, ds := range sources {
		if input.DsName == ds.Name {
			return ds, nil
		}
	}
	if im.parent != nil {
		return im.parent.GetDataSource(input)
	}
	return DataSource{}, fmt.Errorf("data source %s not found", input.DsName)
}

// GetInputDataSource retrieves an input DataSource by name from the IOManager or its parent hierarchy.
func (im *IOManager) GetInputDataSource(name string) (DataSource, error) {
	return im.GetDataSource(GetDsInput{DataSourceInput, name})
}

// GetOutputDataSource retrieves an output DataSource by name from the IOManager or its parent hierarchy.
func (im *IOManager) GetOutputDataSource(name string) (DataSource, error) {
	return im.GetDataSource(GetDsInput{DataSourceOutput, name})
}

// GetAbsolutePath resolves and returns the cleaned full path for a named path key within a DataSource and DataStore.
// It combines the store's "root" parameter (defaulting to "/") with the path registered under pathname in the DataSource.
func (im *IOManager) GetAbsolutePath(storename string, sourcename string, pathname string) (string, error) {
	store, err := im.GetStore(storename)
	if err != nil {
		return "", fmt.Errorf("failed to get store: %s", err)
	}

	ds, err := im.GetDataSource(GetDsInput{
		DsIoType: DataSourceAll,
		DsName:   sourcename,
	})

	if err != nil {
		return "", fmt.Errorf("failed to get data source: %s", err)
	}

	root := store.Parameters.GetStringOrDefault("root", "/")
	if path, ok := ds.Paths[pathname]; ok {
		return filepath.Clean(fmt.Sprintf("%s%c%s", root, os.PathSeparator, path)), nil
	}
	return "", fmt.Errorf("invalid path name: %s", pathname)

}

// GetReader resolves an input DataSource and its backing DataStore, performs any requested template variable
// substitutions on the path, and returns an io.ReadCloser from the store's StoreReader session.
// If input.DataSource is nil, the input DataSource is looked up using input.DataSourceName.
// The caller is responsible for closing the returned io.ReadCloser.
func (im *IOManager) GetReader(input DataSourceOpInput) (io.ReadCloser, error) {
	var err error
	var dataSource DataSource
	if input.DataSource == nil {
		dataSource, err = im.GetInputDataSource(input.DataSourceName)
		if err != nil {
			return nil, err
		}
	} else {
		dataSource = *input.DataSource
	}

	dataStore, err := im.GetStore(dataSource.StoreName)
	if err != nil {
		return nil, err
	}
	if readerStore, ok := dataStore.Session.(StoreReader); ok {
		path := dataSource.Paths[input.PathKey]
		if len(input.TemplateVars) > 0 {
			path = templateVarSubstitution(path, input.TemplateVars)
		}
		datapath := ""
		if input.DataPathKey != "" {
			datapath = dataSource.DataPaths[input.DataPathKey]
		}
		return readerStore.Get(path, datapath)
	}
	return nil, fmt.Errorf("data store %s session does not implement a StoreReader", dataStore.Name)
}

// Get obtains a reader via GetReader and reads the entire resource into memory as a byte slice.
// Note: GetReader opens an underlying stream that is not closed by Get.
func (im *IOManager) Get(input DataSourceOpInput) ([]byte, error) {
	reader, err := im.GetReader(input)
	if err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(reader)
	return buf.Bytes(), nil
}

// Put writes data from input.SrcReader to the target output DataSource's backing DataStore.
// It resolves the output DataSource (when input.DataSource is nil), applies template variable
// substitutions to the target path, resolves any optional DataPathKey, and writes the data using
// the store's StoreWriter session. It returns the number of bytes written and any error encountered.
func (im *IOManager) Put(input PutOpInput) (int, error) {
	var err error
	var ds DataSource

	if input.DataSource == nil {
		ds, err = im.GetOutputDataSource(input.DataSourceName)
		if err != nil {
			return 0, err
		}
	}

	store, err := im.GetStore(ds.StoreName)
	if err != nil {
		return 0, err
	}

	if writer, ok := store.Session.(StoreWriter); ok {
		if path, ok := ds.Paths[input.PathKey]; ok {
			if len(input.TemplateVars) > 0 {
				path = templateVarSubstitution(path, input.TemplateVars)
			}
			datapath := ""
			if input.DataPathKey != "" {
				var dpok bool
				if datapath, dpok = ds.DataPaths[input.DataPathKey]; !dpok {
					return 0, fmt.Errorf("expected data source data path %s not found", input.DataPathKey)
				}
			}
			return writer.Put(input.SrcReader, path, datapath)
		}
		return 0, fmt.Errorf("data source path %s not found", input.PathKey)
	}
	return 0, fmt.Errorf("data store %s session does not implement a storewriter", ds.StoreName)
}

// Copy streams data from a source output DataSource to a destination output DataSource.
// The source DataStore session must implement StoreReader and the destination DataStore session
// must implement StoreWriter.
func (im *IOManager) Copy(src DataSourceOpInput, dest DataSourceOpInput) error {
	srcds, err := im.GetOutputDataSource(src.DataSourceName)
	if err != nil {
		return err
	}

	srcstore, err := im.GetStore(srcds.StoreName)
	if err != nil {
		return err
	}

	destds, err := im.GetOutputDataSource(dest.DataSourceName)
	if err != nil {
		return err
	}

	deststore, err := im.GetStore(destds.StoreName)
	if err != nil {
		return err
	}

	if srcReader, ok := srcstore.Session.(StoreReader); ok {
		if destwriter, ok := deststore.Session.(StoreWriter); ok {

			//get the reader
			srcpath := srcds.Paths[src.PathKey]
			srcdatapath := ""
			if src.DataPathKey != "" {
				srcdatapath = srcds.DataPaths[src.DataPathKey]
			}
			reader, err := srcReader.Get(srcpath, srcdatapath)
			if err != nil {
				return err
			}

			//write
			destpath := destds.Paths[dest.PathKey]
			destdatapath := ""
			if dest.DataPathKey != "" {
				destdatapath = destds.DataPaths[dest.DataPathKey]
			}
			_, err = destwriter.Put(reader, destpath, destdatapath)
			return err
		}
		return fmt.Errorf("destination data store %s session does not implement a storewriter", srcstore.Name)
	}
	return fmt.Errorf("source data store %s session does not implement a storereader", srcstore.Name)
}

// CopyToLocalInput specifies parameters for copying a file or directory from an input DataSource
// to the local filesystem.
type CopyToLocalInput struct {
	// DsName is the name of the input DataSource to copy from.
	DsName string
	// PathKey is the key in the DataSource's Paths map identifying the remote file or directory.
	PathKey string
	// LocalPath is the destination directory path on the local filesystem.
	LocalPath string
}

// CopyFileToLocal copies a single file or recursively walks and copies a directory prefix
// from an input DataSource's backing FileDataStore to the local directory specified by input.LocalPath.
func (im *IOManager) CopyFileToLocal(input CopyToLocalInput) error {
	ds, err := im.GetDataSource(GetDsInput{DataSourceInput, input.DsName})
	if err != nil {
		return err
	}

	store, err := im.GetStore(ds.StoreName)
	if err != nil {
		return err
	}

	relativePath, ok := ds.Paths[input.PathKey]
	if !ok {
		return fmt.Errorf("pathkey '%s' not found", input.PathKey)
	}

	ifds, ok := store.Session.(FileDataStore)
	if !ok {
		return fmt.Errorf("data store %s is not a filestore", input.DsName)
	}

	fullpath := ifds.GetAbsolutePath(relativePath)
	fstore := ifds.GetFilestore()

	_, err = fstore.GetObjectInfo(filesapi.PathConfig{Path: fullpath})
	if err == nil {
		//file exists...copy it
		return copyToLocal(fstore, fullpath, input.LocalPath, false)
	}
	log.Printf("failed to get object info: %s.", err)

	//assume its a dir/prefix and attempt to walk
	storeRoot := ""
	if sr, ok := store.Parameters["root"]; ok {
		if srstring, ok := sr.(string); ok {
			storeRoot = strings.TrimLeft(srstring, "/")
		}
	}

	//walk the directory structure
	return fstore.Walk(filesapi.WalkInput{
		Path: filesapi.PathConfig{Path: fullpath},
	}, func(path string, file os.FileInfo) error {
		f := file.Name()
		rootPrefix := fmt.Sprintf("%s/%s", storeRoot, relativePath)
		localPath := strings.Replace(f, rootPrefix, input.LocalPath, 1)
		return copyToLocal(fstore, f, localPath, true)
	})
}

// copyToLocal downloads a single object from fs at remoteAbsolutePath and writes it to the local filesystem.
// It creates any necessary parent directories before creating the local file.
func copyToLocal(fs filesapi.FileStore, remoteAbsolutePath string, localPath string, isdir bool) error {
	reader, err := fs.GetObject(filesapi.GetObjectInput{
		Path: filesapi.PathConfig{Path: remoteAbsolutePath},
	})
	if err != nil {
		return err
	}
	defer reader.Close()

	if isdir {
		localPath = filepath.Dir(localPath)
	}
	err = os.MkdirAll(localPath, 0755)
	if err != nil {
		return fmt.Errorf("failed to create local directory %s: %s", localPath, err)
	}

	localfile := fmt.Sprintf("%s/%s", localPath, filepath.Base(remoteAbsolutePath))
	writer, err := os.Create(localfile)
	if err != nil {
		return err
	}
	defer writer.Close()
	_, err = io.Copy(writer, reader)
	return err
}

// CopyFileToRemoteInput specifies parameters for uploading a local file or directory to a remote FileDataStore.
// It supports two configuration modes:
//
//  1. Direct store configuration:
//     - RemoteStoreName: Name of the target DataStore.
//     - RemotePath: Relative path from the store root to the destination resource.
//     - LocalPath: Local file or directory path to copy.
//
//  2. Output DataSource configuration (when RemoteStoreName is empty):
//     - RemoteDsName: Name of the output DataSource to copy to.
//     - DsPathKey: Key in the DataSource's Paths map.
//     - DsDataPathKey: Optional key in the DataSource's DataPaths map.
//     - LocalPath: Local file or directory path to copy.
type CopyFileToRemoteInput struct {
	RemoteStoreName string //optional store name
	RemotePath      string
	LocalPath       string
	RemoteDsName    string
	DsPathKey       string
	DsDataPathKey   string
	TemplateVars    map[string]string
}

// CopyFileToRemote uploads a local file or directory (recursively) to a remote FileDataStore.
// The destination can be specified either directly by store name and path, or via an output DataSource
// and path key. Any {VAR::key} placeholders in the destination path are replaced using input.TemplateVars.
func (im *IOManager) CopyFileToRemote(input CopyFileToRemoteInput) error {
	storeName := input.RemoteStoreName
	path := input.RemotePath
	if storeName == "" {
		//get store name from datasource and use datasource semantics
		ds, err := im.GetDataSource(GetDsInput{DataSourceOutput, input.RemoteDsName})
		if err != nil {
			return err
		}
		storeName = ds.StoreName
		path = ds.Paths[input.DsPathKey]
	}

	for k, valStr := range input.TemplateVars {
		varStr := fmt.Sprintf("{VAR::%s}", k)
		path = strings.ReplaceAll(path, varStr, valStr)
	}

	store, err := im.GetStore(storeName)
	if err != nil {
		return err
	}

	info, err := os.Stat(input.LocalPath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("Path '%s' does not exist.\n", path)
			return err
		}
		fmt.Printf("Error getting file info for '%s': %v\n", path, err)
		return err
	}

	if ifds, ok := store.Session.(FileDataStore); ok {
		fullRemotePath := ifds.GetAbsolutePath(path)
		fs := ifds.GetFilestore()
		if info.IsDir() {
			localFs, err := filesapi.NewFileStore(filesapi.BlockFSConfig{})
			if err != nil {
				return err
			}
			localFs.Walk(filesapi.WalkInput{
				Path: filesapi.PathConfig{Path: input.LocalPath},
			}, func(path string, file os.FileInfo) error {
				if !file.IsDir() {
					localRelativePath := strings.TrimPrefix(strings.TrimPrefix(path, input.LocalPath), "/")
					fullRemoteFilePath := fmt.Sprintf("%s/%s", fullRemotePath, localRelativePath)
					return writeFileToRemote(fs, path, fullRemoteFilePath)
				}
				return nil
			})
		} else {
			return writeFileToRemote(fs, input.LocalPath, fullRemotePath)
		}
	}
	return nil
}

// writeFileToRemote uploads a single local file at localPath to remoteAbsolutePath in fs.
// Files exceeding multipartThreshold (~4 GB) are uploaded using multipart transfer.
func writeFileToRemote(fs filesapi.FileStore, localPath string, remoteAbsolutePath string) error {
	fileInfo, err := os.Stat(localPath)
	if err != nil {
		return err
	}

	// Size() returns size in bytes
	sizeInMb := fileInfo.Size() / mbconversion
	useMultipart := sizeInMb > multipartThreshold

	reader, err := os.Open(localPath)
	if err != nil {
		return err
	}

	_, err = fs.PutObject(filesapi.PutObjectInput{
		Source: filesapi.ObjectSource{
			Reader: reader,
		},
		Dest:     filesapi.PathConfig{Path: remoteAbsolutePath},
		Mutipart: useMultipart,
	})
	return err
}

// GetStoreAs searches mgr.Stores for a DataStore with the given name and type-asserts
// its Session to the requested type T. It returns an error if the store is not found
// or if its Session cannot be asserted to type T.
func GetStoreAs[T any](mgr *IOManager, name string) (T, error) {
	for _, s := range mgr.Stores {
		if s.Name == name {
			if t, ok := s.Session.(T); ok {
				return t, nil
			} else {
				return t, fmt.Errorf("invalid store type: %s", s.StoreType)
			}
		}
	}
	var t T
	return t, fmt.Errorf("session %s does not exist", name)
}
