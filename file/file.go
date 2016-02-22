package file

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/storage/v1"
)

// ZipIT This will ZIP a file and delete the source file after zip
func ZipIT(source string, target string) error {
	zipfile, err := os.Create(target)
	if err != nil {
		return err
	}
	defer zipfile.Close()

	archive := zip.NewWriter(zipfile)
	defer archive.Close()
	info, err := os.Stat(source)
	if err != nil {
		return nil
	}

	var baseDir string
	if info.IsDir() {
		baseDir = filepath.Base(source)
	}

	filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}
		if baseDir != "" {
			header.Name = filepath.Join(baseDir, strings.TrimPrefix(path, source))
		}
		if info.IsDir() {
			header.Name += "/"
		} else {
			header.Method = zip.Deflate
		}
		writer, err := archive.CreateHeader(header)
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		_, err = io.Copy(writer, file)
		return err
	})
	err = os.Remove(source)
	return err
}

// LoadProperties Loads JSON properties into a MAP, Use GetProperty to get a named property
func LoadProperties(fileName string) (map[string]interface{}, error) {
	b, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}
	var data interface{}
	json.Unmarshal(b, &data)
	m := data.(map[string]interface{})
	return m, err
}

/*
// SaveProperties Saves properties back to JSON file
func SaveProperties(fileName string, props map[string]interface{}) error {
	b := json.Marshal(props)

}
*/

// GetProperty Gets a string property by name
func GetProperty(props map[string]interface{}, name string) string {
	value, ok := props[name].(string)
	if !ok {
		log.Fatal("No supplierID in properties")
	}
	return value
}

// InitLogs Initialise log files
func InitLogs(handler io.Writer) (*log.Logger, *log.Logger) {
	infoLog := log.New(handler, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	errorLog := log.New(handler, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
	return infoLog, errorLog
}

/*

file, err := os.OpenFile("file.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
if err != nil {
    log.Fatalln("Failed to open log file", output, ":", err)
}
*/

// SendGS Sends file to a Google Storage Bucket, then deletes it
func SendGS(fileName string, bucketName string) error {
	sfile := fileName[strings.LastIndex(fileName, "/")+1 : len(fileName)]
	client, err := google.DefaultClient(context.Background(), storage.DevstorageFullControlScope)
	if err != nil {
		log.Fatalf("Unable to get default client: %v", err)
	}
	service, err := storage.New(client)
	if err != nil {
		log.Fatalf("Unable to create storage service: %v", err)
	}
	object := &storage.Object{Name: sfile}
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal("Error opening file", err.Error())
	}
	if res, err := service.Objects.Insert(bucketName, object).Media(file).Do(); err == nil {
		fmt.Printf("Created object %v at location %v\n\n", res.Name, res.SelfLink)
	} else {
		log.Fatal("Error sending ZIP file", err.Error())
	}
	return os.Remove(fileName)
}
