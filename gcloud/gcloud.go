package gcloud

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/CaboodleData/gotools/file"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/storage/v1"
)

// QueryBQ queries Bigquery and returns headers as a string array and rows as a multi dimension interface array
func QueryBQ(projectID string, queryStr string) ([]string, [][]interface{}, error) {
	client, err := newClient()
	if err != nil {
		return nil, nil, err
	}
	bq, err := bigquery.New(client)
	if err != nil {
		return nil, nil, err
	}

	query := &bigquery.QueryRequest{}
	query.Query = queryStr
	query.Kind = "json"
	query.MaxResults = 1000

	// call the query
	jobService := bigquery.NewJobsService(bq)
	rslt, err := jobService.Query(projectID, query).Do()
	if err != nil {
		return nil, nil, err
	}

	// get field headings into an array
	headers := make([]string, len(rslt.Schema.Fields))
	for i, f := range rslt.Schema.Fields {
		headers[i] = f.Name
	}

	// create rows into arry of interfaces
	rows := make([][]interface{}, len(rslt.Rows))
	// Create rows
	for i, tableRow := range rslt.Rows {
		row := make([]interface{}, len(rslt.Schema.Fields))
		// create columns
		for j, tableCell := range tableRow.F {
			schemaField := rslt.Schema.Fields[j]

			if schemaField.Type == "RECORD" {
				//TODO deal with nested columns as per https://github.com/dailyburn/bigquery/blob/master/client/client.go
				//	row[j] = c.nestedFieldsData(schemaField.Fields, tableCell.V)
			} else {
				row[j] = tableCell.V
			}
		}
		rows[i] = row
	}

	_ = "breakpoint"
	return headers, rows, nil

}

// CreateTableBQ Creates a table if it does not exist
// Create a schema file in json format
func CreateTableBQ(schemaFileName string, projectID string, datasetID string, tableID string) error {
	client, err := newClient()
	if err != nil {
		return err
	}
	bq, err := bigquery.New(client)
	if err != nil {
		log.Fatal(err)
	}
	tablesService := bigquery.NewTablesService(bq)

	table := &bigquery.Table{}
	f, err := ioutil.ReadFile(schemaFileName)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(f, &table); err != nil {
		return err
	}
	//
	table.TableReference.ProjectId = projectID
	table.TableReference.TableId = tableID
	table.TableReference.DatasetId = datasetID

	// Create the table
	_, err = tablesService.Insert(projectID, datasetID, table).Do()
	//buf, _ := json.Marshal(resp)
	//_ = "breakpoint"
	//fmt.Print(string(buf))
	return err
}

// JobStatusBQ Creates a job and returns the job ID
// Create a schema file in json format to use this method
// Uses the URI to load data into the mentioned table
func JobStatusBQ(projectID string, jobID string) (bool, error) {
	client, err := newClient()
	if err != nil {
		return false, err
	}
	bq, err := bigquery.New(client)
	if err != nil {
		return false, err
	}
	jobService := bigquery.NewJobsService(bq)
	rslt, _ := jobService.Get(projectID, jobID).Do()
	done := rslt.Status.State == "DONE"
	if rslt.Status.ErrorResult != nil {
		return done, errors.New(rslt.Status.ErrorResult.Message)
	}
	return done, nil

	//err := errors.New("emit macho dwarf: elf header corrupted")
	//return
}

// InsertJobBQ Creates a job and returns the job ID
// Create a schema file in json format to use this method
// Uses the URI to load data into the mentioned table
func InsertJobBQ(jobFileName string, projectID string, datasetID string, tableID string, uri string) (string, error) {
	client, err := newClient()
	if err != nil {
		return "", err
	}
	bq, err := bigquery.New(client)
	if err != nil {
		return "", err
	}
	jobService := bigquery.NewJobsService(bq)

	f, err := ioutil.ReadFile(jobFileName)
	if err != nil {
		return "", err
	}
	job := &bigquery.Job{}
	if err = json.Unmarshal(f, &job); err != nil {
		return "", err
	}
	// Update fields
	t := time.Now()
	job.JobReference.JobId = t.Format("20060102150405")
	job.JobReference.ProjectId = projectID
	job.Configuration.Load.DestinationTable.DatasetId = datasetID
	job.Configuration.Load.DestinationTable.ProjectId = projectID
	job.Configuration.Load.DestinationTable.TableId = tableID
	job.Configuration.Load.SourceUris = append(job.Configuration.Load.SourceUris, uri)

	// Create the job
	_, err = jobService.Insert(projectID, job).Do()
	return job.JobReference.JobId, err
}

// DeleteGS deletes file from a Google Storage Bucket.
// If the file is in a folder, then include the folder in the filename
func DeleteGS(bucketName string, fileName string) error {
	_, service, err := newStorageService()
	if err != nil {
		return err
	}
	err = service.Objects.Delete(bucketName, fileName).Do()
	return err
}

// CopyGS Copies file to a differnet folder
func CopyGS(srcBucketName string, srcObjectName string, destBucketName string, destObjectName string) error {
	_, service, err := newStorageService()
	if err != nil {
		return err
	}

	_, err = service.Objects.Copy(srcBucketName, srcObjectName, destBucketName, destObjectName, nil).Do()
	return err
}

// SendGS Sends file to a Google Storage Bucket
func SendGS(bucketName string, bucketFolder string, fileName string) error {
	_, service, err := newStorageService()
	if err != nil {
		return err
	}

	sfile := fileName[strings.LastIndex(fileName, string(os.PathSeparator))+1 : len(fileName)]
	var objectName string
	if bucketFolder == "" {
		objectName = sfile
	} else {
		objectName = bucketFolder + "/" + sfile
	}
	object := &storage.Object{Name: objectName}
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	_, err = service.Objects.Insert(bucketName, object).Media(file).Do()
	if err != nil {
		return err
	}

	err = os.Remove(fileName)
	return err
}

// DownloadGS downloads all files in a Google Storage bucket and returns a list of files downloaded
func DownloadGS(bucketName string, folder string, remove bool) ([]string, error) {
	client, service, err := newStorageService()
	if err != nil {
		return nil, err
	}

	// List all objects in a bucket using pagination
	var results []string
	pageToken := ""
	for {
		call := service.Objects.List(bucketName)
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}
		res, err := call.Do()
		if err != nil {
			return nil, err
		}

		for _, object := range res.Items {
			if res, err := service.Objects.Get(bucketName, string(object.Name)).Do(); err == nil {
				file.Wget(client, folder, res.Name, res.MediaLink)
				results = append(results, res.Name)
				// after downloading, should we delete the file
				if remove == true {
					service.Objects.Delete(bucketName, string(object.Name)).Do()
				}
			} else {
				return nil, err
			}
		}
		if pageToken = res.NextPageToken; pageToken == "" {
			break
		}
	}
	return results, nil
}

// ListBucketGS Lists files in a bucket
func ListBucketGS(bucketName string) ([]string, error) {
	_, service, err := newStorageService()
	if err != nil {
		return nil, err
	}

	// List all objects in a bucket using pagination
	var results []string
	pageToken := ""
	for {
		call := service.Objects.List(bucketName)
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}
		res, err := call.Do()
		if err != nil {
			return nil, err
		}

		for _, object := range res.Items {
			if res, err := service.Objects.Get(bucketName, string(object.Name)).Do(); err == nil {
				results = append(results, res.Name)
			} else {
				return nil, err
			}
		}
		if pageToken = res.NextPageToken; pageToken == "" {
			break
		}
	}
	// return a slice with file names
	return results, nil
}

func newStorageService() (*http.Client, *storage.Service, error) {
	client, err := newClient()
	if err != nil {
		return nil, nil, err
	}
	service, err := storage.New(client)
	if err != nil {
		return nil, nil, err
	}
	return client, service, nil
}

func newClient() (*http.Client, error) {
	client, err := google.DefaultClient(context.Background(), storage.DevstorageFullControlScope)
	return client, err
}
