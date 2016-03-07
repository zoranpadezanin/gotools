package file

import (
	"archive/zip"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
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
			header.Name += string(os.PathSeparator)
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

// UnZipIT unzips a file to the working folder
// and returns the file names in a slice
func UnZipIT(folder string, fileName string) ([]string, error) {
	// Open a zip archive for reading.
	r, err := zip.OpenReader(filepath.Join(folder, fileName))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	// Iterate through the files in the archive,
	var results []string
	for _, f := range r.File {
		results = append(results, f.Name)
		//fmt.Printf("Contents of %s:\n", f.Name)
		rc, err := f.Open()
		defer rc.Close()
		if err != nil {
			return nil, err
		}
		newf, err := os.Create(filepath.Join(folder, f.Name))
		if err != nil {
			return nil, err
		}
		defer newf.Close()
		_, err = io.Copy(newf, rc)
		if err != nil {
			return nil, err
		}
	}
	return results, err
}

// LoadProperties Loads JSON properties into a MAP,
// Use GetProperty, thereafter to get a named property
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

// Wget Downloads a file from google url.
// If you are using Google cloud Storage, ensure you pass in a google Client
func Wget(client *http.Client, folder string, filename string, url string) (string, error) {
	//ext := filepath.Ext(url)
	filePath := filepath.Join(folder, filename)
	os.Mkdir(folder, 0666)

	file, err := os.Create(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return "", err
	}
	return folder, nil
}

// CleanFolder start off by cleaning the desired folder of folders than X days
// To delete log files in a folder older than 2 days, call this function : file.CleanFolder("logs", 2)
func CleanFolder(root string, daystokeep int) error {
	err := filepath.Walk(root, visit(daystokeep))
	if err != nil {
		return err
	}
	return nil
}

func visit(daystokeep int) filepath.WalkFunc {
	return func(path string, f os.FileInfo, err error) error {
		dateToKeep := time.Now().AddDate(0, daystokeep*-1, 0)
		if f.ModTime().Before(dateToKeep) {
			err = os.Remove(f.Name())
			if err != nil {
				return err
			}
		}
		return nil
	}
}

/*
// ToMap Converts a structure to map
func ToMap(in interface{}, tag string) (map[string]interface{}), error){
    out := make(map[string]interface{})

    v := reflect.ValueOf(in)
    if v.Kind() == reflect.Ptr {
        v = v.Elem()
    }

    // we only accept structs
    if v.Kind() != reflect.Struct {
        return nil, fmt.Errorf("ToMap only accepts structs; got %T", v)
    }

    typ := v.Type()
    for i := 0; i < v.NumField(); i++ {
        // gets us a StructField
        fi := typ.Field(i)
        if tagv := fi.Tag.Get(tag); tagv != "" {
            // set key of map to value in struct field
            out[tagv] = v.Field(i).Interface()
        }
    }
    return out, nil
}
*/

/*
func readCSV(folder string, filename string) error {

	client, err := google.DefaultClient(context.Background(), storage.DevstorageFullControlScope)
	if err != nil {
		return err
	}
	bq, err := bigquery.New(client)
	if err != nil {
		log.Fatal(err)
	}

	csvfile, err := os.Open(filepath.Join(folder, filename))
	if err != nil {
		return err
	}
	rows := make([]*bigquery.TableDataInsertAllRequestRows, 0)
	reader := csv.NewReader(csvfile)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		// create a new row MAP
		row := &bigquery.TableDataInsertAllRequestRows{
			Json: make(map[string]bigquery.JsonValue, 0),
		}
		row.Json["SupplierID"] = record[0]
		row.Json["OrderDate"] = record[1]
		row.Json["Year"], _ = strconv.Atoi(record[2])
		row.Json["Month"], _ = strconv.Atoi(record[3])
		row.Json["Hour"], _ = strconv.Atoi(record[4])
		row.Json["Quarter"], _ = strconv.Atoi(record[5])
		row.Json["AccountID"] = record[6]
		row.Json["Name"] = record[7]
		row.Json["GroupCode"] = record[8]
		row.Json["GroupDescription"] = record[9]
		row.Json["RepCode"] = record[10]
		row.Json["RepName"] = record[11]
		row.Json["ProductID"] = record[12]
		row.Json["CategoryCode"] = record[13]
		row.Json["CategoryDescription"] = record[14]
		row.Json["Ordered"], _ = strconv.Atoi(record[15])
		row.Json["Delivered"], _ = strconv.ParseFloat(record[16], 64)
		row.Json["LineTotal"], _ = strconv.ParseFloat(record[17], 64)
		row.Json["Cost"], _ = strconv.ParseFloat(record[18], 64)

		rows = append(rows, row)

	}
	_ = "breakpoint"
	//Create a new map to hold the rows of data
	req := &bigquery.TableDataInsertAllRequest{
		Rows: rows,
	}
	call := bq.Tabledata.InsertAll("citric-optics-107909", "History", "LILGREEN_ProductHistory", req)
	resp, err := call.Do()
	if err != nil {
		return err
	}

	buf, _ := json.Marshal(resp)
	log.Print(string(buf))
	return nil
}
*/
