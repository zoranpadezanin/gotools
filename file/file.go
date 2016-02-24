package file

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/storage/v1"
)

var wg sync.WaitGroup

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

// UnZipIT unzips a file to the current folder
// and returns the file names in a slice
func UnZipIT(fileName string) ([]string, error) {
	// Open a zip archive for reading.
	r, err := zip.OpenReader(fileName)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	// Iterate through the files in the archive,
	// printing some of their contents.
	var results []string
	for _, f := range r.File {
		results = append(results, f.Name)
		/*
		   	fmt.Printf("Contents of %s:\n", f.Name)
		       rc, err := f.Open()
		       if err != nil {
		           return "", err
		       }
		       _, err = io.CopyN(os.Stdout, rc, 68)
		       if err != nil {
		           return "", err
		       }
		       rc.Close()
		       fmt.Println()
		*/
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

// DownloadGS downloads all files in a Google Storage bucket and returns a list of files downloaded
func DownloadGS(bucketName string, folder string) ([]string, error) {
	client, err := google.DefaultClient(context.Background(), storage.DevstorageFullControlScope)
	if err != nil {
		return nil, err
	}
	service, err := storage.New(client)
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
				DownloadFile(folder, res.Name, res.MediaLink)

				fmt.Println("%s", res.MediaLink)
				results = append(results, res.Name)
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

// DownloadFile Downloads a file from a URL
func DownloadFile(folder string, filename string, url string) (string, error) {
	//ext := filepath.Ext(url)

	file_path := folder + "/" + filename

	os.Mkdir(folder, 0666)

	file, err := os.Create(file_path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	res, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	file_content, err := ioutil.ReadAll(res.Body)

	if err != nil {
		return "", err
	}

	// returns file size and err
	_, err = file.Write(file_content)

	if err != nil {
		return "", err
	}

	return folder, nil
}

// DownloadFile Downloads a file from a URL
func DownloadFileo(folder string, filename string, url string) {
	/*
		out, err := os.Create(folder + "/" + filename)
		defer out.Close()
		resp, err := http.Get(url)
		defer resp.Body.Close()
		//_, err = io.Copy(out, resp.Body)
		file_content, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return err
		}
		return nil
	*/
	_ = "breakpoint"
	res, _ := http.Head(url) // 187 MB file of random numbers per line
	maps := res.Header
	length, _ := strconv.Atoi(maps["Content-Length"][0]) // Get the content length from the header request
	limit := 3                                           // 10 Go-routines for the process so each downloads 18.7MB
	lenSub := length / limit                             // Bytes for each Go-routine
	diff := length % limit                               // Get the remaining for the last request
	body := make([]string, 11)                           // Make up a temporary array to hold the data to be written to the file
	for i := 0; i < limit; i++ {
		wg.Add(1)

		min := lenSub * i       // Min range
		max := lenSub * (i + 1) // Max range

		if i == limit-1 {
			max += diff // Add the remaining bytes in the last request
		}

		go func(min int, max int, i int) {
			client := &http.Client{}
			req, _ := http.NewRequest("GET", url, nil)
			rangeHeader := "bytes=" + strconv.Itoa(min) + "-" + strconv.Itoa(max-1) // Add the data for the Range header of the form "bytes=0-100"
			req.Header.Add("Range", rangeHeader)
			resp, _ := client.Do(req)
			defer resp.Body.Close()
			reader, _ := ioutil.ReadAll(resp.Body)
			body[i] = string(reader)
			ioutil.WriteFile(strconv.Itoa(i), []byte(string(body[i])), 0x777) // Write to the file i as a byte array
			wg.Done()
			//          ioutil.WriteFile("new_oct.png", []byte(string(body)), 0x777)
		}(min, max, i)
	}
	wg.Wait()
}
