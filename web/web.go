package web

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

//Get is to call a rest service via a GET method
func Get(url string, username string, password string) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "text/xml")
	resp, err := client.Do(req)
	if err != nil {
		return resp, err
	}
	if resp.StatusCode != 200 {
		return resp, errors.New(fmt.Sprint(resp))
	}
	return resp, nil
}

//Post is to post to a REST service
func Post(url string, body string, username string, password string) (*http.Response, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, strings.NewReader(body))
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "text/xml")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, errors.New(fmt.Sprint(resp))
	}
	return resp, nil
}

// Wget Downloads a file from google url.
// If you are using Google cloud Storage, ensure you pass in a google Client, otherwise you can pass in a usual http client
// eg. file.Wget(....)
func Wget(client *http.Client, folder string, filename string, url string) (string, error) {
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
