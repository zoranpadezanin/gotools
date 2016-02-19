package sql

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

// DownloadCSV blh bl
func DownloadCSV(props map[string]interface{}) (l string, p string) {
	// Get properties
	connString := getProperty(props, "connectionString")
	nextmonth := getProperty(props, "nextmonth")
	supplierid := getProperty(props, "supplierid")
	filename := getProperty(props, "name")
	workingfolder := getProperty(props, "workingfolder")

	//get csv file Name
	t := time.Now()
	timestamp := t.Format("20060102150405")
	csvfilename := workingfolder + "/" + supplierid + "_" + filename + "_" + timestamp + ".csv"

	//create our temp folder
	os.Mkdir(workingfolder, 0777)

	// Open SQL connection
	db, err := sql.Open("mssql", connString)
	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
	}
	defer db.Close()

	//Get SQL & replace <lastyear> with last date
	sql := loadSQL()
	sql = strings.Replace(sql, "<nextmonth>", nextmonth, -1)

	type ProductHistory struct {
		SupplierID          string
		OrderDate           string
		Year                int32
		Month               int32
		Hour                int32
		Quarter             int32
		AccountID           string
		Name                string
		GroupCode           string
		GroupDecription     string
		RepCode             string
		RepName             string
		ProductID           string
		CategoryCode        string
		CategoryDescription string
		Ordered             int32
		Delivered           int32
		LineTotal           float32
		Cost                float32
	}

	//Get rows and load into structure
	rows, err := db.Query(sql)
	defer rows.Close()

	//open csv filename
	csvfile, err := os.Create(csvfilename)
	if err != nil {
		fmt.Println("Error:", err)
		return nil, nil
	}
	defer csvfile.Close()
	writer := csv.NewWriter(csvfile)

	// Loop to write CSV file
	for rows.Next() {
		//err = rows.Scan(&rslt.SupplierID, &rslt.OrderDate, &rslt.Year, &rslt.Month, &rslt.Hour, &rslt.Quarter, &rslt.AccountID, &rslt.Name, &rslt.GroupCode, &rslt.GroupDecription, &rslt.RepCode, &rslt.RepName, &rslt.ProductID, &rslt.CategoryCode, &rslt.CategoryDescription, &rslt.Ordered, &rslt.Delivered, &rslt.LineTotal, &rslt.Cost)
		var row [19]string
		err = rows.Scan(&row[0], &row[1], &row[2], &row[3], &row[4], &row[5], &row[6], &row[7], &row[8], &row[9], &row[10], &row[11], &row[12], &row[13], &row[14], &row[15], &row[15], &row[16], &row[17])
		if err != nil {
			log.Fatal("Scan failed:", err.Error())
		}
		err := writer.Write(row[:])
		if err != nil {
			fmt.Println("Error:", err)
			return nil, nil
		}
	}
	// Close CSV File
	writer.Flush()
	if err := writer.Error(); err != nil {
		log.Fatal(err)
	}
	return csvfilename, nextmonth
}
