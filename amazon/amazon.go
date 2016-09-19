// http://docs.aws.amazon.com/sdk-for-go/api/

package amazon

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/CaboodleData/gotools/file"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/ses"
	"github.com/aws/aws-sdk-go/service/swf"
)

//SESSendEmail use this to send an email fo SES
func SESSendEmail(from string, to string, subject string, message string) error {
	svc := ses.New(session.New(&aws.Config{Region: aws.String("us-east-1")}))

	params := &ses.SendEmailInput{
		Destination: &ses.Destination{ // Required
			ToAddresses: []*string{
				aws.String(to), // Required
			},
		},
		Message: &ses.Message{ // Required
			Body: &ses.Body{ // Required
				Html: &ses.Content{
					Data: aws.String(message), // Required
				},
				Text: &ses.Content{
					Data: aws.String(message), // Required
				},
			},
			Subject: &ses.Content{ // Required
				Data: aws.String(subject), // Required
			},
		},
		Source: aws.String(from), // Required
		ReplyToAddresses: []*string{
			aws.String(from), // Required
		},
		ReturnPath: aws.String(from),
		//ReturnPathArn: aws.String("AmazonResourceName"),
		//SourceArn:     aws.String("AmazonResourceName"),
	}
	resp, err := svc.SendEmail(params)
	fmt.Println(resp)
	if err != nil {
		fmt.Println(err.Error())
	}
	return err

}

// SWFNewSession gets new session
func SWFNewSession(id string, secret string) *swf.SWF {
	sess := session.New(&aws.Config{Region: aws.String("us-east-1"), Credentials: credentials.NewStaticCredentials(id, secret, "")})
	return swf.New(sess)
}

// SWFStartWorkflow starts a new workflow
func SWFStartWorkflow(svc *swf.SWF, domainName string, workflowName string, version string, input string, tags []string, tasklist string) (*string, error) {
	// convert TagList
	var awstags []*string
	for _, row := range tags {
		awstags = append(awstags, aws.String(row))
	}
	//svc := swf.New(session.New())
	id := workflowName + time.Now().Format("20060102150405")
	params := &swf.StartWorkflowExecutionInput{
		Domain:     aws.String(domainName), // Required
		WorkflowId: aws.String(id),         // Required
		WorkflowType: &swf.WorkflowType{ // Required
			Name:    aws.String(workflowName), // Required
			Version: aws.String(version),      // Required
		},
		//ChildPolicy:                  aws.String("ChildPolicy"),
		//ExecutionStartToCloseTimeout: aws.String("DurationInSecondsOptional"),
		Input: aws.String(input),
		//LambdaRole: aws.String("Arn"),
		TagList: awstags,
		//TagList: []*string{
		//	aws.String(tag), // Required
		//},
		TaskList: &swf.TaskList{
			Name: aws.String(tasklist), // Required
		},
		//TaskPriority:            aws.String("TaskPriority"),
		//TaskStartToCloseTimeout: aws.String("DurationInSecondsOptional"),
	}
	resp, err := svc.StartWorkflowExecution(params)
	return resp.RunId, err
}

// SWFPollForActivity will poll for up to 10 minutes for the job to load, there after will cancel out.
// cdecider will schedule the loadcompleted activity under a tasklist for with the supplierid
func SWFPollForActivity(svc *swf.SWF, domain string, tasklist string, supplierID string, Info *log.Logger, onComplete func(taskname string, input string, tasktoken string)) error {
	params := &swf.PollForActivityTaskInput{
		Domain: aws.String(domain), //
		TaskList: &swf.TaskList{ //
			Name: aws.String(tasklist), //
		},
		Identity: aws.String(supplierID),
	}

	// loop for 10 minutes, if no response then error out
	for i := 0; i < 10; i++ {
		resp, err := svc.PollForActivityTask(params)
		if err != nil {
			return err
		}

		// if we receive a task token then 60 second time out occured so try again
		if resp.TaskToken != nil {
			if *resp.TaskToken != "" {
				_ = "breakpoint"
				onComplete("", *resp.Input, *resp.TaskToken)
				return nil
			}
		}
		Info.Printf("Wait another minute...> %v ", i)
	}
	return errors.New("No response from " + domain + ", check the helpdesk as data is not loaded, then try cdump again...")
}

// SWFCompleteActivity - completes the activity
func SWFCompleteActivity(swfsvc *swf.SWF, tt string, result string) error {
	//swfsvc := swf.New(session.New(), &aws.Config{Region: aws.String("us-east-1")})
	params := &swf.RespondActivityTaskCompletedInput{
		Result:    aws.String(result),
		TaskToken: aws.String(tt),
	}
	_, err := swfsvc.RespondActivityTaskCompleted(params)
	return err

}

// SWFCancelActivity - completes the activity
func SWFCancelActivity(tt string, details string) error {
	swfsvc := swf.New(session.New(), &aws.Config{Region: aws.String("us-east-1")})
	params := &swf.RespondActivityTaskCanceledInput{
		TaskToken: aws.String(tt),
		Details:   aws.String(details),
	}
	_, err := swfsvc.RespondActivityTaskCanceled(params)
	return err
}

// S3SendFile bla
func S3SendFile(id string, secret string, keyName string, bucketName string, file io.Reader) error {
	sess := session.New(&aws.Config{Region: aws.String("us-east-1"), Credentials: credentials.NewStaticCredentials(id, secret, "")})
	uploader := s3manager.NewUploader(sess)

	// Upload input parameters
	upParams := &s3manager.UploadInput{
		Bucket: &bucketName,
		Key:    &keyName,
		Body:   file,
	}

	_, err := uploader.Upload(upParams)
	return err

}

// S3Download a file from S3 buy sending in the buckey and key to download
// This needs default credentials to be setup
func S3Download(bucket string, objectKey string) error {
	//sess := session.New(&aws.Config{Region: aws.String("us-east-1"), Credentials: credentials.NewStaticCredentials(id, secret, "")})
	sess := session.New(&aws.Config{Region: aws.String("us-east-1")})
	dfile, err := os.Create(objectKey)
	if err != nil {
		log.Fatal("Failed to create file", err)
	}
	defer dfile.Close()

	downloader := s3manager.NewDownloader(sess)
	_, err = downloader.Download(dfile,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(objectKey),
		})
	return err
}

// S3DownloadUnMarshal downloads a file from S3, but also unmarshals into a structure
// This needs default credentials to be setup
func S3DownloadUnMarshal(bucket string, objectKey string, s interface{}) error {
	//sess := session.New(&aws.Config{Region: aws.String("us-east-1"), Credentials: credentials.NewStaticCredentials(id, secret, "")})
	sess := session.New(&aws.Config{Region: aws.String("us-east-1")})

	dfile, err := os.Create(objectKey)
	if err != nil {
		log.Fatal("Failed to create file", err)
	}
	defer dfile.Close()

	downloader := s3manager.NewDownloader(sess)
	_, err = downloader.Download(dfile,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(objectKey),
		})
	if err != nil {
		return err
	}
	if err = file.LoadJSON(dfile.Name(), &s); err != nil {
		return err
	}
	err = os.Remove(dfile.Name())
	return err
}
