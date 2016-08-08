package workflow

import (
	"encoding/json"
	"log"

	"github.com/CaboodleData/gotools/file"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/swf"
)

//Activity structure holds the required data to deal with our workflow
type Activity struct {
	svc         *swf.SWF
	tt          string // task token associated with this decision
	input       string
	name        string
	ProjectID   string
	swfDomain   string
	swfTasklist string
	swfIdentity string
}

var (
	Info  *log.Logger
	Error *log.Logger
)

// NewActivity sets up the struc
func NewActivity(swfDomain string, swfTasklist string, swfIdentity string) *Activity {
	a := &Activity{
		swfDomain:   swfDomain,
		swfTasklist: swfTasklist,
		swfIdentity: swfIdentity,
	}
	return a
}

//StartPolling start the polling, ensure to pass in the call back function to handle the activity
func (a *Activity) StartPolling(stdout bool, logfolder string, handleActivity func(name string, input string) (result string, err error)) error {
	Info, Error = file.InitLogs(stdout, logfolder, a.swfTasklist)
	Info.Println("Starting " + a.swfIdentity + " ==>")
	swfsvc := swf.New(session.New(), &aws.Config{Region: aws.String("us-east-1")})

	params := &swf.PollForActivityTaskInput{
		Domain: aws.String(a.swfDomain), //
		TaskList: &swf.TaskList{ //
			Name: aws.String(a.swfTasklist), //
		},
		Identity: aws.String(a.swfIdentity),
	}
	Info.Println(params)

	// loop forever while polling for work
	x := 0
	for {
		resp, err := swfsvc.PollForActivityTask(params)
		if err != nil {
			Error.Fatalf("error: unable to poll for decision: %v\n", err)
		}

		// if we do not receive a task token then 60 second time out occured so try again
		//_ = "breakpoint"
		if resp.TaskToken != nil {
			if *resp.TaskToken != "" {
				Info, Error = file.InitLogs(stdout, logfolder, a.swfTasklist) // so that we update log file date
				a.svc = swfsvc
				a.tt = *resp.TaskToken
				a.input = *resp.Input
				a.name = *resp.ActivityType.Name

				result, err := handleActivity(a.name, a.input)
				if err != nil {
					Info.Printf("Error sending POD: \n" + a.input)
					a.TaskFailed(err.Error())
				} else {
					a.TaskCompleted(result)
				}
			}
		} else {
			// Every 20 minutes check in, just so we have some log activity
			x++
			if x == 20 {
				Info.Printf("debug - no activity required\n")
				x = 0
			}
		}
	}
}

// TaskFailed is used to complete to fail this activity so the decider can take action
func (a *Activity) TaskFailed(reason string) error {
	Info.Printf("Setting task as failed %s", a.name)
	faiparams := &swf.RespondActivityTaskFailedInput{
		Reason:    aws.String(reason),
		TaskToken: aws.String(a.tt),
	}
	_, err := a.svc.RespondActivityTaskFailed(faiparams)
	if err != nil {
		return err
	}
	return nil
}

// TaskCompleted is used to complete this activity so the decider moves onto the next step
func (a *Activity) TaskCompleted(result string) error {
	Info.Printf("Setting task as completed %s", a.name)
	comparams := &swf.RespondActivityTaskCompletedInput{
		Result:    aws.String(result),
		TaskToken: aws.String(a.tt),
	}
	_, err := a.svc.RespondActivityTaskCompleted(comparams)
	if err != nil {
		return err
	}
	return nil
}

// getJSON handy function into a map
func (a *Activity) getJSON(input string) map[string]interface{} {
	var data interface{}
	json.Unmarshal([]byte(input), &data)
	m := data.(map[string]interface{})
	return m
}
