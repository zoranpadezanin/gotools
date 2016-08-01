// godebug run -instrument github.com/CaboodleData/gotools/workflow rapidDecider.go -stdout
// env GOOS=linux GOARCH=386 go build -v rapidDecider.go
// scp -i '/Users/shaun/Google Drive/Keys for Encryption etc/Amazon/RapidtradeDB3.pem' rapidDecider ubuntu@52.204.248.107:/opt/rapidDecider

package workflow

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/CaboodleData/gotools/amazon"
	"github.com/CaboodleData/gotools/file"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/swf"
)

type result struct {
	SupplierID string
	File       string
}

//Globals variables
var (
	swfDomain   = "Orders"
	stdout      bool
	helpdesk    = "shaun@rapidtrade.biz"
	swfTasklist = "OrderDecider"
	swfIdentity = "RapidDecider"
)

//Decider structure holds the required data to deal with our workflow
type Decider struct {
	svc              *swf.SWF
	tt               string // task token associated with this decision
	input            string
	name             string
	runid            string //Workflows runid
	workflowid       string //
	ProjectID        string
	swfDomain        string
	swfTasklist      string
	swfIdentity      string
	swfFirstActivity string
}

// NextActivity bla
type NextActivity struct {
	name       string
	version    string
	input      string
	stcTimeout string
	tasklist   string
	context    string
	complete   bool
}

// NewDecider sets up the struc
func NewDecider(swfDomain string, swfTasklist string, swfIdentity string, swfFirstActivity string) *Decider {
	d := &Decider{
		swfDomain:        swfDomain,
		swfTasklist:      swfTasklist,
		swfIdentity:      swfIdentity,
		swfFirstActivity: swfFirstActivity,
	}
	return d
}

//StartDeciderPolling start the polling, ensure to pass in the call back function to handle the activity
func (d *Decider) StartDeciderPolling(name string, stdout bool, logfolder string, handleDecision func(name string, input string) (result string, err error)) error {

	// initialise logs
	Info, Error = file.InitLogs(stdout, logfolder, name)
	Info.Println("Starting Decider =================>")

	// start workflow
	swfsvc := swf.New(session.New(), &aws.Config{Region: aws.String("us-east-1")})
	params := &swf.PollForDecisionTaskInput{
		Domain: aws.String(swfDomain), //
		TaskList: &swf.TaskList{ //
			Name: aws.String(swfTasklist), //
		},
		Identity:        aws.String(swfIdentity),
		MaximumPageSize: aws.Int64(100),
		ReverseOrder:    aws.Bool(true),
	}

	// loop forever while polling for work
	for {
		resp, err := swfsvc.PollForDecisionTask(params)
		if err != nil {
			amazon.SESSendEmail("support@rapidtrade.biz", helpdesk, swfIdentity+" unable to pole", err.Error())
			Error.Printf("error: unable to poll for decision: %v\n", err)
			panic("Broken, check logs")
		}

		// if we do not receive a task token then 60 second time out occured so try again
		if resp.TaskToken != nil {
			if *resp.TaskToken != "" {
				// Re-initialise logs so we get latest date
				Info, Error = file.InitLogs(stdout, logfolder, name)
				d.svc = swfsvc
				d.tt = *resp.TaskToken
				d.runid = *resp.WorkflowExecution.RunId
				d.workflowid = *resp.WorkflowExecution.WorkflowId
			}
			// make each decision in a goroutine which means that multiple decisions can be made
			go d.makeDecision(resp.Events, resp.WorkflowExecution.RunId, nil)
		} else {
			Info.Printf("debug - no decisions required\n")
		}
	}
}

func (d *Decider) makeDecision(events []*swf.HistoryEvent, ID *string, handleActivity func(d *Decider, lastActivity string) (*NextActivity, error)) {
	Info.Print("###############\n")
	Info.Print(ID)
	Info.Print("\n##############\n")

	var handled bool
	var err error

	// loop backwards through time and make decisions
	for k, event := range events {
		switch *event.EventType {
		case "WorkflowExecutionStarted":
			d.handleWorkflowStart(event)
			handled = true

		case "ActivityTaskCompleted":
			_ = "breakpoint"
			lastActivity := d.getLastScheduledActivity(events)
			nextactivity, err1 := handleActivity(d, lastActivity)
			if err1 != nil {
				d.emailError("ActivityTaskFailed")
				d.failWorkflow(*event.ActivityTaskFailedEventAttributes.Reason, nil)
			}
			if nextactivity.complete {
				d.CompleteWorkflow(nextactivity.input)
				handled = true
			} else {
				d.ScheduleNextActivity(nextactivity.name, nextactivity.version, nextactivity.input, nextactivity.stcTimeout, nextactivity.tasklist, nextactivity.context)
			}

		case "ActivityTaskTimedOut":
			d.handleTimeout()
			handled = true

		case "ActivityTaskFailed":
			Info.Println("Cancelling workflow")
			d.emailError("ActivityTaskFailed")
			d.failWorkflow(*event.ActivityTaskFailedEventAttributes.Reason, nil)
			handled = true

		case "ActivityTaskCanceled":
			d.failWorkflow("Workflow cancelled after activity cancelled", nil)
			handled = true

		case "WorkflowExecutionCancelRequested":
			d.failWorkflow("Workflow cancelled by request", nil)
			handled = true

		case "TimerFired":
			err = d.handleTimerFired(k, events)
			handled = true

		default:
			Info.Printf("Unhandled: %s\n", *event.EventType)
		}
		if handled == true {
			break // decision has been made so stop scanning the events
		}
	}

	if err != nil {
		Info.Printf("Error making decision. workflow failed: %v\n", err)
		// we are not able to process the workflow so fail it
		err2 := d.failWorkflow("", err)
		if err2 != nil {
			Info.Printf("error while failing workflow: %v\n", err2)
		}
	}

	if handled == false {
		Info.Printf("debug dump of received event for taskToken: %s\n", d.tt)
		Info.Println(events)
		Info.Printf("xxxx debug unhandled decision\n")
	}
	Info.Print("#################### completed handling Decision ####################\n")
	// exit goroutine
}

// ============================== generic functions =========================================
// getLastScheduledActivity loops through the workflow events in reverse order to pick up the details of the name of the last scheduled activity
func (d *Decider) getLastScheduledActivity(events []*swf.HistoryEvent) string {
	for _, event := range events {
		if *event.EventType == "ActivityTaskScheduled" {
			return *event.ActivityTaskScheduledEventAttributes.ActivityType.Name
		}
	}
	return ""
}

func (d *Decider) handleTimerFired(k int, es []*swf.HistoryEvent) error {
	return nil
}

func (d *Decider) setTimer(sec, data, id string) error {
	Info.Printf("debug start set timer to wait: %s seconds\n", sec)

	params := &swf.RespondDecisionTaskCompletedInput{
		TaskToken: aws.String(d.tt),
		Decisions: []*swf.Decision{
			{
				DecisionType: aws.String("StartTimer"),
				StartTimerDecisionAttributes: &swf.StartTimerDecisionAttributes{
					StartToFireTimeout: aws.String(sec),
					TimerId:            aws.String(id),
					Control:            aws.String(data),
				},
			},
		},
		ExecutionContext: aws.String("ssec2-amicreate"),
	}
	_, err := d.svc.RespondDecisionTaskCompleted(params)
	return err
}

// handleTimeout will send an email if the first timeout, then set marker so next time we dont email
func (d *Decider) handleTimeout() error {
	to, _ := d.emailError("Activity Timeout")
	params := &swf.RespondDecisionTaskCompletedInput{
		TaskToken: aws.String(d.tt),
		Decisions: []*swf.Decision{
			{
				DecisionType: aws.String("RecordMarker"),
				RecordMarkerDecisionAttributes: &swf.RecordMarkerDecisionAttributes{
					MarkerName: aws.String("HelpdeskNotified"),
					Details:    aws.String(to),
				},
			},
		},
		ExecutionContext: aws.String("Data"),
	}
	_, err := d.svc.RespondDecisionTaskCompleted(params)
	return err // which may be nil
}

func (d *Decider) emailError(reason string) (string, error) {
	runid := strings.Replace(d.runid, "=", "!=", 1)
	msg := "https://console.aws.amazon.com/swf/home?region=us-east-1#execution_events:domain=" + swfDomain + ";workflowId=" + d.workflowid + ";runId=" + runid
	err := amazon.SESSendEmail("support@rapidtrade.biz", helpdesk, "Workflow "+reason+" Occured", msg)
	if err != nil {
		return "", err
	}
	Info.Printf("Error emailed to %v", helpdesk)
	return helpdesk, nil
}

// CompleteWorkflow will complete workflow
func (d *Decider) CompleteWorkflow(result string) error {
	params := &swf.RespondDecisionTaskCompletedInput{
		TaskToken: aws.String(d.tt),
		Decisions: []*swf.Decision{
			{
				DecisionType: aws.String("CompleteWorkflowExecution"),
				CompleteWorkflowExecutionDecisionAttributes: &swf.CompleteWorkflowExecutionDecisionAttributes{
					Result: aws.String(result),
				},
			},
		},
		ExecutionContext: aws.String("Data"),
	}
	_, err := d.svc.RespondDecisionTaskCompleted(params)
	return err // which may be nil
}

// failWorkflow will fail this workflow
func (d *Decider) failWorkflow(details string, err error) error {
	errorD := ""
	if err != nil {
		errorD = fmt.Sprintf("%v", err)
	}
	params := &swf.RespondDecisionTaskCompletedInput{
		TaskToken: aws.String(d.tt),
		Decisions: []*swf.Decision{
			{
				DecisionType: aws.String("FailWorkflowExecution"),
				FailWorkflowExecutionDecisionAttributes: &swf.FailWorkflowExecutionDecisionAttributes{
					Details: aws.String(details),
					Reason:  aws.String(errorD),
				},
			},
		},
		ExecutionContext: aws.String("Data"),
	}
	_, err = d.svc.RespondDecisionTaskCompleted(params)
	return err // which may be nil
}

// ScheduleNextActivity will start the next activity
func (d *Decider) ScheduleNextActivity(name string, version string, input string, stcTimeout string, tasklist string, context string) error {
	id := name + time.Now().Format("200601021504")
	Info.Printf("Scheduling eventType: %s\n", name)
	params := &swf.RespondDecisionTaskCompletedInput{
		TaskToken: aws.String(d.tt),
		Decisions: []*swf.Decision{
			{
				DecisionType: aws.String("ScheduleActivityTask"), //
				ScheduleActivityTaskDecisionAttributes: &swf.ScheduleActivityTaskDecisionAttributes{
					ActivityId: aws.String(id),
					ActivityType: &swf.ActivityType{
						Name:    aws.String(name),
						Version: aws.String(version),
					},
					Input:               aws.String(input),
					StartToCloseTimeout: aws.String(stcTimeout),
					TaskList: &swf.TaskList{
						Name: aws.String(tasklist),
					},
				},
			},
		},
		ExecutionContext: aws.String(context),
	}
	_, err := d.svc.RespondDecisionTaskCompleted(params)
	return err
}

func (d *Decider) getJSON(input string) map[string]interface{} {
	var data interface{}
	json.Unmarshal([]byte(input), &data)
	m := data.(map[string]interface{})
	return m
}

func (d *Decider) handleWorkflowStart(event *swf.HistoryEvent) error {
	wfInput := *event.WorkflowExecutionStartedEventAttributes.Input
	json := d.getJSON(wfInput)
	supplierid, _ := json["SupplierID"].(string)
	err := d.ScheduleNextActivity(d.swfFirstActivity, "1", wfInput, "10000", supplierid, "")
	return err
}

//======================================= handle routines ==================================================
// handleLoadBigQueryComplete gets result JSON and starts loadcompleted using the supplierid as the tasklist
func (d *Decider) handlePostorderComplete(jsonstr string) error {
	var rslt result
	err := json.Unmarshal([]byte(jsonstr), &rslt)
	if err != nil {
		return err
	}
	err = d.ScheduleNextActivity("loadcompleted", "2", rslt.File, "10000", rslt.SupplierID, "")
	return err
}
