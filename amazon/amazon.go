package amazon

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
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

// SWFStartWorkflow starts a new workflow
func SWFStartWorkflow(svc *swf.SWF, domainName string, workflowName string, version string, input string, tag string, tasklist string) (*string, error) {
	//svc := swf.New(session.New())
	id := workflowName + time.Now().Format("200601021504")
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
		TagList: []*string{
			aws.String(tag), // Required
		},
		TaskList: &swf.TaskList{
			Name: aws.String(tasklist), // Required
		},
		//TaskPriority:            aws.String("TaskPriority"),
		//TaskStartToCloseTimeout: aws.String("DurationInSecondsOptional"),
	}
	resp, err := svc.StartWorkflowExecution(params)
	return resp.RunId, err
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
