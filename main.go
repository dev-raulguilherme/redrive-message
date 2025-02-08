package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

type Ticket struct {
	DLQName string
}

func main() {
	lambda.Start(Handler)
}

func Handler(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	var ticket Ticket
	if err := json.Unmarshal([]byte(request.Body), &ticket); err != nil {
		log.Println("Error unmarshalling request", err)
		return handleErrorResponse(http.StatusBadRequest, "Invalid request"), nil
	}

	if ticket.DLQName == "" {
		messageError := "DLQName is required"
		log.Println(messageError)
		return handleErrorResponse(http.StatusBadRequest, messageError), nil

	}

	err := RedriveMessages(ctx, ticket.DLQName)
	if err != nil {
		messageError := "Error redriving message"
		log.Println(messageError, err)
		return handleErrorResponse(http.StatusInternalServerError, messageError), nil
	}

	return handleSuccessResponse(http.StatusOK), nil
}

func handleSuccessResponse(status int) events.APIGatewayProxyResponse {
	return events.APIGatewayProxyResponse{
		StatusCode: status,
		Body:       `{"message": "Success"}`,
	}
}

func handleErrorResponse(status int, message string) events.APIGatewayProxyResponse {
	return events.APIGatewayProxyResponse{
		StatusCode: status,
		Body:       `{"message": "` + message + `"}`,
	}
}
