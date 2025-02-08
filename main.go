package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)

var strategies = map[string]RequestHandlerStrategy{
	"purge":   &PurgeMessagesHander{},
	"redrive": &RedriveMessagesHander{},
}

func main() {
	lambda.Start(Handler)
}

func Handler(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	type Payload struct {
		Action string `json:"action"`
	}

	var payload Payload
	if err := json.Unmarshal([]byte(request.Body), &payload); err != nil {
		log.Println("Error unmarshalling request", err)
		return handleErrorResponse(http.StatusBadRequest, "Invalid request"), nil
	}

	stategy, exists := strategies[payload.Action]
	if !exists {
		log.Println("Invalid action")
		return handleErrorResponse(http.StatusBadRequest, "Invalid action"), nil
	}

	if err := stategy.Execute(ctx, []byte(request.Body)); err != nil {
		log.Println("Error executing strategy", err)
		return handleErrorResponse(http.StatusInternalServerError, "Internal server error"), nil
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
