package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
)

type RequestHandlerStrategy interface {
	Execute(ctx context.Context, data json.RawMessage) error
}

// Purge Messages from queue
type PurgeMessagesHander struct{}

func (p *PurgeMessagesHander) Execute(ctx context.Context, data json.RawMessage) error {
	var payload struct {
		QueueName string `json:"queueName"`
	}

	if err := json.Unmarshal(data, &payload); err != nil {
		log.Println("Error unmarshalling request", err)
		return fmt.Errorf("invalid request: %w", err)
	}

	if payload.QueueName == "" {
		messageError := "DLQName is required"
		log.Println(messageError)
		return fmt.Errorf("invalid request: %s", messageError)
	}

	if err := PurgeMessages(ctx, payload.QueueName); err != nil {
		return err
	}

	log.Println("Messages purged successfully")
	return nil
}

// Redrive Messages from DLQ to source queue
type RedriveMessagesHander struct{}

func (r *RedriveMessagesHander) Execute(ctx context.Context, data json.RawMessage) error {
	var payload struct {
		QueueName string `json:"queueName"`
	}

	if err := json.Unmarshal(data, &payload); err != nil {
		log.Println("Error unmarshalling request", err)
		return fmt.Errorf("invalid request: %w", err)
	}

	if payload.QueueName == "" {
		messageError := "DLQName is required"
		log.Println(messageError)
		return fmt.Errorf("invalid request: %s", messageError)
	}

	if err := RedriveMessages(ctx, payload.QueueName); err != nil {
		return err
	}

	log.Println("Messages redrived successfully")
	return nil
}
