package main

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type sqsClient struct {
	client *sqs.Client
	ctx    context.Context
}

func RedriveMessages(ctx context.Context, dlqName string) error {
	sqsClient, err := newSQSClient(ctx)
	if err != nil {
		return err
	}

	dlqURL, err := sqsClient.getQueueUrl(dlqName)
	if err != nil {
		return err
	}

	sourceQueueURL, err := sqsClient.getSourceQueue(dlqURL)
	if err != nil {
		return err
	}

	messages, err := sqsClient.receiveMessage(dlqURL)
	if err != nil {
		return err
	}

	if len(messages) == 0 {
		return nil
	}

	for _, message := range messages {
		err := sqsClient.sendMessage(sourceQueueURL, *message.Body)
		if err != nil {
			log.Println("Erro ao reenviar mensagem", err)
			continue
		}

		err = sqsClient.deleteMessage(dlqURL, *message.ReceiptHandle)
		if err != nil {
			return err
		}
	}

	return nil
}

func PurgeMessages(ctx context.Context, queueName string) error {
	sqsClient, err := newSQSClient(ctx)
	if err != nil {
		return err
	}

	queueUrl, err := sqsClient.getQueueUrl(queueName)
	if err != nil {
		return err
	}

	sqsClient.client.PurgeQueue(ctx, &sqs.PurgeQueueInput{
		QueueUrl: aws.String(queueUrl),
	})

	return nil
}

func newSQSClient(ctx context.Context) (*sqsClient, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration, %v", err)
	}

	return &sqsClient{
		client: sqs.NewFromConfig(cfg),
		ctx:    ctx,
	}, nil
}

func (s *sqsClient) getQueueUrl(queueName string) (string, error) {
	result, err := s.client.GetQueueUrl(s.ctx, &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	})
	if err != nil {
		return "", fmt.Errorf("failed to get queue URL, %v", err)
	}

	return *result.QueueUrl, nil
}

func (s *sqsClient) getSourceQueue(dlrUrl string) (string, error) {
	result, err := s.client.ListDeadLetterSourceQueues(s.ctx, &sqs.ListDeadLetterSourceQueuesInput{
		QueueUrl: &dlrUrl,
	})
	if err != nil {
		return "", fmt.Errorf("failed to get source queue, %v", err)
	}
	return result.QueueUrls[0], nil
}

func (s *sqsClient) receiveMessage(queueUrl string) ([]types.Message, error) {
	resp, err := s.client.ReceiveMessage(
		s.ctx,
		&sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueUrl),
			MaxNumberOfMessages: 10,
			VisibilityTimeout:   30,
			WaitTimeSeconds:     5,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to receive message, %v", err)
	}

	return resp.Messages, nil
}

func (s *sqsClient) sendMessage(queueUrl string, messageBody string) error {
	_, err := s.client.SendMessage(
		s.ctx,
		&sqs.SendMessageInput{
			QueueUrl:    aws.String(queueUrl),
			MessageBody: aws.String(messageBody),
		},
	)

	if err != nil {
		return fmt.Errorf("failed to send message, %v", err)
	}

	return nil
}

func (s *sqsClient) deleteMessage(queueUrl string, receiptHandle string) error {
	_, err := s.client.DeleteMessage(
		s.ctx,
		&sqs.DeleteMessageInput{
			QueueUrl:      aws.String(queueUrl),
			ReceiptHandle: aws.String(receiptHandle),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to delete message, %v", err)
	}

	return nil
}
