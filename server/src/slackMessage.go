package main

// Package blatantly taken from  Sheryas, and modified for ferry  thanks bud.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	log "github.com/sirupsen/logrus"
)

func SlackMessage(ctx context.Context, message string) error {
	if e := ctx.Err(); e != nil {
		log.Errorf("Error sending slack message: %s", e)
		return e
	}
	if message == "" {
		log.Warn("Slack message is empty.  Will not attempt to send it")
		return nil
	}
	message = fmt.Sprintf("Server: %s - %s", serverRole, message)
	msg := []byte(fmt.Sprintf(`{"text": "%s"}`, strings.Replace(message, "\"", "\\\"", -1)))
	req, err := http.NewRequest("POST", FerryAlertsURL, bytes.NewBuffer(msg))
	if err != nil {
		log.Errorf("Error sending slack message: %s", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(ctx)
	client := http.DefaultClient
	resp, err := client.Do(req)
	if err != nil {
		log.Errorf("Error sending slack message: %s", err)
		return err
	}
	// This should be redundant, but just in case the timeout before didn't trigger.
	if e := ctx.Err(); e != nil {
		log.Errorf("Error sending slack message: %s", e)
		return e
	}
	defer resp.Body.Close()
	// Parse the response to make sure we're good
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		err := errors.New("could not send slack message")
		log.WithFields(log.Fields{
			"response status":  resp.Status,
			"response headers": resp.Header,
			"response body":    string(body),
		}).Error(err)
		return err
	}

	return nil
}
