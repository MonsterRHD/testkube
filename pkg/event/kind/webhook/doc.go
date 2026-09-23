// Package webhook loads the Webhook resources and sends an HTTP request for each event
// that a webhook matches.
//
// The listener checks an event in this order:
//
//  1. The event type is in the events of the webhook.
//  2. The resource of the event matches the selector of the webhook.
//  3. The webhook is enabled, and the execution does not disable webhooks or run in
//     silent mode for webhooks.
//  4. When the webhook sets status details types, the test workflow execution carries a
//     status details object with a type in the list. An event without the object, for
//     example a start event, never matches a set list. An empty list sets no condition.
//
// A webhook that refers to a WebhookTemplate gets the union of its own events and status
// details types and those of the template. Thus a webhook cannot make the condition of
// its template narrower.
package webhook
