package v1

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	executorv1 "github.com/kubeshop/testkube/api/executor/v1"
	executorsclientv1 "github.com/kubeshop/testkube/pkg/operator/client/executors/v1"
)

func TestTestkubeAPI_WebhookHandlersStatusDetailsTypes(t *testing.T) {
	stored := func() *executorv1.Webhook {
		return &executorv1.Webhook{Spec: executorv1.WebhookSpec{StatusDetailsTypes: []string{"step-failure"}}}
	}

	tests := []struct {
		name       string
		method     string
		body       string
		expect     func(client *executorsclientv1.MockWebhooksInterface)
		wantStatus int
	}{
		{
			name:       "create with an invalid type is a bad request",
			method:     http.MethodPost,
			body:       `{"name":"hook","statusDetailsTypes":["step-failed"]}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "create with valid types stores them",
			method: http.MethodPost,
			body:   `{"name":"hook","statusDetailsTypes":["init-failure","step-failure"]}`,
			expect: func(client *executorsclientv1.MockWebhooksInterface) {
				client.EXPECT().Create(gomock.Any()).DoAndReturn(func(item *executorv1.Webhook) (*executorv1.Webhook, error) {
					assert.Equal(t, []string{"init-failure", "step-failure"}, item.Spec.StatusDetailsTypes)
					return item, nil
				})
			},
			wantStatus: http.StatusCreated,
		},
		{
			name:       "update with an invalid type is a bad request",
			method:     http.MethodPatch,
			body:       `{"name":"hook","statusDetailsTypes":[""]}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "update with an empty list clears the types",
			method: http.MethodPatch,
			body:   `{"name":"hook","statusDetailsTypes":[]}`,
			expect: func(client *executorsclientv1.MockWebhooksInterface) {
				client.EXPECT().Get("hook").Return(stored(), nil)
				client.EXPECT().Update(gomock.Any()).DoAndReturn(func(item *executorv1.Webhook) (*executorv1.Webhook, error) {
					assert.Equal(t, []string{}, item.Spec.StatusDetailsTypes)
					return item, nil
				})
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "update without types keeps the types",
			method: http.MethodPatch,
			body:   `{"name":"hook"}`,
			expect: func(client *executorsclientv1.MockWebhooksInterface) {
				client.EXPECT().Get("hook").Return(stored(), nil)
				client.EXPECT().Update(gomock.Any()).DoAndReturn(func(item *executorv1.Webhook) (*executorv1.Webhook, error) {
					assert.Equal(t, []string{"step-failure"}, item.Spec.StatusDetailsTypes)
					return item, nil
				})
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := executorsclientv1.NewMockWebhooksInterface(gomock.NewController(t))
			if tt.expect != nil {
				tt.expect(client)
			}
			testAPI := &TestkubeAPI{WebhooksClient: client}
			app := fiber.New()
			app.Post("/webhooks", testAPI.CreateWebhookHandler())
			app.Patch("/webhooks", testAPI.UpdateWebhookHandler())

			req := httptest.NewRequestWithContext(context.Background(), tt.method, "/webhooks", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req)

			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, resp.StatusCode)
		})
	}
}
