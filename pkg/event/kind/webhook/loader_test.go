package webhook

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	executorsv1 "github.com/kubeshop/testkube/api/executor/v1"
	v1 "github.com/kubeshop/testkube/internal/app/api/metrics"
	cloudwebhook "github.com/kubeshop/testkube/pkg/cloud/data/webhook"
	executorsclientv1 "github.com/kubeshop/testkube/pkg/operator/client/executors/v1"
)

func TestWebhookLoader(t *testing.T) {

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockWebhooksClient := executorsclientv1.NewMockWebhooksInterface(mockCtrl)
	mockWebhookTemplatesClient := executorsclientv1.NewMockWebhookTemplatesInterface(mockCtrl)
	mockWebhooksClient.EXPECT().List(gomock.Any()).Return(&executorsv1.WebhookList{
		Items: []executorsv1.Webhook{
			{Spec: executorsv1.WebhookSpec{Uri: "http://localhost:3333", Events: []executorsv1.EventType{"start-test"}, PayloadObjectField: "text", PayloadTemplate: "{{ .Id }}", Headers: map[string]string{"Content-Type": "application/xml"}}},
		},
	}, nil).AnyTimes()
	mockWebhookRepository := cloudwebhook.NewMockWebhookRepository(mockCtrl)

	webhooksLoader := NewWebhookLoader(
		mockWebhooksClient,
		WithWebhookTemplateClient(mockWebhookTemplatesClient),
		WithMetrics(v1.NewMetrics()),
		WithWebhookResultsRepository(mockWebhookRepository),
	)
	listeners, err := webhooksLoader.Load()

	assert.Equal(t, 1, len(listeners))
	assert.NoError(t, err)
}

func TestWebhookTemplateLoader(t *testing.T) {

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockWebhooksClient := executorsclientv1.NewMockWebhooksInterface(mockCtrl)
	mockWebhookTemplatesClient := executorsclientv1.NewMockWebhookTemplatesInterface(mockCtrl)
	mockWebhooksClient.EXPECT().List(gomock.Any()).Return(&executorsv1.WebhookList{
		Items: []executorsv1.Webhook{
			{Spec: executorsv1.WebhookSpec{WebhookTemplateRef: &executorsv1.WebhookTemplateRef{Name: "name"}}},
		},
	}, nil).AnyTimes()
	mockWebhookTemplatesClient.EXPECT().Get("name").Return(&executorsv1.WebhookTemplate{
		Spec: executorsv1.WebhookTemplateSpec{
			Uri: "http://localhost:3333", Events: []executorsv1.EventType{"start-test"}, PayloadObjectField: "text", PayloadTemplate: "{{ .Id }}", Headers: map[string]string{"Content-Type": "application/xml"},
		},
	}, nil).AnyTimes()

	mockWebhookRepository := cloudwebhook.NewMockWebhookRepository(mockCtrl)

	webhooksLoader := NewWebhookLoader(
		mockWebhooksClient,
		WithWebhookTemplateClient(mockWebhookTemplatesClient),
		WithMetrics(v1.NewMetrics()),
		WithWebhookResultsRepository(mockWebhookRepository),
	)
	listeners, err := webhooksLoader.Load()

	assert.Equal(t, 1, len(listeners))
	assert.NoError(t, err)
}

func TestWebhooksLoader_Load(t *testing.T) {
	tests := []struct {
		name                   string
		webhookTypes           []string
		templateTypes          []string
		wantStatusDetailsTypes []string
	}{
		{
			name: "no types on the webhook and the template",
		},
		{
			name:                   "types from the webhook only",
			webhookTypes:           []string{"step-failure"},
			wantStatusDetailsTypes: []string{"step-failure"},
		},
		{
			name:                   "types from the template only",
			templateTypes:          []string{"init-failure"},
			wantStatusDetailsTypes: []string{"init-failure"},
		},
		{
			name:                   "union of the webhook and the template types",
			webhookTypes:           []string{"step-failure", "execution-failure"},
			templateTypes:          []string{"init-failure", "execution-failure"},
			wantStatusDetailsTypes: []string{"execution-failure", "init-failure", "step-failure"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			mockWebhooksClient := executorsclientv1.NewMockWebhooksInterface(mockCtrl)
			mockWebhookTemplatesClient := executorsclientv1.NewMockWebhookTemplatesInterface(mockCtrl)
			mockWebhooksClient.EXPECT().List(gomock.Any()).Return(&executorsv1.WebhookList{
				Items: []executorsv1.Webhook{{Spec: executorsv1.WebhookSpec{
					WebhookTemplateRef: &executorsv1.WebhookTemplateRef{Name: "template"},
					StatusDetailsTypes: tt.webhookTypes,
				}}},
			}, nil)
			mockWebhookTemplatesClient.EXPECT().Get("template").Return(&executorsv1.WebhookTemplate{
				Spec: executorsv1.WebhookTemplateSpec{
					Uri:                "http://localhost:3333",
					Events:             []executorsv1.EventType{"end-testworkflow-failed"},
					StatusDetailsTypes: tt.templateTypes,
				},
			}, nil)

			listeners, err := NewWebhookLoader(mockWebhooksClient, WithWebhookTemplateClient(mockWebhookTemplatesClient)).Load()

			require.NoError(t, err)
			require.Len(t, listeners, 1)
			listener := listeners[0].(*WebhookListener)
			assert.Equal(t, tt.wantStatusDetailsTypes, listener.statusDetailsTypes)
			assert.Equal(t, fmt.Sprintf("%v", tt.wantStatusDetailsTypes), listener.Metadata()["statusDetailsTypes"])
		})
	}
}
