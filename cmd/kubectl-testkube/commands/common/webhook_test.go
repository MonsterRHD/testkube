package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetWebhookStatusDetailsTypes(t *testing.T) {
	tests := []struct {
		name    string
		values  []string
		want    []string
		wantErr string
	}{
		{name: "no values give an empty list", values: nil, want: []string{}},
		{name: "an empty value gives an empty list", values: []string{""}, want: []string{}},
		{name: "empty values are dropped", values: []string{"", "step-failure", ""}, want: []string{"step-failure"}},
		{name: "valid values are kept", values: []string{"init-failure", "user-cancel"}, want: []string{"init-failure", "user-cancel"}},
		{name: "an invalid value is an error", values: []string{"step-failed"}, wantErr: `invalid status details type "step-failed"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetWebhookStatusDetailsTypes(tt.values)
			if tt.wantErr != "" {
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
