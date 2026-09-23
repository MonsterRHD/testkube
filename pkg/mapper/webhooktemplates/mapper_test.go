package webhooktemplates

import (
	"testing"

	"github.com/stretchr/testify/assert"

	executorv1 "github.com/kubeshop/testkube/api/executor/v1"
	"github.com/kubeshop/testkube/pkg/api/v1/testkube"
)

func TestMapStatusDetailsTypes(t *testing.T) {
	types := []string{"execution-failure", "step-failure"}
	crd := executorv1.WebhookTemplate{Spec: executorv1.WebhookTemplateSpec{StatusDetailsTypes: types}}

	assert.Equal(t, types, MapCRDToAPI(crd).StatusDetailsTypes, "CRD to API")
	assert.Equal(t, types, MapAPIToCRD(testkube.WebhookTemplateCreateRequest{StatusDetailsTypes: types}).Spec.StatusDetailsTypes, "create request to CRD")
	assert.Equal(t, types, *MapSpecToUpdate(&crd).StatusDetailsTypes, "CRD to update request")
}

func TestMapUpdateToSpec(t *testing.T) {
	stored := []string{"execution-failure", "step-failure"}

	tests := []struct {
		name  string
		types *[]string
		want  []string
	}{
		{name: "a list replaces the types", types: &[]string{"init-failure"}, want: []string{"init-failure"}},
		{name: "an empty list clears the types", types: &[]string{}, want: []string{}},
		{name: "no list keeps the types", types: nil, want: stored},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			crd := &executorv1.WebhookTemplate{Spec: executorv1.WebhookTemplateSpec{StatusDetailsTypes: stored}}
			got := MapUpdateToSpec(testkube.WebhookTemplateUpdateRequest{StatusDetailsTypes: tt.types}, crd)
			assert.Equal(t, tt.want, got.Spec.StatusDetailsTypes)
		})
	}
}
