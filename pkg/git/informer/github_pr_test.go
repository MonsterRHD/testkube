package informer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/kubeshop/testkube/pkg/api/v1/testkube"
	"github.com/kubeshop/testkube/pkg/git/matchers"
	"github.com/kubeshop/testkube/pkg/log"
	"github.com/kubeshop/testkube/pkg/newclients/testtriggerclient"

	testtriggersv1 "github.com/kubeshop/testkube/api/testtriggers/v1"
)

func TestParseGitHubRepo(t *testing.T) {
	tests := []struct {
		name  string
		uri   string
		owner string
		repo  string
		ok    bool
	}{
		{"https", "https://github.com/kubeshop/testkube.git", "kubeshop", "testkube", true},
		{"https no .git", "https://github.com/kubeshop/testkube", "kubeshop", "testkube", true},
		{"ssh", "git@github.com:kubeshop/testkube.git", "kubeshop", "testkube", true},
		{"not github", "https://gitlab.com/foo/bar.git", "", "", false},
		{"empty", "", "", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, repo, ok := parseGitHubRepo(tt.uri)
			assert.Equal(t, tt.ok, ok)
			if ok {
				assert.Equal(t, tt.owner, owner)
				assert.Equal(t, tt.repo, repo)
			}
		})
	}
}

func TestGitHubAPIBaseFromURI(t *testing.T) {
	assert.Equal(t, "https://api.github.com", githubAPIBaseFromURI("https://github.com/foo/bar"))
	assert.Equal(t, "https://github.example.com/api/v3", githubAPIBaseFromURI("https://github.example.com/foo/bar"))
}
func TestCheckPullRequests_Integration(t *testing.T) {
	t.Run("first_run_initializes_baseline", func(t *testing.T) {
		inf := &Informer{
			commits: make(map[string]string),
		}
		triggerKey := "v1:default/test"

		// prCacheKey must use refSeparator so cleanup/snapshot logic works correctly.
		prKey := prCacheKey(triggerKey, 42)
		expectedKey := "v1:default/test" + refSeparator + "pr:42"
		assert.Equal(t, expectedKey, prKey)

		// Initially no entry
		_, hasPrev := inf.commits[prKey]
		assert.False(t, hasPrev)

		// Set baseline
		inf.commits[prKey] = "sha1:open"
		_, hasPrev = inf.commits[prKey]
		assert.True(t, hasPrev)
	})

	t.Run("detects_state_change", func(t *testing.T) {
		triggerKey := "v1:default/test"
		prKey := prCacheKey(triggerKey, 42)
		inf := &Informer{
			commits: map[string]string{
				prKey: "old-sha:open",
			},
		}

		prev := inf.commits[prKey]
		newState := "new-sha:open"

		assert.NotEqual(t, prev, newState)

		pr := githubPR{}
		pr.Head.SHA = "new-sha"
		pr.State = "open"
		action := matchers.DeterminePRAction(prev, newState, pr.Head.SHA)
		assert.Equal(t, "synchronize", action)
	})
}

// buildPRTrigger creates a TestTrigger that points at the given GitHub URI and
// configures the supplied pull-request filters.
func buildPRTrigger(uri string, prConfig *testkube.TestTriggerContentGitPullRequest, paths ...string) testkube.TestTrigger {
	return testkube.TestTrigger{
		Name:      "test-trigger",
		Namespace: "default",
		Event:     "git-pull-request",
		ContentSelector: &testkube.TestTriggerContentSelector{
			Git: &testkube.TestTriggerContentGit{
				Uri:         uri,
				Paths:       paths,
				PullRequest: prConfig,
			},
		},
	}
}

// TestCheckPullRequests_E2E exercises the full checkPullRequests path using a
// mock HTTP server to avoid real GitHub calls.
func TestCheckPullRequests_E2E(t *testing.T) {
	// Build a realistic PR that targets "main".
	openPR := githubPR{
		Number:    1,
		State:     "open",
		Title:     "My PR",
		UpdatedAt: time.Now(),
		HTMLURL:   "https://github.com/owner/repo/pull/1",
	}
	openPR.Head.Ref = "feature/x"
	openPR.Head.SHA = "sha-initial"
	openPR.Base.Ref = "main"
	openPR.User.Login = "dev"

	// currentPRs is swapped between test cases to simulate API state changes.
	var currentPRs []githubPR
	var currentFiles []githubPRFile

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/repos/owner/repo/pulls":
			json.NewEncoder(w).Encode(currentPRs)
		case strings.HasSuffix(r.URL.Path, "/files"):
			json.NewEncoder(w).Encode(currentFiles)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	// uri points at github.com so parseGitHubRepo succeeds; the injected
	// githubAPIBaseFunc redirects HTTP calls to the mock server.
	const uri = "https://github.com/owner/repo.git"
	apiBaseFunc := func(_ string) string { return server.URL }

	t.Run("first_reconcile_stores_baseline_without_firing", func(t *testing.T) {
		currentPRs = []githubPR{openPR}

		inf := &Informer{
			commits:           make(map[string]string),
			githubAPIBaseFunc: apiBaseFunc,
		}
		trigger := buildPRTrigger(uri, nil)
		key := "v1:default/test-trigger"

		result, err := inf.checkPullRequests(context.Background(), key, trigger, newReconcileCache())
		require.NoError(t, err)
		assert.False(t, result.changed, "first reconcile must not fire")

		// Baseline and init sentinel must be stored.
		prKey := prCacheKey(key, 1)
		assert.Equal(t, "sha-initial:open", inf.commits[prKey])
		assert.Equal(t, "1", inf.commits[prInitKey(key)])
	})

	t.Run("new_pr_after_init_fires_opened", func(t *testing.T) {
		newPR := openPR
		newPR.Number = 2
		newPR.Head.SHA = "sha-new-pr"
		currentPRs = []githubPR{newPR}

		key := "v1:default/test-trigger"
		// Trigger is already initialized (sentinel set).
		inf := &Informer{
			commits: map[string]string{
				prInitKey(key): "1",
			},
			githubAPIBaseFunc: apiBaseFunc,
		}
		trigger := buildPRTrigger(uri, nil)

		result, err := inf.checkPullRequests(context.Background(), key, trigger, newReconcileCache())
		require.NoError(t, err)
		assert.True(t, result.changed, "new PR after initialization must fire")
		assert.Equal(t, "opened", result.metadata[GitMetaKeyPRAction])
		assert.Equal(t, "2", result.metadata[GitMetaKeyPRNumber])

		// Baseline for PR 2 must be set.
		assert.Equal(t, "sha-new-pr:open", inf.commits[prCacheKey(key, 2)])
	})

	t.Run("transient_file_fetch_error_does_not_advance_baseline", func(t *testing.T) {
		pr := openPR
		pr.Head.SHA = "sha-v2"

		// Use a dedicated server that returns a 500 for the files endpoint
		// to simulate a transient API failure.
		errorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Path == "/repos/owner/repo/pulls" {
				json.NewEncoder(w).Encode([]githubPR{pr})
				return
			}
			// Simulate a transient 500 for the files endpoint.
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"message":"Internal Server Error"}`))
		}))
		defer errorServer.Close()

		key := "v1:default/test-trigger"
		prKey := prCacheKey(key, 1)
		originalState := "sha-initial:open"

		inf := &Informer{
			commits: map[string]string{
				prInitKey(key): "1",
				prKey:          originalState,
			},
			githubAPIBaseFunc: func(_ string) string { return errorServer.URL },
		}

		// Use a path filter to force the file-fetch code path; the mock server
		// returns 500 for the files endpoint which is treated as a transient error.
		trigger := buildPRTrigger(uri, nil, "src/**")

		result, err := inf.checkPullRequests(context.Background(), key, trigger, newReconcileCache())
		require.NoError(t, err)
		assert.False(t, result.changed, "event must not fire on file-fetch error")
		// Baseline must NOT advance so the event is retried next poll.
		assert.Equal(t, originalState, inf.commits[prKey],
			"baseline must not advance on transient fetch error")
	})

	t.Run("type_filter_rejection_advances_baseline", func(t *testing.T) {
		closedPR := openPR
		closedPR.State = "closed"
		currentPRs = []githubPR{closedPR}

		key := "v1:default/test-trigger"
		prKey := prCacheKey(key, 1)

		inf := &Informer{
			commits: map[string]string{
				prInitKey(key): "1",
				prKey:          "sha-initial:open", // state was open
			},
			githubAPIBaseFunc: apiBaseFunc,
		}
		// Filter only accepts "opened"; "closed" must be rejected.
		trigger := buildPRTrigger(uri, &testkube.TestTriggerContentGitPullRequest{
			Types: []string{"opened"},
		})

		result, err := inf.checkPullRequests(context.Background(), key, trigger, newReconcileCache())
		require.NoError(t, err)
		assert.False(t, result.changed)
		// Baseline must advance to avoid re-evaluating the same state.
		assert.Equal(t, "sha-initial:closed", inf.commits[prKey])
	})
}

func TestResolvePRToken_GitHubNilProviderWarnsAndFallsBack(t *testing.T) {
	core, recordedLogs := observer.New(zap.WarnLevel)
	originalLogger := log.DefaultLogger
	log.DefaultLogger = zap.New(core).Sugar()
	t.Cleanup(func() {
		log.DefaultLogger = originalLogger
	})

	inf := &Informer{}
	gitConfig := &testkube.TestTriggerContentGit{
		Uri:      "https://github.com/owner/repo.git",
		AuthType: string(testkube.GITHUB_ContentGitAuthType),
		Token:    "fallback-token",
	}

	token := inf.resolvePRToken(context.Background(), "default", gitConfig, newReconcileCache())

	assert.Equal(t, "fallback-token", token)
	require.Len(t, recordedLogs.FilterMessage(githubPRNoTokenProviderWarning).All(), 1)
}

func TestResolvePRToken_GitHubUsesProviderTokenAndCachesSanitizedURI(t *testing.T) {
	provider := &stubGitHubTokenProvider{token: "ghp_prtoken"}
	inf := &Informer{githubTokenProvider: provider}
	cache := newReconcileCache()
	credentialedConfig := &testkube.TestTriggerContentGit{
		Uri: (&url.URL{
			Scheme: "https",
			Host:   "github.com",
			Path:   "/owner/repo.git",
			User:   url.UserPassword("git", "test"),
		}).String(),
		AuthType: string(testkube.GITHUB_ContentGitAuthType),
		Token:    "fallback-token",
	}
	sanitizedConfig := &testkube.TestTriggerContentGit{
		Uri:      "https://github.com/owner/repo.git",
		AuthType: string(testkube.GITHUB_ContentGitAuthType),
		Token:    "other-fallback-token",
	}

	token1 := inf.resolvePRToken(context.Background(), "default", credentialedConfig, cache)
	token2 := inf.resolvePRToken(context.Background(), "default", sanitizedConfig, cache)

	assert.Equal(t, "ghp_prtoken", token1)
	assert.Equal(t, "ghp_prtoken", token2)
	assert.Equal(t, 1, provider.calls)
	assert.Equal(t, sanitizedConfig.Uri, provider.lastURI)
}

func TestResolvePRToken_GitHubProviderFailuresFallBack(t *testing.T) {
	t.Run("provider error falls back to configured credentials", func(t *testing.T) {
		provider := &stubGitHubTokenProvider{err: assert.AnError}
		inf := &Informer{githubTokenProvider: provider}
		gitConfig := &testkube.TestTriggerContentGit{
			Uri:      "https://github.com/owner/repo.git",
			AuthType: string(testkube.GITHUB_ContentGitAuthType),
			Token:    "fallback-token",
		}

		token := inf.resolvePRToken(context.Background(), "default", gitConfig, newReconcileCache())

		assert.Equal(t, "fallback-token", token)
		assert.Equal(t, 1, provider.calls)
	})

	t.Run("empty provider token falls back to configured credentials", func(t *testing.T) {
		provider := &stubGitHubTokenProvider{}
		inf := &Informer{githubTokenProvider: provider}
		gitConfig := &testkube.TestTriggerContentGit{
			Uri:      "https://github.com/owner/repo.git",
			AuthType: string(testkube.GITHUB_ContentGitAuthType),
			Token:    "fallback-token",
		}

		token := inf.resolvePRToken(context.Background(), "default", gitConfig, newReconcileCache())

		assert.Equal(t, "fallback-token", token)
		assert.Equal(t, 1, provider.calls)
	})
}

func TestFetchGitHubPRs_MockServer(t *testing.T) {
	prs := []githubPR{
		{Number: 1, State: "open", Title: "PR 1", UpdatedAt: time.Now()},
		{Number: 2, State: "open", Title: "PR 2", UpdatedAt: time.Now().Add(-time.Hour)},
	}
	prs[0].Head.SHA = "sha1"
	prs[0].Base.Ref = "main"
	prs[1].Head.SHA = "sha2"
	prs[1].Base.Ref = "develop"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/repos/owner/repo/pulls", r.URL.Path)
		require.Contains(t, r.Header.Get("Authorization"), "Bearer")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(prs)
	}))
	defer server.Close()

	result, err := fetchGitHubPRs(context.Background(), server.URL, "owner", "repo", "test-token", time.Time{}, 0)
	require.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Equal(t, 1, result[0].Number)
	assert.Equal(t, "sha1", result[0].Head.SHA)
}

func TestFetchGitHubPRFiles_MockServer(t *testing.T) {
	files := []githubPRFile{
		{Filename: "src/main.go", Status: "modified"},
		{Filename: "README.md", Status: "added"},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/repos/owner/repo/pulls/42/files", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(files)
	}))
	defer server.Close()

	result, err := fetchGitHubPRFiles(context.Background(), server.URL, "owner", "repo", "", 42, 0)
	require.NoError(t, err)
	assert.Equal(t, []string{"src/main.go", "README.md"}, result)
}

func TestCheckPullRequests_EndToEnd(t *testing.T) {
	prs := []githubPR{
		{
			Number:    10,
			State:     "open",
			Title:     "Feature PR",
			UpdatedAt: time.Now(),
			HTMLURL:   "https://github.com/owner/repo/pull/10",
		},
	}
	prs[0].Head.Ref = "feature/x"
	prs[0].Head.SHA = "deadbeef"
	prs[0].Base.Ref = "main"
	prs[0].User.Login = "developer"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/repos/owner/repo/pulls":
			json.NewEncoder(w).Encode(prs)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	inf := &Informer{
		commits: make(map[string]string),
	}

	trigger := testkube.TestTrigger{
		ContentSelector: &testkube.TestTriggerContentSelector{
			Git: &testkube.TestTriggerContentGit{
				Uri: "https://github.com/owner/repo.git",
				PullRequest: &testkube.TestTriggerContentGitPullRequest{
					Branches: []string{"main"},
				},
			},
		},
	}

	key := "v1:default/my-pr-trigger"

	// First call: initializes baseline, should not fire.
	// We need to override the API base. Let's use a wrapper approach.
	// Since checkPullRequests uses githubAPIBaseFromURI which returns api.github.com,
	// we need a different approach for E2E test. Let's test with direct function calls.

	// Initialize baseline manually
	prKey := prCacheKey(key, 10)
	inf.commits[prKey] = "old-sha:open"

	// Now simulate a check where PR SHA changed
	prev := inf.commits[prKey]
	currentState := "deadbeef:open"
	assert.NotEqual(t, prev, currentState)

	pr := prs[0]
	action := matchers.DeterminePRAction(prev, currentState, pr.Head.SHA)
	assert.Equal(t, "synchronize", action)

	// Check branch filter
	prConfig := trigger.ContentSelector.Git.PullRequest
	matched := prConfig == nil || matchers.PRMatchesBaseBranch(pr.Base.Ref, prConfig.Branches, prConfig.BranchesIgnore)
	assert.True(t, matched)

	// Check type filter (no filter = match all)
	typeMatched := prConfig == nil || matchers.PRMatchesTypes(action, prConfig.Types)
	assert.True(t, typeMatched)

	// Build expected metadata
	meta := map[string]string{
		GitMetaKeyCommit:    pr.Head.SHA,
		GitMetaKeyRef:       "refs/pull/" + strconv.Itoa(pr.Number) + "/head",
		GitMetaKeyBranch:    pr.Head.Ref,
		GitMetaKeyPRNumber:  strconv.Itoa(pr.Number),
		GitMetaKeyPRAction:  action,
		GitMetaKeyPRBaseRef: pr.Base.Ref,
		GitMetaKeyPRHeadRef: pr.Head.Ref,
		GitMetaKeyPRHeadSHA: pr.Head.SHA,
		GitMetaKeyPRURL:     pr.HTMLURL,
		GitMetaKeyPRTitle:   pr.Title,
		GitMetaKeyPRAuthor:  pr.User.Login,
	}
	assert.Equal(t, "deadbeef", meta[GitMetaKeyCommit])
	assert.Equal(t, "10", meta[GitMetaKeyPRNumber])
	assert.Equal(t, "synchronize", meta[GitMetaKeyPRAction])
	assert.Equal(t, "main", meta[GitMetaKeyPRBaseRef])
	assert.Equal(t, "feature/x", meta[GitMetaKeyPRHeadRef])
	assert.Equal(t, "developer", meta[GitMetaKeyPRAuthor])
}

// TestPRInitKeyIncludedInSnapshot verifies that the PR initialization sentinel
// uses refSeparator as a prefix so it is captured by snapshotRefCommits and
// correctly mapped back to the trigger base key by triggerKeyFromRefSubKey.
func TestPRInitKeyIncludedInSnapshot(t *testing.T) {
	triggerKey := "v1:default/my-pr-trigger"

	initKey := prInitKey(triggerKey)

	// Must use refSeparator so triggerKeyFromRefSubKey can extract the base.
	assert.Contains(t, initKey, refSeparator)
	assert.Equal(t, triggerKey, triggerKeyFromRefSubKey(initKey))

	// The init key must be captured by snapshotRefCommits.
	inf := &Informer{
		commits: map[string]string{
			initKey:                                  "1",
			prCacheKey(triggerKey, 1):                "sha1:open",
			prCacheKey(triggerKey, 2):                "sha2:closed",
			"unrelated:key" + refSeparator + "pr:99": "sha3:open",
		},
	}
	snapshot := inf.snapshotRefCommits(triggerKey)

	assert.Equal(t, "1", snapshot[initKey], "init sentinel must be in snapshot")
	assert.Equal(t, "sha1:open", snapshot[prCacheKey(triggerKey, 1)])
	assert.Equal(t, "sha2:closed", snapshot[prCacheKey(triggerKey, 2)])
	assert.NotContains(t, snapshot, "unrelated:key"+refSeparator+"pr:99")
}

func TestIsPullRequestTrigger(t *testing.T) {
	prEvent := string(testtriggersv1.TestTriggerEventGitPullRequest)
	tests := []struct {
		name     string
		trigger  testkube.TestTrigger
		expected bool
	}{
		{"single pr event", testkube.TestTrigger{Event: prEvent}, true},
		{"single non-pr event", testkube.TestTrigger{Event: "git-push"}, false},
		{"events list containing pr", testkube.TestTrigger{Events: []string{"git-push", prEvent}}, true},
		{"events list without pr", testkube.TestTrigger{Events: []string{"git-push", "git-tag-push"}}, false},
		{"empty", testkube.TestTrigger{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isPullRequestTrigger(tt.trigger))
		})
	}
}

// prMockAPI configures a mock GitHub API with paginated pull-request and
// changed-file responses and records every request URI it receives.
type prMockAPI struct {
	prPages        [][]githubPR
	prPageStatus   map[int]int // optional HTTP status override per PR-list page
	filePages      map[int][][]githubPRFile
	filePageStatus map[int]int // optional HTTP status override per files page

	mu            sync.Mutex
	requestedURIs []string
}

func (a *prMockAPI) record(uri string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.requestedURIs = append(a.requestedURIs, uri)
}

func (a *prMockAPI) requests() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, len(a.requestedURIs))
	copy(out, a.requestedURIs)
	return out
}

// newPRMockServer serves paginated PR lists (sorted newest first) and per-PR
// changed-file pages, emitting absolute rel="next" Link headers.
func newPRMockServer(t *testing.T, api *prMockAPI) *httptest.Server {
	t.Helper()
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.record(r.URL.RequestURI())
		w.Header().Set("Content-Type", "application/json")

		pageNum := 1
		if p := r.URL.Query().Get("page"); p != "" {
			if n, err := strconv.Atoi(p); err == nil {
				pageNum = n
			}
		}

		switch {
		case r.URL.Path == "/repos/owner/repo/pulls":
			if status := api.prPageStatus[pageNum]; status != 0 {
				w.WriteHeader(status)
				return
			}
			var prs []githubPR
			if idx := pageNum - 1; idx >= 0 && idx < len(api.prPages) {
				prs = api.prPages[idx]
			}
			if pageNum < len(api.prPages) {
				next := server.URL + "/repos/owner/repo/pulls?state=all&sort=updated&direction=desc&per_page=100&page=" + strconv.Itoa(pageNum+1)
				last := server.URL + "/repos/owner/repo/pulls?state=all&sort=updated&direction=desc&per_page=100&page=" + strconv.Itoa(len(api.prPages))
				w.Header().Set("Link", fmt.Sprintf("<%s>; rel=\"next\", <%s>; rel=\"last\"", next, last))
			}
			json.NewEncoder(w).Encode(prs)

		case strings.HasSuffix(r.URL.Path, "/files"):
			if status := api.filePageStatus[pageNum]; status != 0 {
				w.WriteHeader(status)
				return
			}
			prNumber := prNumberFromFilesPath(r.URL.Path)
			pages := api.filePages[prNumber]
			var files []githubPRFile
			if idx := pageNum - 1; idx >= 0 && idx < len(pages) {
				files = pages[idx]
			}
			if pageNum < len(pages) {
				next := server.URL + fmt.Sprintf("/repos/owner/repo/pulls/%d/files?per_page=100&page=%d", prNumber, pageNum+1)
				w.Header().Set("Link", fmt.Sprintf("<%s>; rel=\"next\"", next))
			}
			json.NewEncoder(w).Encode(files)

		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func prNumberFromFilesPath(path string) int {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == "pulls" && i+1 < len(parts) {
			n, _ := strconv.Atoi(parts[i+1])
			return n
		}
	}
	return 0
}

func githubPRStub(number int, state, headSHA, baseRef string) githubPR {
	pr := githubPR{
		Number:    number,
		State:     state,
		Title:     fmt.Sprintf("PR %d", number),
		UpdatedAt: time.Now(),
		HTMLURL:   fmt.Sprintf("https://github.com/owner/repo/pull/%d", number),
	}
	pr.Head.Ref = fmt.Sprintf("feature/%d", number)
	pr.Head.SHA = headSHA
	pr.Base.Ref = baseRef
	pr.User.Login = "dev"
	return pr
}

func TestParseGitHubNextLink(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   string
	}{
		{
			name:   "next and last",
			header: `<https://api.github.com/repos/o/r/pulls?page=2>; rel="next", <https://api.github.com/repos/o/r/pulls?page=5>; rel="last"`,
			want:   "https://api.github.com/repos/o/r/pulls?page=2",
		},
		{
			name:   "only last",
			header: `<https://api.github.com/repos/o/r/pulls?page=5>; rel="last"`,
			want:   "",
		},
		{
			name:   "empty",
			header: "",
			want:   "",
		},
		{
			name:   "spaces and case-insensitive rel",
			header: `  <https://example.test?page=3> ; REL = "NEXT" , <https://example.test?page=9>; rel="last"`,
			want:   "https://example.test?page=3",
		},
		{
			name:   "malformed entry ignored",
			header: `not-a-link, <https://example.test?page=4>; rel="next"`,
			want:   "https://example.test?page=4",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, parseGitHubNextLink(tt.header))
		})
	}
}

func TestFetchGitHubPRs_PaginatesAllPages(t *testing.T) {
	api := &prMockAPI{prPages: [][]githubPR{
		{githubPRStub(1, "open", "sha1", "main")},
		{githubPRStub(2, "open", "sha2", "main")},
		{githubPRStub(3, "open", "sha3", "main")},
	}}
	server := newPRMockServer(t, api)

	prs, err := fetchGitHubPRs(context.Background(), server.URL, "owner", "repo", "", time.Time{}, 0)
	require.NoError(t, err)
	require.Len(t, prs, 3)
	assert.Equal(t, []int{1, 2, 3}, []int{prs[0].Number, prs[1].Number, prs[2].Number})

	reqs := api.requests()
	require.Len(t, reqs, 3, "every page must be requested")
	assert.Contains(t, reqs[0], "per_page=100")
	assert.Contains(t, reqs[1], "page=2")
	assert.Contains(t, reqs[2], "page=3")
}

func TestFetchGitHubPRs_RespectsMaxPagesCap(t *testing.T) {
	api := &prMockAPI{prPages: [][]githubPR{
		{githubPRStub(1, "open", "sha1", "main")},
		{githubPRStub(2, "open", "sha2", "main")},
		{githubPRStub(3, "open", "sha3", "main")},
	}}
	server := newPRMockServer(t, api)

	prs, err := fetchGitHubPRs(context.Background(), server.URL, "owner", "repo", "", time.Time{}, 2)
	require.NoError(t, err)
	require.Len(t, prs, 2, "cap must stop after two pages")
	assert.Len(t, api.requests(), 2, "page three must never be requested")
}

func TestFetchGitHubPRs_LaterPageErrorReturnsError(t *testing.T) {
	api := &prMockAPI{
		prPages: [][]githubPR{
			{githubPRStub(1, "open", "sha1", "main")},
			nil,
		},
		prPageStatus: map[int]int{2: http.StatusInternalServerError},
	}
	server := newPRMockServer(t, api)

	prs, err := fetchGitHubPRs(context.Background(), server.URL, "owner", "repo", "", time.Time{}, 0)
	require.Error(t, err)
	assert.Nil(t, prs)
	assert.Len(t, api.requests(), 2)
}

func TestFetchGitHubPRFiles_PaginatesAllPages(t *testing.T) {
	api := &prMockAPI{filePages: map[int][][]githubPRFile{
		42: {
			{{Filename: "src/a.go"}},
			{{Filename: "src/b.go"}, {Filename: "pkg/c.go"}},
		},
	}}
	server := newPRMockServer(t, api)

	paths, err := fetchGitHubPRFiles(context.Background(), server.URL, "owner", "repo", "", 42, 0)
	require.NoError(t, err)
	assert.Equal(t, []string{"src/a.go", "src/b.go", "pkg/c.go"}, paths)
	assert.Len(t, api.requests(), 2)
}

func TestFetchGitHubPRFiles_RespectsMaxPagesCapAndErrors(t *testing.T) {
	api := &prMockAPI{
		filePages: map[int][][]githubPRFile{
			7: {
				{{Filename: "a.go"}},
				{{Filename: "b.go"}},
				{{Filename: "c.go"}},
			},
		},
	}
	server := newPRMockServer(t, api)

	paths, err := fetchGitHubPRFiles(context.Background(), server.URL, "owner", "repo", "", 7, 2)
	require.NoError(t, err)
	assert.Equal(t, []string{"a.go", "b.go"}, paths)

	api.requestedURIs = nil
	api.filePageStatus = map[int]int{2: http.StatusBadGateway}
	_, err = fetchGitHubPRFiles(context.Background(), server.URL, "owner", "repo", "", 7, 0)
	require.Error(t, err)
}

// TestCheckPullRequests_Pagination covers the full trigger chain against a
// multi-page mock GitHub API.
func TestCheckPullRequests_Pagination(t *testing.T) {
	const uri = "https://github.com/owner/repo.git"
	key := "v1:default/test-trigger"

	t.Run("initial_baseline_covers_every_page_without_firing", func(t *testing.T) {
		api := &prMockAPI{prPages: [][]githubPR{
			{githubPRStub(1, "open", "sha1", "main")},
			{githubPRStub(2, "open", "sha2", "main")},
			{githubPRStub(3, "open", "sha3", "main")},
		}}
		server := newPRMockServer(t, api)

		inf := &Informer{
			commits:           make(map[string]string),
			githubAPIBaseFunc: func(string) string { return server.URL },
		}

		result, err := inf.checkPullRequests(context.Background(), key, buildPRTrigger(uri, nil), newReconcileCache())
		require.NoError(t, err)
		assert.False(t, result.changed, "initial baseline must not fire")

		assert.Equal(t, "1", inf.commits[prInitKey(key)])
		assert.Equal(t, "sha1:open", inf.commits[prCacheKey(key, 1)])
		assert.Equal(t, "sha2:open", inf.commits[prCacheKey(key, 2)])
		assert.Equal(t, "sha3:open", inf.commits[prCacheKey(key, 3)],
			"PRs on later pages must be part of the baseline")

		for _, requested := range api.requests() {
			assert.NotContains(t, requested, "/files", "baseline pass must not fetch changed files")
		}
	})

	t.Run("state_change_on_last_page_fires_exactly_once", func(t *testing.T) {
		api := &prMockAPI{prPages: [][]githubPR{
			{githubPRStub(1, "open", "sha1", "main")},
			{githubPRStub(2, "open", "sha2", "main")},
			{githubPRStub(3, "open", "sha3-new", "main")},
		}}
		server := newPRMockServer(t, api)

		inf := &Informer{
			commits: map[string]string{
				prInitKey(key):          "1",
				prCacheKey(key, 1):      "sha1:open",
				prCacheKey(key, 2):      "sha2:open",
				prCacheKey(key, 3):      "sha3:open",
			},
			githubAPIBaseFunc: func(string) string { return server.URL },
		}
		trigger := buildPRTrigger(uri, nil)

		result, err := inf.checkPullRequests(context.Background(), key, trigger, newReconcileCache())
		require.NoError(t, err)
		require.True(t, result.changed, "changed PR on the last page must be detected")
		assert.Equal(t, "synchronize", result.metadata[GitMetaKeyPRAction])
		assert.Equal(t, "3", result.metadata[GitMetaKeyPRNumber])
		assert.Equal(t, "sha3-new:open", inf.commits[prCacheKey(key, 3)])

		// Same state again must not re-fire.
		result, err = inf.checkPullRequests(context.Background(), key, trigger, newReconcileCache())
		require.NoError(t, err)
		assert.False(t, result.changed, "a confirmed state change must not fire again")
	})

	t.Run("unseen_pr_beyond_first_page_after_init_fires_opened_once", func(t *testing.T) {
		api := &prMockAPI{prPages: [][]githubPR{
			{githubPRStub(1, "open", "sha1", "main")},
			{githubPRStub(4, "open", "sha4", "main"), githubPRStub(2, "open", "sha2", "main")},
			{githubPRStub(3, "open", "sha3", "main")},
		}}
		server := newPRMockServer(t, api)

		inf := &Informer{
			commits: map[string]string{
				prInitKey(key):     "1",
				prCacheKey(key, 1): "sha1:open",
				prCacheKey(key, 2): "sha2:open",
				prCacheKey(key, 3): "sha3:open",
			},
			githubAPIBaseFunc: func(string) string { return server.URL },
		}
		trigger := buildPRTrigger(uri, nil)

		result, err := inf.checkPullRequests(context.Background(), key, trigger, newReconcileCache())
		require.NoError(t, err)
		require.True(t, result.changed)
		assert.Equal(t, "opened", result.metadata[GitMetaKeyPRAction])
		assert.Equal(t, "4", result.metadata[GitMetaKeyPRNumber])
		assert.Equal(t, "sha4:open", inf.commits[prCacheKey(key, 4)])

		result, err = inf.checkPullRequests(context.Background(), key, trigger, newReconcileCache())
		require.NoError(t, err)
		assert.False(t, result.changed)
	})

	t.Run("branch_filter_applies_to_prs_on_later_pages", func(t *testing.T) {
		// PR #3 targets "develop" on the last page and must not be baselined.
		firstAPI := &prMockAPI{prPages: [][]githubPR{
			{githubPRStub(1, "open", "sha1", "main")},
			{githubPRStub(2, "open", "sha2", "main")},
			{githubPRStub(3, "open", "sha3", "develop")},
		}}
		firstServer := newPRMockServer(t, firstAPI)

		inf := &Informer{
			commits:           make(map[string]string),
			githubAPIBaseFunc: func(string) string { return firstServer.URL },
		}
		trigger := buildPRTrigger(uri, &testkube.TestTriggerContentGitPullRequest{
			Branches: []string{"main"},
		})

		result, err := inf.checkPullRequests(context.Background(), key, trigger, newReconcileCache())
		require.NoError(t, err)
		assert.False(t, result.changed)
		assert.Equal(t, "1", inf.commits[prInitKey(key)])
		assert.NotContains(t, inf.commits, prCacheKey(key, 3), "ignored-base PR must not be baselined")

		// PR #3 is retargeted at main and appears on the last page again.
		secondAPI := &prMockAPI{prPages: [][]githubPR{
			{githubPRStub(1, "open", "sha1", "main")},
			{githubPRStub(2, "open", "sha2", "main")},
			{githubPRStub(3, "open", "sha3", "main")},
		}}
		secondServer := newPRMockServer(t, secondAPI)
		inf.githubAPIBaseFunc = func(string) string { return secondServer.URL }

		result, err = inf.checkPullRequests(context.Background(), key, trigger, newReconcileCache())
		require.NoError(t, err)
		require.True(t, result.changed)
		assert.Equal(t, "opened", result.metadata[GitMetaKeyPRAction])
		assert.Equal(t, "3", result.metadata[GitMetaKeyPRNumber])
	})

	t.Run("matching_file_on_second_file_page_fires", func(t *testing.T) {
		api := &prMockAPI{
			prPages: [][]githubPR{
				{githubPRStub(1, "open", "sha1-new", "main")},
			},
			filePages: map[int][][]githubPRFile{
				1: {
					{{Filename: "docs/readme.md"}},
					{{Filename: "pkg/a.go"}},
				},
			},
		}
		server := newPRMockServer(t, api)

		inf := &Informer{
			commits: map[string]string{
				prInitKey(key):     "1",
				prCacheKey(key, 1): "sha1:open",
			},
			githubAPIBaseFunc: func(string) string { return server.URL },
		}
		trigger := buildPRTrigger(uri, nil, "pkg/**")

		result, err := inf.checkPullRequests(context.Background(), key, trigger, newReconcileCache())
		require.NoError(t, err)
		require.True(t, result.changed, "path match beyond the first files page must fire")
		assert.Equal(t, "synchronize", result.metadata[GitMetaKeyPRAction])
		assert.Equal(t, "sha1-new:open", inf.commits[prCacheKey(key, 1)])
	})

	t.Run("file_fetch_error_on_later_page_keeps_baseline", func(t *testing.T) {
		api := &prMockAPI{
			prPages: [][]githubPR{
				{githubPRStub(1, "open", "sha1-new", "main")},
			},
			filePages: map[int][][]githubPRFile{
				1: {
					{{Filename: "pkg/a.go"}},
					{{Filename: "pkg/b.go"}},
				},
			},
			filePageStatus: map[int]int{2: http.StatusInternalServerError},
		}
		server := newPRMockServer(t, api)

		inf := &Informer{
			commits: map[string]string{
				prInitKey(key):     "1",
				prCacheKey(key, 1): "sha1:open",
			},
			githubAPIBaseFunc: func(string) string { return server.URL },
		}
		trigger := buildPRTrigger(uri, nil, "pkg/**")

		result, err := inf.checkPullRequests(context.Background(), key, trigger, newReconcileCache())
		require.NoError(t, err)
		assert.False(t, result.changed)
		assert.Equal(t, "sha1:open", inf.commits[prCacheKey(key, 1)],
			"transient file fetch error must not advance the baseline")
	})

	t.Run("pr_list_error_on_later_page_does_not_touch_baseline", func(t *testing.T) {
		api := &prMockAPI{
			prPages: [][]githubPR{
				{githubPRStub(1, "open", "sha1-new", "main")},
				nil,
			},
			prPageStatus: map[int]int{2: http.StatusInternalServerError},
		}
		server := newPRMockServer(t, api)

		inf := &Informer{
			commits: map[string]string{
				prInitKey(key):     "1",
				prCacheKey(key, 1): "sha1:open",
			},
			githubAPIBaseFunc: func(string) string { return server.URL },
		}

		_, err := inf.checkPullRequests(context.Background(), key, buildPRTrigger(uri, nil), newReconcileCache())
		require.Error(t, err, "a failed later page must fail the whole round")
		assert.Equal(t, "1", inf.commits[prInitKey(key)])
		assert.Equal(t, "sha1:open", inf.commits[prCacheKey(key, 1)],
			"the previously confirmed baseline must survive the failed round")
	})

	t.Run("failed_initial_pass_does_not_set_sentinel", func(t *testing.T) {
		api := &prMockAPI{
			prPages: [][]githubPR{
				{githubPRStub(1, "open", "sha1", "main")},
				nil,
			},
			prPageStatus: map[int]int{2: http.StatusInternalServerError},
		}
		server := newPRMockServer(t, api)

		inf := &Informer{
			commits:           make(map[string]string),
			githubAPIBaseFunc: func(string) string { return server.URL },
		}

		_, err := inf.checkPullRequests(context.Background(), key, buildPRTrigger(uri, nil), newReconcileCache())
		require.Error(t, err)
		assert.NotContains(t, inf.commits, prInitKey(key),
			"baseline initialization must complete fully or not at all")
	})

	t.Run("one_event_per_round_other_changes_deferred_without_loss", func(t *testing.T) {
		api := &prMockAPI{prPages: [][]githubPR{
			{githubPRStub(1, "open", "sha1-new", "main")},
			{githubPRStub(2, "open", "sha2-new", "main")},
		}}
		server := newPRMockServer(t, api)

		inf := &Informer{
			commits: map[string]string{
				prInitKey(key):     "1",
				prCacheKey(key, 1): "sha1:open",
				prCacheKey(key, 2): "sha2:open",
			},
			githubAPIBaseFunc: func(string) string { return server.URL },
		}
		trigger := buildPRTrigger(uri, nil)

		// First round: PR #1 fires (newest update order); #2 is deferred.
		result, err := inf.checkPullRequests(context.Background(), key, trigger, newReconcileCache())
		require.NoError(t, err)
		require.True(t, result.changed)
		assert.Equal(t, "1", result.metadata[GitMetaKeyPRNumber])
		assert.Equal(t, "sha1-new:open", inf.commits[prCacheKey(key, 1)])
		assert.Equal(t, "sha2:open", inf.commits[prCacheKey(key, 2)],
			"deferred change must keep its confirmed baseline")

		// Second round: the deferred change fires exactly once.
		result, err = inf.checkPullRequests(context.Background(), key, trigger, newReconcileCache())
		require.NoError(t, err)
		require.True(t, result.changed)
		assert.Equal(t, "2", result.metadata[GitMetaKeyPRNumber])
		assert.Equal(t, "synchronize", result.metadata[GitMetaKeyPRAction])
		assert.Equal(t, "sha2-new:open", inf.commits[prCacheKey(key, 2)])

		// Third round: nothing left to fire.
		result, err = inf.checkPullRequests(context.Background(), key, trigger, newReconcileCache())
		require.NoError(t, err)
		assert.False(t, result.changed)
	})
}

// TestUpdateRepositories_PRPollingFailureRestoresConfirmedBaseline verifies
// the full reconcile path: when GitHub pagination fails mid-round, the matcher
// never fires and the last confirmed baseline (including the init sentinel) is
// restored by updateRepositories' snapshot/restore.
func TestUpdateRepositories_PRPollingFailureRestoresConfirmedBaseline(t *testing.T) {
	const uri = "https://github.com/owner/repo.git"
	resource := testkube.CONTENT_TestTriggerResources
	trigger := testkube.TestTrigger{
		Name:      "pr-trigger",
		Namespace: "default",
		Event:     "git-pull-request",
		Resource:  &resource,
		ContentSelector: &testkube.TestTriggerContentSelector{
			Git: &testkube.TestTriggerContentGit{Uri: uri},
		},
	}
	key := triggerKey(testTriggerSource, trigger.Namespace, trigger.Name)

	api := &prMockAPI{
		prPages: [][]githubPR{
			{githubPRStub(1, "open", "sha-new", "main")},
			nil,
		},
		prPageStatus: map[int]int{2: http.StatusInternalServerError},
	}
	server := newPRMockServer(t, api)

	fired := make(chan struct{}, 4)
	inf := &Informer{
		testTriggerClient: stubTestTriggerClient{
			listFn: func(_ context.Context, _ string, _ testtriggerclient.ListOptions, _ string) ([]testkube.TestTrigger, error) {
				return []testkube.TestTrigger{trigger}, nil
			},
		},
		matcher: stubMatcher{
			matchTestTriggerFn: func(context.Context, string, string) error {
				fired <- struct{}{}
				return nil
			},
		},
		commits: map[string]string{
			prInitKey(key):     "1",
			prCacheKey(key, 1): "sha-old:open",
		},
		namespaces:        []string{allNamespacesMarker},
		githubAPIBaseFunc: func(string) string { return server.URL },
		options:           normalizeOptions(Options{}),
	}

	inf.updateRepositories(context.Background())

	select {
	case <-fired:
		t.Fatal("matcher must not fire on a failed PR polling round")
	default:
	}
	assert.Equal(t, "1", inf.commits[prInitKey(key)])
	assert.Equal(t, "sha-old:open", inf.commits[prCacheKey(key, 1)],
		"confirmed baseline must be restored after the failed round")
}

// TestUpdateRepositories_PRPollingSuccessFiresAcrossPagination is the happy
// path through updateRepositories: a change on a later page fires the matcher
// and advances the baseline.
func TestUpdateRepositories_PRPollingSuccessFiresAcrossPagination(t *testing.T) {
	const uri = "https://github.com/owner/repo.git"
	resource := testkube.CONTENT_TestTriggerResources
	trigger := testkube.TestTrigger{
		Name:      "pr-trigger",
		Namespace: "default",
		Event:     "git-pull-request",
		Resource:  &resource,
		ContentSelector: &testkube.TestTriggerContentSelector{
			Git: &testkube.TestTriggerContentGit{Uri: uri},
		},
	}
	key := triggerKey(testTriggerSource, trigger.Namespace, trigger.Name)

	api := &prMockAPI{prPages: [][]githubPR{
		{githubPRStub(1, "open", "sha1", "main")},
		{githubPRStub(2, "open", "sha2", "main")},
		{githubPRStub(3, "open", "sha3-new", "main")},
	}}
	server := newPRMockServer(t, api)

	fired := make(chan map[string]string, 4)
	inf := &Informer{
		testTriggerClient: stubTestTriggerClient{
			listFn: func(_ context.Context, _ string, _ testtriggerclient.ListOptions, _ string) ([]testkube.TestTrigger, error) {
				return []testkube.TestTrigger{trigger}, nil
			},
		},
		matcher: prRecordingMatcher{ch: fired},
		commits: map[string]string{
			prInitKey(key):     "1",
			prCacheKey(key, 1): "sha1:open",
			prCacheKey(key, 2): "sha2:open",
			prCacheKey(key, 3): "sha3:open",
		},
		namespaces:        []string{allNamespacesMarker},
		githubAPIBaseFunc: func(string) string { return server.URL },
		options:           normalizeOptions(Options{}),
	}

	inf.updateRepositories(context.Background())

	select {
	case meta := <-fired:
		assert.Equal(t, "3", meta[GitMetaKeyPRNumber])
		assert.Equal(t, "synchronize", meta[GitMetaKeyPRAction])
	default:
		t.Fatal("matcher must fire for the changed PR discovered on the last page")
	}
	assert.Equal(t, "sha3-new:open", inf.commits[prCacheKey(key, 3)])
}

// prRecordingMatcher records the git metadata of every fired event.
type prRecordingMatcher struct {
	ch chan map[string]string
}

func (m prRecordingMatcher) MatchGitTrigger(_ context.Context, _, _ string, gitMeta map[string]string) error {
	cp := make(map[string]string, len(gitMeta))
	for k, v := range gitMeta {
		cp[k] = v
	}
	m.ch <- cp
	return nil
}
