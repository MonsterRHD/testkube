package informer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	testtriggersv1 "github.com/kubeshop/testkube/api/testtriggers/v1"
	"github.com/kubeshop/testkube/pkg/api/v1/testkube"
	"github.com/kubeshop/testkube/pkg/git/matchers"
	"github.com/kubeshop/testkube/pkg/log"
)

// githubPR represents a minimal GitHub pull request from the REST API.
type githubPR struct {
	Number    int       `json:"number"`
	State     string    `json:"state"`
	Title     string    `json:"title"`
	UpdatedAt time.Time `json:"updated_at"`
	HTMLURL   string    `json:"html_url"`
	Head      struct {
		Ref string `json:"ref"`
		SHA string `json:"sha"`
	} `json:"head"`
	Base struct {
		Ref string `json:"ref"`
	} `json:"base"`
	User struct {
		Login string `json:"login"`
	} `json:"user"`
	Draft bool `json:"draft"`
}

// githubPRFile represents a file changed in a pull request.
type githubPRFile struct {
	Filename string `json:"filename"`
	Status   string `json:"status"`
}

var githubRepoPattern = regexp.MustCompile(`(?:github\.com|github\.[^/:]+)[/:]([^/]+)/([^/]+?)(?:\.git)?/?$`)

// githubHTTPClient is used for GitHub API requests with an appropriate timeout.
var githubHTTPClient = &http.Client{Timeout: 30 * time.Second}

const (
	githubPRPageSize      = 100
	githubPRFilesPageSize = 100

	// defaultGitHubMaxPages bounds how many Link-header pages the informer
	// walks per endpoint per reconcile. It guards against runaway loops on
	// malformed/proxied responses and can be raised via the
	// TEST_TRIGGER_GIT_INFORMER_GITHUB_MAX_PAGES configuration for very large
	// repositories.
	defaultGitHubMaxPages = 100
)

const githubPRNoTokenProviderWarning = "github authType configured for PR polling but no GitHub token provider is available, falling back to configured credentials"

// parseGitHubRepo extracts owner/repo from a GitHub URL (HTTPS or SSH).
func parseGitHubRepo(uri string) (owner, repo string, ok bool) {
	matches := githubRepoPattern.FindStringSubmatch(uri)
	if len(matches) < 3 {
		return "", "", false
	}
	return matches[1], matches[2], true
}

// sanitizeGitHubTokenURI strips userinfo from GitHub URIs before they are sent
// to the token provider or used as cache keys. If parsing fails or no userinfo
// is present, the original URI is returned unchanged.
func sanitizeGitHubTokenURI(uri string) string {
	u, err := url.Parse(uri)
	if err != nil || u.User == nil {
		return uri
	}
	u.User = nil
	return u.String()
}

// githubAPIBaseFromURI returns the GitHub API base URL for the given repo URI.
// For github.com it returns "https://api.github.com", for GHES it derives from the host.
func githubAPIBaseFromURI(uri string) string {
	// Try to parse as URL
	u, err := url.Parse(uri)
	if err == nil && u.Host != "" && u.Host != "github.com" && !strings.HasSuffix(u.Host, "github.com") {
		// GHES
		return fmt.Sprintf("https://%s/api/v3", u.Host)
	}
	return "https://api.github.com"
}

// parseGitHubNextLink extracts the rel="next" URL from a GitHub REST API
// Link response header. The header has the form
//
//	<https://api.github.com/...?page=2>; rel="next", <...?page=5>; rel="last"
//
// Returns an empty string when no next page is present.
func parseGitHubNextLink(header string) string {
	for _, link := range strings.Split(header, ",") {
		parts := strings.Split(link, ";")
		if len(parts) < 2 {
			continue
		}
		urlPart := strings.TrimSpace(parts[0])
		if len(urlPart) < 2 || urlPart[0] != '<' || urlPart[len(urlPart)-1] != '>' {
			continue
		}
		for _, param := range parts[1:] {
			// Parameter form is rel="next"; compare the key and the
			// dequoted value so spacing/casing does not break the parse.
			kv := strings.SplitN(strings.TrimSpace(param), "=", 2)
			if len(kv) != 2 {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(kv[0]), "rel") &&
				strings.EqualFold(strings.Trim(strings.TrimSpace(kv[1]), `"`), "next") {
				return urlPart[1 : len(urlPart)-1]
			}
		}
	}
	return ""
}

// githubGet performs a single authenticated GitHub API GET and returns the
// response body and the rel="next" URL advertised in the Link header.
func githubGet(ctx context.Context, endpoint, token string) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := githubHTTPClient.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode != http.StatusOK {
		snippet := body
		if len(snippet) > 1024 {
			snippet = snippet[:1024]
		}
		return nil, "", fmt.Errorf("GitHub API returned %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	return body, parseGitHubNextLink(resp.Header.Get("Link")), nil
}

// normalizeMaxPages applies the default page cap for direct callers that do
// not supply one.
func normalizeMaxPages(maxPages int) int {
	if maxPages <= 0 {
		return defaultGitHubMaxPages
	}
	return maxPages
}

// fetchGitHubPRs fetches pull requests (all states) from the GitHub REST API,
// following rel="next" Link headers until every PR page has been traversed or
// maxPages is reached. Results are sorted by last update, newest first. When
// since is non-zero, PRs last updated before it are filtered out after
// pagination completes.
func fetchGitHubPRs(ctx context.Context, apiBase, owner, repo, token string, since time.Time, maxPages int) ([]githubPR, error) {
	maxPages = normalizeMaxPages(maxPages)
	endpoint := fmt.Sprintf("%s/repos/%s/%s/pulls?state=all&sort=updated&direction=desc&per_page=%d",
		apiBase, owner, repo, githubPRPageSize)

	var prs []githubPR
	for page := 1; endpoint != ""; page++ {
		if page > maxPages {
			log.DefaultLogger.Warnf(
				"git informer: GitHub PR list pagination limit of %d pages reached for %s/%s; remaining PRs were not scanned this round, raise TEST_TRIGGER_GIT_INFORMER_GITHUB_MAX_PAGES if this is expected",
				maxPages, owner, repo,
			)
			break
		}

		body, next, err := githubGet(ctx, endpoint, token)
		if err != nil {
			return nil, err
		}

		var pagePRs []githubPR
		if err := json.Unmarshal(body, &pagePRs); err != nil {
			return nil, err
		}
		prs = append(prs, pagePRs...)
		endpoint = next
	}

	// Filter by update time after all pages are collected so nothing is dropped
	// just because it did not fit on the first page.
	if !since.IsZero() {
		filtered := prs[:0]
		for _, pr := range prs {
			if !pr.UpdatedAt.Before(since) {
				filtered = append(filtered, pr)
			}
		}
		prs = filtered
	}

	return prs, nil
}

// fetchGitHubPRFiles fetches every file changed in a pull request, following
// rel="next" Link headers until all pages are traversed or maxPages is
// reached, so path matching also works for PRs larger than a single page.
func fetchGitHubPRFiles(ctx context.Context, apiBase, owner, repo, token string, prNumber, maxPages int) ([]string, error) {
	maxPages = normalizeMaxPages(maxPages)
	endpoint := fmt.Sprintf("%s/repos/%s/%s/pulls/%d/files?per_page=%d",
		apiBase, owner, repo, prNumber, githubPRFilesPageSize)

	paths := make([]string, 0)
	for page := 1; endpoint != ""; page++ {
		if page > maxPages {
			log.DefaultLogger.Warnf(
				"git informer: GitHub PR files pagination limit of %d pages reached for %s/%s PR #%d; remaining changed files were not scanned this round, raise TEST_TRIGGER_GIT_INFORMER_GITHUB_MAX_PAGES if this is expected",
				maxPages, owner, repo, prNumber,
			)
			break
		}

		body, next, err := githubGet(ctx, endpoint, token)
		if err != nil {
			return nil, err
		}

		var files []githubPRFile
		if err := json.Unmarshal(body, &files); err != nil {
			return nil, err
		}
		for _, f := range files {
			paths = append(paths, f.Filename)
		}
		endpoint = next
	}

	return paths, nil
}

// isPullRequestTrigger returns true if any of the trigger's effective events
// is git-pull-request.
func isPullRequestTrigger(trigger testkube.TestTrigger) bool {
	for _, e := range trigger.EffectiveEvents() {
		if strings.ToLower(e) == string(testtriggersv1.TestTriggerEventGitPullRequest) {
			return true
		}
	}
	return false
}

// checkPullRequests polls GitHub for PRs matching the trigger configuration and
// fires events.
//
// Baseline semantics:
//
//   - The PR list and every PR's changed-file list are traversed in full via
//     Link-header pagination, so PRs or files beyond the first page cannot be
//     missed.
//   - On the first successful pass for a trigger every currently-visible,
//     branch-matching PR is recorded as the baseline without firing. The init
//     sentinel is only written after that whole traversal succeeds, so a failed
//     first pass never makes existing PRs look "opened" later.
//   - After initialization, an unseen PR is "opened" and a changed SHA/state is
//     "synchronize"/"closed"/"reopened". Each state change fires exactly once:
//     its baseline advances only when the event fires. At most one event is
//     emitted per trigger per round; further matching changes in the same round
//     keep their old baseline and fire on following rounds, in update order.
//   - Filter rejections (action type, non-matching paths) advance the baseline
//     so the same state is not re-evaluated forever. A transient changed-files
//     fetch error does NOT advance the baseline, so the change is retried.
//   - Any failure while listing PRs returns before baselines are touched, and
//     the caller (updateRepositories) restores the last confirmed baseline.
func (i *Informer) checkPullRequests(ctx context.Context, key string, trigger testkube.TestTrigger, cache *reconcileCache) (matchResult, error) {
	gitConfig := trigger.ContentSelector.Git
	if gitConfig == nil {
		return matchResult{}, nil
	}

	owner, repo, ok := parseGitHubRepo(gitConfig.Uri)
	if !ok {
		return matchResult{}, fmt.Errorf("git-pull-request trigger requires a GitHub repository URL, got: %s", gitConfig.Uri)
	}

	// Resolve token for API authentication.
	token := i.resolvePRToken(ctx, trigger.Namespace, gitConfig, cache)
	apiBase := githubAPIBaseFromURI(gitConfig.Uri)
	if i.githubAPIBaseFunc != nil {
		apiBase = i.githubAPIBaseFunc(gitConfig.Uri)
	}

	// Fetch every PR page BEFORE touching any baseline state. An error here
	// (including a failure on a later page) leaves i.commits untouched so the
	// caller's snapshot/restore keeps the last confirmed baseline.
	prs, err := fetchGitHubPRs(ctx, apiBase, owner, repo, token, time.Time{}, i.options.GitHubMaxPages)
	if err != nil {
		return matchResult{}, fmt.Errorf("failed to fetch PRs: %w", err)
	}

	prConfig := gitConfig.PullRequest

	paths := matchers.NormalizePaths(gitConfig.Paths)
	pathsIgnore := matchers.NormalizePaths(gitConfig.PathsIgnore)

	// On the first reconcile pass for a trigger all currently-visible PRs are
	// recorded as baselines without firing.  Once the sentinel is set, a PR
	// that has never been seen before is treated as "opened".
	initKey := prInitKey(key)
	prInitialized := i.commits[initKey] != ""

	// eventMeta holds the single event emitted this round; later matching
	// changes are deferred to the next round instead of being baselined.
	var eventMeta map[string]string

	for _, pr := range prs {
		// Apply base branch filters before state tracking. A nil prConfig
		// preserves the pre-refactor "match all" semantic.
		if prConfig != nil && !matchers.PRMatchesBaseBranch(pr.Base.Ref, prConfig.Branches, prConfig.BranchesIgnore) {
			continue
		}

		prKey := prCacheKey(key, pr.Number)
		prev, hasPrev := i.commits[prKey]
		currentState := pr.Head.SHA + ":" + pr.State

		if !hasPrev {
			if !prInitialized {
				// Initial baseline pass: record the current state without firing.
				i.commits[prKey] = currentState
				log.DefaultLogger.Infof("git informer: initializing PR baseline for trigger %s/%s PR #%d",
					trigger.Namespace, trigger.Name, pr.Number)
				continue
			}
			// Trigger is initialized: a PR that has never been seen is "opened".
		} else if prev == currentState {
			continue // No change
		}

		// Determine action.
		var action string
		if !hasPrev {
			action = "opened"
		} else {
			action = matchers.DeterminePRAction(prev, currentState, pr.Head.SHA)
		}

		// Apply type filter. Advance baseline so the same state is not re-evaluated
		// on the next reconcile regardless of whether the action matches. A nil
		// prConfig preserves the pre-refactor "match all" semantic.
		if prConfig != nil && !matchers.PRMatchesTypes(action, prConfig.Types) {
			i.commits[prKey] = currentState
			continue
		}

		// Apply path filters if configured.
		if len(paths) > 0 || len(pathsIgnore) > 0 {
			changedFiles, fileErr := fetchGitHubPRFiles(ctx, apiBase, owner, repo, token, pr.Number, i.options.GitHubMaxPages)
			if fileErr != nil {
				// Transient error: do NOT advance the baseline so the event can be
				// retried on the next reconcile.
				log.DefaultLogger.Warnf("git informer: failed to fetch PR #%d files: %v", pr.Number, fileErr)
				continue
			}
			if !matchers.PRPathsMatch(changedFiles, paths, pathsIgnore) {
				// Paths do not match; advance baseline to skip this state on the next pass.
				i.commits[prKey] = currentState
				continue
			}
		}

		// All filters passed. Only the first matching change in the round fires
		// and advances; leave later ones on their confirmed baseline so each
		// state change emits exactly once, on its own subsequent round.
		if eventMeta != nil {
			continue
		}
		i.commits[prKey] = currentState

		eventMeta = map[string]string{
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
	}

	// Mark the trigger as initialized only after the full PR list was traversed
	// and every current PR was recorded. The initial pass never produces an
	// event, but guard on eventMeta so an interrupted-looking pass cannot both
	// fire and flip the sentinel.
	if !prInitialized && eventMeta == nil {
		i.commits[initKey] = "1"
	}

	if eventMeta != nil {
		return matchResult{changed: true, metadata: eventMeta}, nil
	}
	return matchResult{}, nil
}

func (i *Informer) resolvePRToken(ctx context.Context, namespace string, gitConfig *testkube.TestTriggerContentGit, cache *reconcileCache) string {
	// If authType is "github", fetch token from control plane.
	authType := strings.ToLower(gitConfig.AuthType)
	if authType == string(testkube.GITHUB_ContentGitAuthType) {
		if i.githubTokenProvider == nil {
			log.DefaultLogger.Warnw(githubPRNoTokenProviderWarning)
		} else {
			// Use the per-reconcile cache to avoid repeated gRPC calls for the same URI.
			uri := sanitizeGitHubTokenURI(gitConfig.Uri)

			if token, ok := cache.githubToken(uri); ok {
				return token
			}
			token, err := i.githubTokenProvider.GetGitHubToken(ctx, uri)
			if err != nil {
				log.DefaultLogger.Warnw("failed to get GitHub App token for PR polling, falling back to configured credentials", "error", err)
			} else if strings.TrimSpace(token) == "" {
				log.DefaultLogger.Warnw("received empty GitHub App token for PR polling, falling back to configured credentials")
			} else {
				cache.setGitHubToken(uri, token)
				return token
			}
		}
	}
	if i.kubeClient != nil {
		return i.resolveCredentialValue(ctx, gitConfig.Token, namespace, gitConfig.TokenFrom)
	}
	return resolveCredentialValue(gitConfig.Token, gitConfig.TokenFrom)
}

func prCacheKey(triggerKey string, prNumber int) string {
	// Use refSeparator so PR baselines are treated like other per-trigger sub-keys
	// by snapshot/restore and cleanup logic.
	return triggerKey + refSeparator + "pr:" + strconv.Itoa(prNumber)
}

// prInitKey returns the commit-map key used as a sentinel to mark that the
// trigger has completed its initial PR baseline pass. After the sentinel is
// set, new PRs that have never been seen before are treated as "opened".
func prInitKey(triggerKey string) string {
	return triggerKey + refSeparator + "pr:__init__"
}
