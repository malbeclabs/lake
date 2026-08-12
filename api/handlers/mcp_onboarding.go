package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/malbeclabs/lake/utils/pkg/docsfetch"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Onboarding tools live on the single DoubleZero MCP (this server). Runbook
// markdown stays in public malbeclabs/docs; we fetch it via GitHub raw.
//
//   - get_onboarding_runbook: entrypoint on "walk me through connecting…"
//   - check_edge_access: runs `doublezero access-pass get` (user payer + receiving IP)
//     against onchain state, which is the source of truth for access passes

// defaultDocsSource fetches docs markdown from GitHub raw. It is read-only;
// tests inject their own client via API.DocsSource instead of mutating this.
var defaultDocsSource = docsfetch.FromEnv()

// docsSource returns the docs client to read runbooks and docs pages with.
func (a *API) docsSource() *docsfetch.Client {
	if a.DocsSource != nil {
		return a.DocsSource
	}
	return defaultDocsSource
}

// onboardingPreamble tells the agent how to drive a runbook. Returned with the
// runbook so the guidance travels with the data, client-agnostically.
const onboardingPreamble = `You are guiding a user through DoubleZero onboarding using the runbook below.

How to drive it:
- If the runbook has YAML front-matter with 'steps', that is the step skeleton; walk the steps in order. The markdown body has one section per step id.
- If the runbook defines multiple 'modes', ask the user which one they want before starting, then follow steps for that mode (steps with no 'mode' apply to all).
- If there is no step skeleton (plain LLM runbook), follow the numbered sections in order and treat the "What success looks like" / verify commands as the done condition.
- Each structured step has a 'locus':
    - local  -> run the commands ON THE TARGET HOST. That is the machine that will receive the feed; reach it locally or over SSH. Never assume localhost. Show the user what you run and its output.
    - verify -> call the named MCP tool (see the step's 'tool'/'inputs') to check backend state; do not fake it.
    - external -> the user must do this out-of-band (e.g. buy a feed, request an access pass). Hand them the exact details and wait.
- Honor data flow: a step's 'produces' value (e.g. pubkey from 'doublezero address') feeds a later step's 'inputs'. Capture and reuse it.
- Confirm each step's 'verify' condition before advancing. If a 'blocking' step fails, stop and resolve it before continuing.
- Go one step at a time. Explain briefly, run/verify, then continue.

Runbook:
`

// runbookIndexPage is the docs slug the MCP reads from GitHub raw to discover
// runbooks. Add a walkthrough by linking it on that file; do not edit lake.
//
// Docs dependency: the index page ships separately in malbeclabs/docs
// (https://github.com/malbeclabs/docs/pull/197) and cannot merge in this PR.
const runbookIndexPage = "runbooks"

type runbookRef struct {
	Service string
	Page    string
	Title   string
}

// runbookListItem matches "- `service` — [Title](page.md)" or "- [Title](page.md)".
var runbookListItem = regexp.MustCompile("(?m)^[\\t ]*[-*][\\t ]+(?:`([a-zA-Z0-9][a-zA-Z0-9\\-]*)`[\\t ]*[—–-]+[\\t ]+)?\\[([^\\]]+)\\]\\(([^)]+)\\)")

var fencedBlock = regexp.MustCompile("(?s)```.*?```")

func (a *API) loadRunbookCatalog(ctx context.Context) ([]runbookRef, error) {
	content, _, err := a.docsSource().Read(ctx, runbookIndexPage)
	if err != nil {
		return nil, fmt.Errorf("failed to load runbook index: %w", err)
	}
	refs := parseRunbookIndex(content)
	if len(refs) == 0 {
		return nil, fmt.Errorf("runbook index %q has no runbook links", runbookIndexPage)
	}
	return refs, nil
}

func catalogServices(refs []runbookRef) []string {
	out := make([]string, 0, len(refs))
	for _, e := range refs {
		out = append(out, e.Service)
	}
	return out
}

func catalogPage(refs []runbookRef, service string) (page string, ok bool) {
	for _, e := range refs {
		if e.Service == service || e.Page == service {
			return e.Page, true
		}
	}
	return "", false
}

func parseRunbookIndex(md string) []runbookRef {
	md = fencedBlock.ReplaceAllString(md, "")
	if section := runbookIndexSection(md); section != "" {
		md = section
	}

	var out []runbookRef
	seen := map[string]bool{}
	for _, m := range runbookListItem.FindAllStringSubmatch(md, -1) {
		service, title, href := m[1], strings.TrimSpace(m[2]), m[3]
		page := runbookPageFromHref(href)
		if page == "" || page == runbookIndexPage || page == "mcp" {
			continue
		}
		if service == "" {
			service = page
		}
		if !docsfetch.ValidPage(service) || !docsfetch.ValidPage(page) {
			continue
		}
		if seen[service] {
			continue
		}
		seen[service] = true
		out = append(out, runbookRef{Service: service, Page: page, Title: title})
	}
	return out
}

func runbookIndexSection(md string) string {
	const heading = "## Index"
	i := strings.Index(md, heading)
	if i < 0 {
		return ""
	}
	rest := md[i+len(heading):]
	if next := strings.Index(rest, "\n## "); next >= 0 {
		rest = rest[:next]
	}
	return rest
}

func runbookPageFromHref(href string) string {
	href = strings.TrimSpace(href)
	href = strings.SplitN(href, " ", 2)[0]
	href = strings.SplitN(href, "#", 2)[0]
	href = strings.Trim(href, "<>")
	if strings.Contains(href, "://") {
		return ""
	}
	href = strings.TrimPrefix(href, "./")
	if !strings.HasSuffix(strings.ToLower(href), ".md") {
		return ""
	}
	base := path.Base(href)
	return strings.TrimSuffix(base, path.Ext(base))
}

// GetOnboardingRunbookInput is the input for the get_onboarding_runbook tool.
type GetOnboardingRunbookInput struct {
	Service string `json:"service,omitempty" jsonschema:"Service id from the docs runbooks index. Omit to list available runbooks."`
	Mode    string `json:"mode,omitempty" jsonschema:"Optional connection mode to follow (e.g. 'edge-connect'). The agent may also let the user choose after reading the runbook."`
}

// GetOnboardingRunbookOutput is the output from the get_onboarding_runbook tool.
type GetOnboardingRunbookOutput struct {
	Service           string   `json:"service"`
	Page              string   `json:"page,omitempty"`
	Source            string   `json:"source,omitempty"`
	Mode              string   `json:"mode,omitempty"`
	AvailableRunbooks []string `json:"available_runbooks"`
	Preamble          string   `json:"preamble"`
	Runbook           string   `json:"runbook"`
}

func (a *API) registerGetOnboardingRunbookTool(server *mcp.Server) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_onboarding_runbook",
		Title:       "Get Onboarding Runbook",
		Description: "Start a guided onboarding walkthrough for connecting to a DoubleZero service. Call this whenever a user asks to be walked through, set up, connect to, or onboard onto a DoubleZero service. Returns a step-by-step runbook from DoubleZero docs plus instructions for how to guide the user through it. Omit 'service' to list available runbooks.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, func(ctx context.Context, req *mcp.CallToolRequest, input GetOnboardingRunbookInput) (*mcp.CallToolResult, GetOnboardingRunbookOutput, error) {
		catalog, err := a.loadRunbookCatalog(ctx)
		if err != nil {
			return nil, GetOnboardingRunbookOutput{}, err
		}
		available := catalogServices(catalog)

		service := strings.TrimSpace(input.Service)
		if service == "" {
			return nil, GetOnboardingRunbookOutput{
				AvailableRunbooks: available,
				Preamble:          "Ask the user which service they want to onboard onto, then call get_onboarding_runbook again with that 'service'.",
			}, nil
		}

		page, ok := catalogPage(catalog, service)
		if !ok || !docsfetch.ValidPage(service) || !docsfetch.ValidPage(page) {
			return nil, GetOnboardingRunbookOutput{}, fmt.Errorf("unknown runbook %q (available: %s)", service, strings.Join(available, ", "))
		}

		content, source, err := a.loadRunbook(ctx, page)
		if err != nil {
			return nil, GetOnboardingRunbookOutput{}, err
		}

		return nil, GetOnboardingRunbookOutput{
			Service:           service,
			Page:              page,
			Source:            source,
			Mode:              strings.TrimSpace(input.Mode),
			AvailableRunbooks: available,
			Preamble:          onboardingPreamble,
			Runbook:           content,
		}, nil
	})
}

func (a *API) loadRunbook(ctx context.Context, page string) (content, source string, err error) {
	return a.docsSource().Read(ctx, page)
}

// CheckEdgeAccessInput is the input for the check_edge_access tool.
type CheckEdgeAccessInput struct {
	Pubkey   string `json:"pubkey" jsonschema:"The user payer pubkey of the access pass (for self-service passes this is the wallet from 'doublezero address')"`
	PublicIP string `json:"public_ip" jsonschema:"The public IPv4 address the user will receive the feed on"`
}

// CheckEdgeAccessOutput is the output from the check_edge_access tool.
type CheckEdgeAccessOutput struct {
	Status   string `json:"status"` // "active" | "pending"
	Message  string `json:"message"`
	PassPK   string `json:"pass_pk,omitempty"`
	ClientIP string `json:"client_ip,omitempty"`
	TypeTag  string `json:"type_tag,omitempty"`
}

type edgeAccessPass struct {
	PK       string
	ClientIP string
	Status   string
	TypeTag  string
}

// DZCLIRunner runs the doublezero CLI with the given args and returns its
// stdout. The API's DZCLI field lets tests inject one; nil means exec the real
// binary on the host.
type DZCLIRunner func(ctx context.Context, args ...string) ([]byte, error)

func (a *API) registerCheckEdgeAccessTool(server *mcp.Server, r *http.Request) {
	clientIP := GetIPFromRequest(r)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "check_edge_access",
		Title:       "Check Edge Access",
		Description: "Check whether a DoubleZero user payer pubkey is authorized for a given receiving IP by looking up its access pass onchain (via the doublezero CLI). Pass the user payer pubkey and the public IPv4 the user will receive on. Returns status 'active' if a connected pass covers that IP (exact match or 0.0.0.0), otherwise 'pending'.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, func(ctx context.Context, req *mcp.CallToolRequest, input CheckEdgeAccessInput) (*mcp.CallToolResult, CheckEdgeAccessOutput, error) {
		if errMsg := CheckRateLimit(QueryRateLimiter, clientIP); errMsg != "" {
			return nil, CheckEdgeAccessOutput{}, errors.New(errMsg)
		}

		pubkey := strings.TrimSpace(input.Pubkey)
		ip := strings.TrimSpace(input.PublicIP)
		if pubkey == "" || ip == "" {
			return nil, CheckEdgeAccessOutput{}, errors.New("both pubkey and public_ip are required")
		}
		parsed := net.ParseIP(ip)
		if parsed == nil || parsed.To4() == nil {
			return nil, CheckEdgeAccessOutput{}, errors.New("public_ip must be a valid IPv4 address")
		}
		ip = parsed.To4().String()

		pass, err := a.lookupEdgeAccessPass(ctx, pubkey, ip)
		if err != nil {
			return nil, CheckEdgeAccessOutput{}, err
		}
		return nil, classifyEdgeAccess(pubkey, ip, pass), nil
	})
}

// runDoubleZeroCLI executes the doublezero CLI on the API host and returns its
// stdout. CLI errors carry the command's stderr so the caller sees the real cause.
func runDoubleZeroCLI(ctx context.Context, args ...string) ([]byte, error) {
	out, err := exec.CommandContext(ctx, "doublezero", args...).Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return nil, fmt.Errorf("doublezero %s: %s", strings.Join(args, " "), strings.TrimSpace(string(exitErr.Stderr)))
		}
		return nil, fmt.Errorf("doublezero CLI: %w", err)
	}
	return out, nil
}

// lookupEdgeAccessPass mirrors `doublezero access-pass get --user-payer X
// --client-ip Y`. Onchain state is the source of truth for access passes; the
// CLI resolves the 0.0.0.0 (any-IP) wildcard itself and errors with "Access
// Pass not found" on a miss. Returns (nil, nil) when no pass exists.
func (a *API) lookupEdgeAccessPass(ctx context.Context, userPayer, publicIP string) (*edgeAccessPass, error) {
	run := a.DZCLI
	if run == nil {
		run = runDoubleZeroCLI
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	out, err := run(ctx, "access-pass", "get", "--user-payer", userPayer, "--client-ip", publicIP, "--json")
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "access pass not found") {
			return nil, nil
		}
		return nil, fmt.Errorf("access pass lookup failed: %w", err)
	}

	var pass struct {
		Account  string `json:"account"`
		Type     string `json:"type"`
		ClientIP string `json:"client_ip"`
		Status   string `json:"status"`
	}
	if err := json.Unmarshal(out, &pass); err != nil {
		return nil, fmt.Errorf("unexpected access-pass get output: %w", err)
	}
	return &edgeAccessPass{PK: pass.Account, ClientIP: pass.ClientIP, Status: pass.Status, TypeTag: pass.Type}, nil
}

func classifyEdgeAccess(pubkey, publicIP string, pass *edgeAccessPass) CheckEdgeAccessOutput {
	if pass == nil {
		return CheckEdgeAccessOutput{
			Status: "pending",
			Message: fmt.Sprintf("%s has no access pass for %s (or 0.0.0.0). Issue a pass for this user payer+IP and recharge credits, then re-check.",
				pubkey, publicIP),
		}
	}
	out := CheckEdgeAccessOutput{
		PassPK:   pass.PK,
		ClientIP: pass.ClientIP,
		TypeTag:  pass.TypeTag,
	}
	// Status vocabulary is the serviceability AccessPassStatus enum:
	// requested | connected | disconnected | expired. Only connected authorizes.
	switch strings.TrimSuffix(strings.ToLower(pass.Status), " (deprecated)") {
	case "connected":
		out.Status = "active"
		out.Message = fmt.Sprintf("%s is authorized for %s (access pass %s, connected). Proceed to subscribe.",
			pubkey, publicIP, pass.PK)
	case "requested":
		out.Status = "pending"
		out.Message = fmt.Sprintf("Access pass %s for %s on %s is requested and waiting to be approved. Re-check once it is connected.",
			pass.PK, pubkey, publicIP)
	case "expired":
		out.Status = "pending"
		out.Message = fmt.Sprintf("Access pass %s for %s on %s is expired. Renew it, then re-check.", pass.PK, pubkey, publicIP)
	default: // disconnected or an unknown future status
		out.Status = "pending"
		out.Message = fmt.Sprintf("Access pass %s for %s on %s has status %q, not connected. Resolve that, then re-check.",
			pass.PK, pubkey, publicIP, pass.Status)
	}
	return out
}
