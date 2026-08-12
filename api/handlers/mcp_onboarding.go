package handlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/malbeclabs/lake/api/handlers/runbooks"
	"github.com/malbeclabs/lake/api/metrics"
	"github.com/malbeclabs/lake/utils/pkg/docsfetch"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Onboarding tools live on the single DoubleZero MCP (this server). Runbook
// markdown stays in public malbeclabs/docs; we fetch it. A small embed is only
// a fallback for mockup pages not published yet.
//
//   - get_onboarding_runbook: entrypoint on "walk me through connecting…"
//   - check_edge_access: reads dz_access_passes_current (owner pubkey + receiving IP)

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
const runbookIndexPage = "runbooks"

type runbookRef struct {
	Service string
	Page    string
	Title   string
}

// runbookListItem matches:
//
//	- `service` — [Title](page.md)
//	- [Title](page.md)
var runbookListItem = regexp.MustCompile("(?m)^[\\t ]*[-*][\\t ]+(?:`([a-zA-Z0-9][a-zA-Z0-9\\-]*)`[\\t ]*[—–-]+[\\t ]+)?\\[([^\\]]+)\\]\\(([^)]+)\\)")

var fencedBlock = regexp.MustCompile("(?s)```.*?```")

func loadRunbookCatalog(ctx context.Context) ([]runbookRef, error) {
	content, _, err := DocsSource.Read(ctx, runbookIndexPage)
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

func registerGetOnboardingRunbookTool(server *mcp.Server) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_onboarding_runbook",
		Title:       "Get Onboarding Runbook",
		Description: "Start a guided onboarding walkthrough for connecting to a DoubleZero service. Call this whenever a user asks to be walked through, set up, connect to, or onboard onto a DoubleZero service. Returns a step-by-step runbook from DoubleZero docs plus instructions for how to guide the user through it. Omit 'service' to list available runbooks.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, func(ctx context.Context, req *mcp.CallToolRequest, input GetOnboardingRunbookInput) (*mcp.CallToolResult, GetOnboardingRunbookOutput, error) {
		catalog, err := loadRunbookCatalog(ctx)
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

		content, source, err := loadRunbook(ctx, page)
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

func loadRunbook(ctx context.Context, page string) (content, source string, err error) {
	content, source, err = DocsSource.Read(ctx, page)
	if err == nil {
		return content, source, nil
	}
	// Fallback: mockup embed (e.g. dz-edge-subscriber not yet in docs).
	embedded, embErr := runbooks.RunbooksFS.ReadFile(page + ".md")
	if embErr != nil {
		return "", "", err
	}
	return string(embedded), "embed:" + page + ".md", nil
}

// CheckEdgeAccessInput is the input for the check_edge_access tool.
type CheckEdgeAccessInput struct {
	Pubkey   string `json:"pubkey" jsonschema:"The DoubleZero identity pubkey (output of 'doublezero address')"`
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

func (a *API) registerCheckEdgeAccessTool(server *mcp.Server, r *http.Request) {
	env := EnvFromContext(r.Context())
	clientIP := GetIPFromRequest(r)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "check_edge_access",
		Title:       "Check Edge Access",
		Description: "Check whether a DoubleZero identity pubkey is authorized for a given receiving IP by looking up access passes. Pass the pubkey from 'doublezero address' and the public IPv4 the user will receive on. Returns status 'active' if a non-expired pass covers that IP (exact match or 0.0.0.0), otherwise 'pending'.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, func(ctx context.Context, req *mcp.CallToolRequest, input CheckEdgeAccessInput) (*mcp.CallToolResult, CheckEdgeAccessOutput, error) {
		if errMsg := CheckRateLimit(QueryRateLimiter, clientIP); errMsg != "" {
			return nil, CheckEdgeAccessOutput{}, errors.New(errMsg)
		}
		ctx = ContextWithEnv(ctx, env)

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

func (a *API) accessPassDB(ctx context.Context) driver.Conn {
	if conn, ok := a.EnvDBs[string(EnvFromContext(ctx))]; ok && conn != nil {
		return conn
	}
	if a.PublicQueryDB != nil {
		return a.PublicQueryDB
	}
	return a.DB
}

func (a *API) lookupEdgeAccessPass(ctx context.Context, pubkey, publicIP string) (*edgeAccessPass, error) {
	db := a.accessPassDB(ctx)
	if db == nil {
		return nil, errors.New("clickhouse not configured")
	}

	queryCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	start := time.Now()
	var pass edgeAccessPass
	err := db.QueryRow(queryCtx, `
		SELECT pk, client_ip, status, type_tag
		FROM dz_access_passes_current
		WHERE owner_pubkey = ?
		  AND (client_ip = ? OR client_ip = '0.0.0.0')
		ORDER BY
		  if(status = 'expired', 1, 0) ASC,
		  if(client_ip = ?, 0, 1) ASC
		LIMIT 1
	`, pubkey, publicIP, publicIP).Scan(&pass.PK, &pass.ClientIP, &pass.Status, &pass.TypeTag)
	duration := time.Since(start)
	metrics.RecordClickHouseQuery("mcp_edge_access", duration, err)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("access pass lookup failed: %w", err)
	}
	if pass.PK == "" {
		return nil, nil
	}
	return &pass, nil
}

func classifyEdgeAccess(pubkey, publicIP string, pass *edgeAccessPass) CheckEdgeAccessOutput {
	if pass == nil {
		return CheckEdgeAccessOutput{
			Status: "pending",
			Message: fmt.Sprintf("%s has no access pass for %s (or 0.0.0.0). Issue a pass for this identity+IP and recharge credits, then re-check.",
				pubkey, publicIP),
		}
	}
	out := CheckEdgeAccessOutput{
		PassPK:   pass.PK,
		ClientIP: pass.ClientIP,
		TypeTag:  pass.TypeTag,
	}
	if strings.EqualFold(pass.Status, "expired") {
		out.Status = "pending"
		out.Message = fmt.Sprintf("Access pass %s for %s on %s is expired. Renew it, then re-check.", pass.PK, pubkey, publicIP)
		return out
	}
	out.Status = "active"
	out.Message = fmt.Sprintf("%s is authorized for %s (access pass %s, %s). Proceed to subscribe.",
		pubkey, publicIP, pass.PK, pass.Status)
	return out
}
