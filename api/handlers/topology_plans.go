package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// PlanOpType is one of the five topology edit operations.
type PlanOpType string

const (
	OpAddDevice    PlanOpType = "add_device"
	OpRemoveDevice PlanOpType = "remove_device"
	OpAddLink      PlanOpType = "add_link"
	OpRemoveLink   PlanOpType = "remove_link"
	OpMoveLinkEnd  PlanOpType = "move_link_end"
)

// PlanChangeState is the execution state of a single change.
type PlanChangeState string

const (
	StatePending    PlanChangeState = "pending"
	StateDone       PlanChangeState = "done"
	StateSkipped    PlanChangeState = "skipped"
	StateSuperseded PlanChangeState = "superseded"
)

// PlanStatus is the lifecycle status of a plan.
type PlanStatus string

const (
	StatusDraft    PlanStatus = "draft"
	StatusApproved PlanStatus = "approved"
	StatusDone     PlanStatus = "done"
	StatusArchived PlanStatus = "archived"
)

// Plan mirrors a row of topology_plans (with its changes on load).
type Plan struct {
	ID                 uuid.UUID    `json:"id"`
	Name               string       `json:"name"`
	Description        string       `json:"description"`
	Status             PlanStatus   `json:"status"`
	Environment        string       `json:"environment"`
	BaselineAsOf       time.Time    `json:"baseline_as_of"`
	Version            int          `json:"version"`
	CreatedByAccountID *uuid.UUID   `json:"created_by_account_id,omitempty"`
	CreatedByEmail     string       `json:"created_by_email"`
	UpdatedByAccountID *uuid.UUID   `json:"updated_by_account_id,omitempty"`
	UpdatedByEmail     string       `json:"updated_by_email"`
	ForkedFromPlanID   *uuid.UUID   `json:"forked_from_plan_id,omitempty"`
	CreatedAt          time.Time    `json:"created_at"`
	UpdatedAt          time.Time    `json:"updated_at"`
	DeletedAt          *time.Time   `json:"deleted_at,omitempty"`
	Changes            []PlanChange `json:"changes"`
}

// PlanChange mirrors a row of topology_plan_changes.
type PlanChange struct {
	ID                 uuid.UUID       `json:"id"`
	PlanID             uuid.UUID       `json:"plan_id"`
	Seq                int             `json:"seq"`
	OpType             PlanOpType      `json:"op_type"`
	RefDevicePK        string          `json:"ref_device_pk"` // COALESCE(...,'') on scan; "" = absent
	RefLinkPK          string          `json:"ref_link_pk"`   // COALESCE(...,'')
	NewDevicePK        string          `json:"new_device_pk"` // COALESCE(...,''); move/add target when an EXISTING device
	LocalRef           string          `json:"local_ref"`     // COALESCE(...,'')
	Payload            json.RawMessage `json:"payload"`
	RefSnapshot        json.RawMessage `json:"ref_snapshot"`
	TargetDate         *string         `json:"target_date"`   // "YYYY-MM-DD" or nil
	AssigneeNote       string          `json:"assignee_note"` // COALESCE(...,'')
	State              PlanChangeState `json:"state"`
	Version            int             `json:"version"`
	CreatedByAccountID *uuid.UUID      `json:"created_by_account_id,omitempty"`
	CreatedByEmail     *string         `json:"created_by_email,omitempty"`
	CreatedAt          time.Time       `json:"created_at"`
	UpdatedAt          time.Time       `json:"updated_at"`
}

// PlanSummary is a lighter row for the shared-workspace list.
type PlanSummary struct {
	ID               uuid.UUID  `json:"id"`
	Name             string     `json:"name"`
	Description      *string    `json:"description,omitempty"`
	Status           PlanStatus `json:"status"`
	Environment      string     `json:"environment"`
	BaselineAsOf     time.Time  `json:"baseline_as_of"`
	Version          int        `json:"version"`
	CreatedByEmail   *string    `json:"created_by_email,omitempty"`
	UpdatedByEmail   *string    `json:"updated_by_email,omitempty"`
	ForkedFromPlanID *uuid.UUID `json:"forked_from_plan_id,omitempty"`
	ChangeCount      int        `json:"change_count"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

// CreatePlanRequest is the body for POST /api/topology/plans.
type CreatePlanRequest struct {
	Name        string  `json:"name"`
	Description *string `json:"description"`
}

// scanner is satisfied by both pgx.Row and pgx.Rows.
type scanner interface {
	Scan(dest ...any) error
}

// pgDB is satisfied by both *pgxpool.Pool and pgx.Tx.
type pgDB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

const planColumns = `id, name, COALESCE(description, '') AS description, status,
	environment, baseline_as_of, version,
	created_by_account_id, COALESCE(created_by_email, '') AS created_by_email,
	updated_by_account_id, COALESCE(updated_by_email, '') AS updated_by_email,
	forked_from_plan_id, created_at, updated_at, deleted_at`

func scanPlan(row scanner) (Plan, error) {
	var p Plan
	var status string
	err := row.Scan(&p.ID, &p.Name, &p.Description, &status, &p.Environment,
		&p.BaselineAsOf, &p.Version, &p.CreatedByAccountID, &p.CreatedByEmail,
		&p.UpdatedByAccountID, &p.UpdatedByEmail, &p.ForkedFromPlanID,
		&p.CreatedAt, &p.UpdatedAt, &p.DeletedAt)
	p.Status = PlanStatus(status)
	return p, err
}

// appendPlanEvent writes one append-only row to topology_plan_events. Pass a
// pgx.Tx to make the event atomic with its triggering write, or a.PgPool for
// standalone events. before/after may be nil (stored as SQL NULL).
func appendPlanEvent(ctx context.Context, db pgDB, planID uuid.UUID, changeID *uuid.UUID, actor *Account, action string, before, after any) error {
	var actorID *uuid.UUID
	var actorEmail *string
	if actor != nil {
		actorID = &actor.ID
		actorEmail = actor.Email
	}
	var beforeJSON, afterJSON []byte
	var err error
	if before != nil {
		if beforeJSON, err = json.Marshal(before); err != nil {
			return err
		}
	}
	if after != nil {
		if afterJSON, err = json.Marshal(after); err != nil {
			return err
		}
	}
	_, err = db.Exec(ctx, `
		INSERT INTO topology_plan_events
			(plan_id, change_id, actor_account_id, actor_email, action, before, after)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, planID, changeID, actorID, actorEmail, action, beforeJSON, afterJSON)
	return err
}

// CreatePlan creates a new plan in the current environment's shared workspace.
func (a *API) CreatePlan(w http.ResponseWriter, r *http.Request) {
	var req CreatePlanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	env := string(EnvFromContext(ctx))
	account := GetAccountFromContext(ctx)
	var accID *uuid.UUID
	var email *string
	if account != nil {
		accID = &account.ID
		email = account.Email
	}

	tx, err := a.PgPool.Begin(ctx)
	if err != nil {
		http.Error(w, internalError("Failed to begin transaction", err), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(ctx)

	plan, err := scanPlan(tx.QueryRow(ctx, `
		INSERT INTO topology_plans
			(name, description, status, environment,
			 created_by_account_id, created_by_email, updated_by_account_id, updated_by_email)
		VALUES ($1, $2, 'draft', $3, $4, $5, $4, $5)
		RETURNING `+planColumns,
		req.Name, req.Description, env, accID, email))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			http.Error(w, "A plan with this name already exists", http.StatusConflict)
			return
		}
		http.Error(w, internalError("Failed to create plan", err), http.StatusInternalServerError)
		return
	}

	if err := appendPlanEvent(ctx, tx, plan.ID, nil, account, "plan.create", nil, plan); err != nil {
		http.Error(w, internalError("Failed to record event", err), http.StatusInternalServerError)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		http.Error(w, internalError("Failed to commit", err), http.StatusInternalServerError)
		return
	}

	plan.Changes = []PlanChange{}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(plan)
}

// PlansListResponse is the body for GET /api/topology/plans.
type PlansListResponse struct {
	Plans []PlanSummary `json:"plans"`
}

// ListPlans returns the shared workspace for the current environment, excluding
// soft-deleted and archived plans, newest activity first. An optional ?status=
// query param opts back into a single status (e.g. ?status=archived).
func (a *API) ListPlans(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	env := string(EnvFromContext(ctx))

	// Default view hides archived plans; ?status= selects one status explicitly.
	statusFilter := strings.TrimSpace(r.URL.Query().Get("status"))
	switch statusFilter {
	case "", "draft", "approved", "done", "archived":
		// ok
	default:
		http.Error(w, "invalid status", http.StatusBadRequest)
		return
	}

	rows, err := a.PgPool.Query(ctx, `
		SELECT p.id, p.name, p.description, p.status, p.environment, p.baseline_as_of,
		       p.version, p.created_by_email, p.updated_by_email, p.forked_from_plan_id,
		       COUNT(c.id) AS change_count, p.created_at, p.updated_at
		FROM topology_plans p
		LEFT JOIN topology_plan_changes c ON c.plan_id = p.id
		WHERE p.environment = $1 AND p.deleted_at IS NULL
		  AND (
		    ($2 = '' AND p.status <> 'archived')
		    OR ($2 <> '' AND p.status::text = $2)
		  )
		GROUP BY p.id
		ORDER BY p.updated_at DESC, p.id ASC
	`, env, statusFilter)
	if err != nil {
		http.Error(w, internalError("Failed to list plans", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	plans := []PlanSummary{}
	for rows.Next() {
		var s PlanSummary
		var status string
		if err := rows.Scan(&s.ID, &s.Name, &s.Description, &status, &s.Environment,
			&s.BaselineAsOf, &s.Version, &s.CreatedByEmail, &s.UpdatedByEmail,
			&s.ForkedFromPlanID, &s.ChangeCount, &s.CreatedAt, &s.UpdatedAt); err != nil {
			http.Error(w, internalError("Failed to scan plan", err), http.StatusInternalServerError)
			return
		}
		s.Status = PlanStatus(status)
		plans = append(plans, s)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, internalError("Failed to iterate plans", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(PlansListResponse{Plans: plans})
}

const changeColumns = `id, plan_id, seq, op_type,
	COALESCE(ref_device_pk, '') AS ref_device_pk,
	COALESCE(ref_link_pk, '')   AS ref_link_pk,
	COALESCE(new_device_pk, '') AS new_device_pk,
	COALESCE(local_ref, '')     AS local_ref,
	payload, ref_snapshot, to_char(target_date, 'YYYY-MM-DD') AS target_date,
	COALESCE(assignee_note, '') AS assignee_note,
	state, version, created_by_account_id, created_by_email,
	created_at, updated_at`

func scanPlanChange(row scanner) (PlanChange, error) {
	var c PlanChange
	var opType, state string
	err := row.Scan(&c.ID, &c.PlanID, &c.Seq, &opType, &c.RefDevicePK, &c.RefLinkPK,
		&c.NewDevicePK, &c.LocalRef, &c.Payload, &c.RefSnapshot, &c.TargetDate,
		&c.AssigneeNote, &state, &c.Version, &c.CreatedByAccountID, &c.CreatedByEmail,
		&c.CreatedAt, &c.UpdatedAt)
	if err != nil {
		return c, err
	}
	c.OpType = PlanOpType(opType)
	c.State = PlanChangeState(state)
	return c, nil
}

// loadPlanWithChanges reads one non-deleted plan and its ordered change list.
// Returns pgx.ErrNoRows if the plan does not exist or is soft-deleted.
func loadPlanWithChanges(ctx context.Context, db pgDB, id uuid.UUID) (Plan, error) {
	plan, err := scanPlan(db.QueryRow(ctx,
		`SELECT `+planColumns+` FROM topology_plans WHERE id = $1 AND deleted_at IS NULL`, id))
	if err != nil {
		return plan, err
	}

	rows, err := db.Query(ctx,
		`SELECT `+changeColumns+` FROM topology_plan_changes WHERE plan_id = $1 ORDER BY seq ASC`, id)
	if err != nil {
		return plan, err
	}
	defer rows.Close()

	plan.Changes = []PlanChange{}
	for rows.Next() {
		c, err := scanPlanChange(rows)
		if err != nil {
			return plan, err
		}
		plan.Changes = append(plan.Changes, c)
	}
	return plan, rows.Err()
}

// UpdatePlanRequest is the body for PATCH /api/topology/plans/{id}. Version is
// the plan version the caller last saw; the write fails with 409 on mismatch.
type UpdatePlanRequest struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
	Status      *string `json:"status"`
	Version     int     `json:"version"`
}

func validPlanStatus(s string) bool {
	switch PlanStatus(s) {
	case StatusDraft, StatusApproved, StatusDone, StatusArchived:
		return true
	}
	return false
}

// UpdatePlan renames/re-describes/re-statuses a plan under optimistic concurrency.
func (a *API) UpdatePlan(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, "Invalid plan ID", http.StatusBadRequest)
		return
	}
	var req UpdatePlanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.Status != nil && !validPlanStatus(*req.Status) {
		http.Error(w, "invalid status", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	account := GetAccountFromContext(ctx)
	var accID *uuid.UUID
	var email *string
	if account != nil {
		accID = &account.ID
		email = account.Email
	}

	tx, err := a.PgPool.Begin(ctx)
	if err != nil {
		http.Error(w, internalError("Failed to begin transaction", err), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(ctx)

	plan, err := scanPlan(tx.QueryRow(ctx, `
		UPDATE topology_plans SET
			name        = COALESCE($3, name),
			description = COALESCE($4, description),
			status      = COALESCE($5::plan_status, status),
			version     = version + 1,
			updated_at  = NOW(),
			updated_by_account_id = $6,
			updated_by_email      = $7
		WHERE id = $1 AND version = $2 AND deleted_at IS NULL
		RETURNING `+planColumns,
		id, req.Version, req.Name, req.Description, req.Status, accID, email))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			var exists bool
			_ = a.PgPool.QueryRow(ctx,
				`SELECT EXISTS(SELECT 1 FROM topology_plans WHERE id=$1 AND deleted_at IS NULL)`, id).Scan(&exists)
			if exists {
				http.Error(w, "Plan was modified by someone else; reload and retry", http.StatusConflict)
				return
			}
			http.Error(w, "Plan not found", http.StatusNotFound)
			return
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			http.Error(w, "A plan with this name already exists", http.StatusConflict)
			return
		}
		http.Error(w, internalError("Failed to update plan", err), http.StatusInternalServerError)
		return
	}

	// Use a specific action so the activity feed distinguishes approvals.
	action := "plan.update"
	if req.Status != nil && *req.Status == string(StatusApproved) {
		action = "plan.approve"
	}
	if err := appendPlanEvent(ctx, tx, plan.ID, nil, account, action, nil, plan); err != nil {
		http.Error(w, internalError("Failed to record event", err), http.StatusInternalServerError)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		http.Error(w, internalError("Failed to commit", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(plan)
}

// DeletePlan soft-deletes a plan (sets deleted_at). Idempotent: a second call
// returns 404.
func (a *API) DeletePlan(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, "Invalid plan ID", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	account := GetAccountFromContext(ctx)

	tx, err := a.PgPool.Begin(ctx)
	if err != nil {
		http.Error(w, internalError("Failed to begin transaction", err), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx,
		`UPDATE topology_plans SET deleted_at = NOW() WHERE id = $1 AND deleted_at IS NULL`, id)
	if err != nil {
		http.Error(w, internalError("Failed to delete plan", err), http.StatusInternalServerError)
		return
	}
	if tag.RowsAffected() == 0 {
		http.Error(w, "Plan not found", http.StatusNotFound)
		return
	}

	if err := appendPlanEvent(ctx, tx, id, nil, account, "plan.delete", nil, nil); err != nil {
		http.Error(w, internalError("Failed to record event", err), http.StatusInternalServerError)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		http.Error(w, internalError("Failed to commit", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// DuplicatePlan deep-copies a plan into a new draft in the same environment.
// Change rows are copied with fresh UUIDs and their state reset to pending.
func (a *API) DuplicatePlan(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, "Invalid plan ID", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	account := GetAccountFromContext(ctx)
	var accID *uuid.UUID
	var email *string
	if account != nil {
		accID = &account.ID
		email = account.Email
	}

	tx, err := a.PgPool.Begin(ctx)
	if err != nil {
		http.Error(w, internalError("Failed to begin transaction", err), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(ctx)

	src, err := scanPlan(tx.QueryRow(ctx,
		`SELECT `+planColumns+` FROM topology_plans WHERE id = $1 AND deleted_at IS NULL`, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "Plan not found", http.StatusNotFound)
			return
		}
		http.Error(w, internalError("Failed to load plan", err), http.StatusInternalServerError)
		return
	}

	newID := uuid.New()
	newPlan, err := scanPlan(tx.QueryRow(ctx, `
		INSERT INTO topology_plans
			(id, name, description, status, environment, forked_from_plan_id,
			 created_by_account_id, created_by_email, updated_by_account_id, updated_by_email)
		VALUES ($1, $2, $3, 'draft', $4, $5, $6, $7, $6, $7)
		RETURNING `+planColumns,
		newID, src.Name+" (copy)", src.Description, src.Environment, src.ID, accID, email))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			http.Error(w, "A plan with this name already exists", http.StatusConflict)
			return
		}
		http.Error(w, internalError("Failed to duplicate plan", err), http.StatusInternalServerError)
		return
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO topology_plan_changes
			(id, plan_id, seq, op_type, ref_device_pk, ref_link_pk, new_device_pk, local_ref,
			 payload, ref_snapshot, target_date, assignee_note, state, version,
			 created_by_account_id, created_by_email)
		SELECT gen_random_uuid(), $1, seq, op_type, ref_device_pk, ref_link_pk, new_device_pk,
		       local_ref, payload, ref_snapshot, target_date, assignee_note,
		       'pending'::plan_change_state, 1, $2, $3
		FROM topology_plan_changes WHERE plan_id = $4
	`, newID, accID, email, src.ID); err != nil {
		http.Error(w, internalError("Failed to copy changes", err), http.StatusInternalServerError)
		return
	}

	if err := appendPlanEvent(ctx, tx, newID, nil, account, "plan.duplicate",
		map[string]any{"forked_from_plan_id": src.ID}, newPlan); err != nil {
		http.Error(w, internalError("Failed to record event", err), http.StatusInternalServerError)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		http.Error(w, internalError("Failed to commit", err), http.StatusInternalServerError)
		return
	}

	full, err := loadPlanWithChanges(ctx, a.PgPool, newID)
	if err != nil {
		http.Error(w, internalError("Failed to load duplicated plan", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(full)
}

// GetPlan loads a plan with its changes.
func (a *API) GetPlan(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, "Invalid plan ID", http.StatusBadRequest)
		return
	}

	plan, err := loadPlanWithChanges(r.Context(), a.PgPool, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "Plan not found", http.StatusNotFound)
			return
		}
		http.Error(w, internalError("Failed to load plan", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(plan)
}

// AddChangeRequest is the body for POST /api/topology/plans/{id}/changes.
type AddChangeRequest struct {
	OpType       PlanOpType       `json:"op_type"`
	RefDevicePK  *string          `json:"ref_device_pk"`
	RefLinkPK    *string          `json:"ref_link_pk"`
	NewDevicePK  *string          `json:"new_device_pk"`
	LocalRef     *string          `json:"local_ref"`
	Payload      json.RawMessage  `json:"payload"`
	RefSnapshot  json.RawMessage  `json:"ref_snapshot"`
	TargetDate   *string          `json:"target_date"`
	AssigneeNote *string          `json:"assignee_note"`
	State        *PlanChangeState `json:"state"`
}

func validOpType(op PlanOpType) bool {
	switch op {
	case OpAddDevice, OpRemoveDevice, OpAddLink, OpRemoveLink, OpMoveLinkEnd:
		return true
	}
	return false
}

func validChangeState(s PlanChangeState) bool {
	switch s {
	case StatePending, StateDone, StateSkipped, StateSuperseded:
		return true
	}
	return false
}

func nonEmpty(p *string) bool { return p != nil && *p != "" }

// validateChangeShape mirrors the DB CHECK so the API returns 400 (not 500) for
// a change missing its required anchor column, plus (for add_device) the
// payload-level shape the DB CHECK cannot express.
func validateChangeShape(op PlanOpType, refDevice, refLink, localRef *string, payload json.RawMessage) error {
	switch op {
	case OpAddLink:
		if !nonEmpty(localRef) {
			return fmt.Errorf("local_ref is required for %s", op)
		}
	case OpAddDevice:
		if !nonEmpty(localRef) {
			return fmt.Errorf("local_ref is required for %s", op)
		}
		return validateAddDevicePayload(payload)
	case OpRemoveDevice:
		if !nonEmpty(refDevice) {
			return fmt.Errorf("ref_device_pk is required for remove_device")
		}
	case OpRemoveLink, OpMoveLinkEnd:
		if !nonEmpty(refLink) {
			return fmt.Errorf("ref_link_pk is required for %s", op)
		}
	}
	return nil
}

// validateAddDevicePayload requires the device code, a contributor (an
// existing contributor_pk or a brand-new contributor_code), and a metro (an
// existing metro_pk or a fully-specified new_metro) in an add_device change's
// JSONB payload. This relaxes the previous "requires contributor_pk +
// metro_pk" so a plan can introduce a contributor or metro that doesn't exist
// onchain yet (SC canonical add_device shape).
func validateAddDevicePayload(payload json.RawMessage) error {
	var p plannerPayload
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &p); err != nil {
			return fmt.Errorf("invalid payload for add_device: %w", err)
		}
	}
	if strings.TrimSpace(p.Code) == "" {
		return fmt.Errorf("code is required for add_device")
	}
	if p.ContributorCode == "" && p.ContributorPK == "" {
		return fmt.Errorf("contributor_code or contributor_pk is required for add_device")
	}
	// Metro: an existing metro_pk, or a fully-specified new_metro. When a
	// new_metro is provided it must carry a code AND real coordinates -- a
	// missing lat/long decodes to 0 and would silently place the metro at the
	// null island (0,0), corrupting every downstream great-circle / metro-pair
	// computation, so reject it here (this function is the authoritative guard).
	if p.NewMetro != nil {
		if err := validateNewMetro(p.NewMetro); err != nil {
			return err
		}
	} else if p.MetroPK == "" {
		return fmt.Errorf("metro_pk or new_metro (with a code and coordinates) is required for add_device")
	}
	return nil
}

// validateNewMetro rejects an inline new_metro that lacks a code or real
// coordinates. Latitude/longitude of exactly (0,0) is treated as unset (a JSON
// payload that omits either field decodes to 0); a real metro is never at the
// null island. Out-of-range coordinates are also rejected.
func validateNewMetro(m *NewMetroPayload) error {
	if strings.TrimSpace(m.Code) == "" {
		return fmt.Errorf("new_metro.code is required for add_device")
	}
	if m.Latitude == 0 && m.Longitude == 0 {
		return fmt.Errorf("new_metro requires valid latitude/longitude (got 0,0)")
	}
	if m.Latitude < -90 || m.Latitude > 90 {
		return fmt.Errorf("new_metro.latitude out of range: %v", m.Latitude)
	}
	if m.Longitude < -180 || m.Longitude > 180 {
		return fmt.Errorf("new_metro.longitude out of range: %v", m.Longitude)
	}
	return nil
}

// addLinkEndpointRule is one add_link endpoint's resolved contributor + metro
// identity, used only to enforce the WAN/DZX rule below (defense-in-depth for
// the frontend's link-type.ts deriveLinkType). An empty ContributorKey or
// MetroKey means "unresolved" -- validateAddLinkEndpointsRule skips the check
// rather than blocking on an unknown endpoint.
type addLinkEndpointRule struct {
	ContributorKey string
	MetroKey       string
}

// resolveAddLinkEndpointRule resolves one add_link endpoint's contributor +
// metro identity for the WAN/DZX rule: an existing device pk resolves against
// the baseline; a sibling add_device change's local_ref resolves against that
// change's own payload (contributor_pk, falling back to contributor_code, and
// metro_pk or the synthesized new-metro key from new_metro.code). Mirrors
// resolveEndpoint (topology_plan_actionlist.go) and resolveAddDeviceMetro
// (planner_graph.go), combined and metro-aware for this rule.
func (b *baselineIndex) resolveAddLinkEndpointRule(devicePK, ref string) addLinkEndpointRule {
	if ref != "" {
		add, ok := b.addDeviceByRef[ref]
		if !ok {
			return addLinkEndpointRule{}
		}
		p, _ := decodePlanChangePayload(add)
		contrib := p.ContributorPK
		if contrib == "" {
			contrib = p.ContributorCode
		}
		metro := p.MetroPK
		if p.NewMetro != nil && p.NewMetro.Code != "" {
			metro = newMetroPK(p.NewMetro.Code)
		}
		return addLinkEndpointRule{ContributorKey: contrib, MetroKey: metro}
	}
	if d, ok := b.deviceByPK[devicePK]; ok {
		contrib := d.ContributorPK
		if contrib == "" {
			contrib = d.ContributorCode
		}
		return addLinkEndpointRule{ContributorKey: contrib, MetroKey: d.MetroPK}
	}
	return addLinkEndpointRule{}
}

// validateAddLinkEndpointsRule enforces the WAN/DZX contributor+metro rule
// (the frontend's link-type.ts deriveLinkType) for an add_link change's two
// resolved endpoints. It rejects ONLY the invalid combo -- different
// contributor AND different metro; WAN (same contributor, different metro),
// DZX (different contributor, same metro), and same-contributor-same-metro
// are all allowed here (the operator picks the type for that last case on the
// frontend). An endpoint that could not be resolved (empty ContributorKey or
// MetroKey) is skipped rather than blocked: the frontend already guards this
// case, and a backend that cannot resolve an endpoint must not block what may
// be a legitimate edit.
func validateAddLinkEndpointsRule(a, z addLinkEndpointRule) error {
	if a.ContributorKey == "" || a.MetroKey == "" || z.ContributorKey == "" || z.MetroKey == "" {
		return nil
	}
	sameContrib := a.ContributorKey == z.ContributorKey
	sameMetro := a.MetroKey == z.MetroKey
	if !sameContrib && !sameMetro {
		return fmt.Errorf("invalid link: a cross-contributor link must be within one metro (DZX); a cross-metro link must be owned by one contributor (WAN)")
	}
	return nil
}

// validateAddLinkRule loads the plan's baseline topology and existing changes
// and applies validateAddLinkEndpointsRule to this add_link payload's two
// endpoints. Defense-in-depth for PlannerMap.tsx's frontend block: a malformed
// or stale client must not be able to stage an invalid link. Any failure to
// load the plan or the baseline resolves nothing and skips the check, rather
// than 500ing a request that the rest of AddPlanChange will otherwise handle
// (including a plan-not-found 404) normally.
func (a *API) validateAddLinkRule(ctx context.Context, planID uuid.UUID, payload json.RawMessage) error {
	var p plannerPayload
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &p); err != nil {
			log.Printf("validateAddLinkRule: invalid add_link payload for plan %s, skipping rule check: %v", planID, err)
			return nil
		}
	}
	plan, err := loadPlanWithChanges(ctx, a.PgPool, planID)
	if err != nil {
		log.Printf("validateAddLinkRule: failed to load plan %s, skipping rule check: %v", planID, err)
		return nil
	}
	baseCtx := ContextWithEnv(ctx, DZEnv(plan.Environment))
	baseline, err := a.FetchTopologyData(baseCtx)
	if err != nil {
		log.Printf("validateAddLinkRule: FetchTopologyData failed for plan %s, skipping rule check: %v", planID, err)
		return nil
	}
	idx := newBaselineIndex(&baseline, plan.Changes)
	aEnd := idx.resolveAddLinkEndpointRule(p.SideADevicePK, p.SideARef)
	zEnd := idx.resolveAddLinkEndpointRule(p.SideZDevicePK, p.SideZRef)
	return validateAddLinkEndpointsRule(aEnd, zEnd)
}

// touchPlan records activity on a plan (updated_at / updated_by) without bumping
// its optimistic-concurrency version. Change edits touch; only plan-metadata
// PATCH bumps version.
func touchPlan(ctx context.Context, db pgDB, planID uuid.UUID, actor *Account) error {
	var accID *uuid.UUID
	var email *string
	if actor != nil {
		accID = &actor.ID
		email = actor.Email
	}
	_, err := db.Exec(ctx, `
		UPDATE topology_plans
		SET updated_at = NOW(), updated_by_account_id = $2, updated_by_email = $3
		WHERE id = $1 AND deleted_at IS NULL
	`, planID, accID, email)
	return err
}

// AddPlanChange appends a change to a plan, allocating the next seq in gaps of 10.
func (a *API) AddPlanChange(w http.ResponseWriter, r *http.Request) {
	planID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, "Invalid plan ID", http.StatusBadRequest)
		return
	}
	var req AddChangeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if !validOpType(req.OpType) {
		http.Error(w, "invalid op_type", http.StatusBadRequest)
		return
	}
	if req.State != nil && !validChangeState(*req.State) {
		http.Error(w, "invalid state", http.StatusBadRequest)
		return
	}
	if err := validateChangeShape(req.OpType, req.RefDevicePK, req.RefLinkPK, req.LocalRef, req.Payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.OpType == OpAddLink {
		if err := a.validateAddLinkRule(r.Context(), planID, req.Payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	payload := req.Payload
	if payload == nil {
		payload = json.RawMessage("{}")
	}
	refSnap := req.RefSnapshot
	if refSnap == nil {
		refSnap = json.RawMessage("{}")
	}
	state := StatePending
	if req.State != nil {
		state = *req.State
	}

	ctx := r.Context()
	account := GetAccountFromContext(ctx)
	var accID *uuid.UUID
	var email *string
	if account != nil {
		accID = &account.ID
		email = account.Email
	}

	tx, err := a.PgPool.Begin(ctx)
	if err != nil {
		http.Error(w, internalError("Failed to begin transaction", err), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(ctx)

	var exists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM topology_plans WHERE id=$1 AND deleted_at IS NULL)`, planID).Scan(&exists); err != nil {
		http.Error(w, internalError("Failed to check plan", err), http.StatusInternalServerError)
		return
	}
	if !exists {
		http.Error(w, "Plan not found", http.StatusNotFound)
		return
	}

	var nextSeq int
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(seq), 0) + 10 FROM topology_plan_changes WHERE plan_id = $1`, planID).Scan(&nextSeq); err != nil {
		http.Error(w, internalError("Failed to compute seq", err), http.StatusInternalServerError)
		return
	}

	change, err := scanPlanChange(tx.QueryRow(ctx, `
		INSERT INTO topology_plan_changes
			(plan_id, seq, op_type, ref_device_pk, ref_link_pk, new_device_pk, local_ref,
			 payload, ref_snapshot, target_date, assignee_note, state,
			 created_by_account_id, created_by_email)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::date, $11, $12, $13, $14)
		RETURNING `+changeColumns,
		planID, nextSeq, string(req.OpType), req.RefDevicePK, req.RefLinkPK, req.NewDevicePK,
		req.LocalRef, payload, refSnap, req.TargetDate, req.AssigneeNote, string(state), accID, email))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23514" {
			http.Error(w, "change shape invalid for op_type", http.StatusBadRequest)
			return
		}
		http.Error(w, internalError("Failed to add change", err), http.StatusInternalServerError)
		return
	}

	if err := touchPlan(ctx, tx, planID, account); err != nil {
		http.Error(w, internalError("Failed to touch plan", err), http.StatusInternalServerError)
		return
	}
	if err := appendPlanEvent(ctx, tx, planID, &change.ID, account, "change.add", nil, change); err != nil {
		http.Error(w, internalError("Failed to record event", err), http.StatusInternalServerError)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		http.Error(w, internalError("Failed to commit", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(change)
}

// UpdateChangeRequest is the body for PATCH .../changes/{changeId}. Nil fields
// are left unchanged; Version is the per-change CAS token.
type UpdateChangeRequest struct {
	Seq          *int             `json:"seq"`
	RefDevicePK  *string          `json:"ref_device_pk"`
	RefLinkPK    *string          `json:"ref_link_pk"`
	NewDevicePK  *string          `json:"new_device_pk"`
	Payload      json.RawMessage  `json:"payload"`
	RefSnapshot  json.RawMessage  `json:"ref_snapshot"`
	TargetDate   *string          `json:"target_date"`
	AssigneeNote *string          `json:"assignee_note"`
	State        *PlanChangeState `json:"state"`
	Version      int              `json:"version"`
}

// UpdatePlanChange edits one change under per-change optimistic concurrency.
func (a *API) UpdatePlanChange(w http.ResponseWriter, r *http.Request) {
	planID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, "Invalid plan ID", http.StatusBadRequest)
		return
	}
	changeID, err := uuid.Parse(chi.URLParam(r, "changeId"))
	if err != nil {
		http.Error(w, "Invalid change ID", http.StatusBadRequest)
		return
	}
	var req UpdateChangeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.State != nil && !validChangeState(*req.State) {
		http.Error(w, "invalid state", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	account := GetAccountFromContext(ctx)

	var statePtr *string
	if req.State != nil {
		s := string(*req.State)
		statePtr = &s
	}

	tx, err := a.PgPool.Begin(ctx)
	if err != nil {
		http.Error(w, internalError("Failed to begin transaction", err), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(ctx)

	// Load the current change so op-shape can be validated against the FULL
	// post-patch shape, not just the patched subset. op_type is immutable
	// (absent from UpdateChangeRequest), so validate against the existing type.
	// The DB CHECK only tests IS NOT NULL, so a patch that sets a required anchor
	// to "" would slip past it; validateChangeShape rejects the empty string.
	existing, err := scanPlanChange(tx.QueryRow(ctx,
		`SELECT `+changeColumns+` FROM topology_plan_changes WHERE id = $1 AND plan_id = $2`,
		changeID, planID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "Change not found", http.StatusNotFound)
			return
		}
		http.Error(w, internalError("Failed to load change", err), http.StatusInternalServerError)
		return
	}

	mergedDevice := existing.RefDevicePK
	if req.RefDevicePK != nil {
		mergedDevice = *req.RefDevicePK
	}
	mergedLink := existing.RefLinkPK
	if req.RefLinkPK != nil {
		mergedLink = *req.RefLinkPK
	}
	mergedLocal := existing.LocalRef // local_ref is not editable via PATCH
	mergedPayload := existing.Payload
	if req.Payload != nil {
		mergedPayload = req.Payload
	}
	if err := validateChangeShape(existing.OpType, &mergedDevice, &mergedLink, &mergedLocal, mergedPayload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Same WAN/DZX contributor+metro rule as AddPlanChange, applied to the
	// MERGED payload -- without this, a PATCH could edit a valid add_link's
	// payload into a cross-contributor + cross-metro pair with no server
	// rejection. Only runs when op_type is add_link AND the request actually
	// touches the payload.
	if existing.OpType == OpAddLink && req.Payload != nil {
		if err := a.validateAddLinkRule(ctx, planID, mergedPayload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	change, err := scanPlanChange(tx.QueryRow(ctx, `
		UPDATE topology_plan_changes SET
			seq           = COALESCE($3, seq),
			ref_device_pk = COALESCE($4, ref_device_pk),
			ref_link_pk   = COALESCE($5, ref_link_pk),
			new_device_pk = COALESCE($6, new_device_pk),
			payload       = COALESCE($7::jsonb, payload),
			ref_snapshot  = COALESCE($8::jsonb, ref_snapshot),
			target_date   = COALESCE($9::date, target_date),
			assignee_note = COALESCE($10, assignee_note),
			state         = COALESCE($11::plan_change_state, state),
			version       = version + 1,
			updated_at    = NOW()
		WHERE id = $1 AND plan_id = $2 AND version = $12
		RETURNING `+changeColumns,
		changeID, planID, req.Seq, req.RefDevicePK, req.RefLinkPK, req.NewDevicePK,
		req.Payload, req.RefSnapshot, req.TargetDate, req.AssigneeNote, statePtr, req.Version))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			var exists bool
			_ = a.PgPool.QueryRow(ctx,
				`SELECT EXISTS(SELECT 1 FROM topology_plan_changes WHERE id=$1 AND plan_id=$2)`,
				changeID, planID).Scan(&exists)
			if exists {
				http.Error(w, "Change was modified by someone else; reload and retry", http.StatusConflict)
				return
			}
			http.Error(w, "Change not found", http.StatusNotFound)
			return
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23514" {
			http.Error(w, "change shape invalid for op_type", http.StatusBadRequest)
			return
		}
		http.Error(w, internalError("Failed to update change", err), http.StatusInternalServerError)
		return
	}

	if err := touchPlan(ctx, tx, planID, account); err != nil {
		http.Error(w, internalError("Failed to touch plan", err), http.StatusInternalServerError)
		return
	}
	// A seq change is a reorder; emit the specific action for the activity feed.
	action := "change.update"
	if req.Seq != nil {
		action = "change.reorder"
	}
	if err := appendPlanEvent(ctx, tx, planID, &change.ID, account, action, nil, change); err != nil {
		http.Error(w, internalError("Failed to record event", err), http.StatusInternalServerError)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		http.Error(w, internalError("Failed to commit", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(change)
}

// ReorderPlanChangesRequest is the body for POST .../changes/reorder.
// OrderedIDs must contain exactly the plan's current change ids, in the
// desired final order.
type ReorderPlanChangesRequest struct {
	OrderedIDs []uuid.UUID `json:"ordered_ids"`
}

// Final seq allocation after a reorder: 10, 20, 30, ... (matches AddPlanChange's
// gap-of-10 convention so a later single AddPlanChange still has room to append).
const (
	reorderSeqStart = 10
	reorderSeqStep  = 10
	// reorderSeqOffset must exceed any plausible seq value in a single plan so
	// phase 1 (vacate) can never land on a value phase 2 (final assignment)
	// will also use.
	reorderSeqOffset = 1000000
)

// ReorderPlanChanges rewrites every change's seq in one transaction, in the
// exact order given by ordered_ids.
//
// The client's previous approach — PATCHing each change's seq one at a time —
// collides with the non-deferrable UNIQUE(plan_id, seq) constraint (migration
// 00018) whenever a change's target seq is still held by another row that
// hasn't been moved out of the way yet (e.g. swapping positions 1 and 2 tries
// to set #1's seq to #2's current seq before #2 has moved), producing a 500
// from UpdatePlanChange (which only handles 23514/ErrNoRows).
//
// A single-transaction, two-phase reorder avoids the transient collision:
// phase 1 offsets every one of the plan's changes by reorderSeqOffset (a
// shift by a constant preserves distinctness, so this can never collide with
// itself or with any not-yet-processed row); phase 2 then assigns the final
// seq (10, 20, 30, ...) by position, which cannot collide with any row still
// sitting in the offset range as long as the plan has far fewer changes than
// reorderSeqOffset/reorderSeqStep.
func (a *API) ReorderPlanChanges(w http.ResponseWriter, r *http.Request) {
	planID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, "Invalid plan ID", http.StatusBadRequest)
		return
	}
	var req ReorderPlanChangesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	account := GetAccountFromContext(ctx)

	tx, err := a.PgPool.Begin(ctx)
	if err != nil {
		http.Error(w, internalError("Failed to begin transaction", err), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(ctx)

	var exists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM topology_plans WHERE id=$1 AND deleted_at IS NULL)`, planID).Scan(&exists); err != nil {
		http.Error(w, internalError("Failed to check plan", err), http.StatusInternalServerError)
		return
	}
	if !exists {
		http.Error(w, "Plan not found", http.StatusNotFound)
		return
	}

	rows, err := tx.Query(ctx, `SELECT id FROM topology_plan_changes WHERE plan_id = $1`, planID)
	if err != nil {
		http.Error(w, internalError("Failed to load changes", err), http.StatusInternalServerError)
		return
	}
	existingIDs := map[uuid.UUID]bool{}
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			http.Error(w, internalError("Failed to scan change", err), http.StatusInternalServerError)
			return
		}
		existingIDs[id] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		http.Error(w, internalError("Failed to iterate changes", err), http.StatusInternalServerError)
		return
	}
	rows.Close()

	// ordered_ids must be exactly the plan's current change ids: same members,
	// no missing or extra, no duplicates.
	if len(req.OrderedIDs) != len(existingIDs) {
		http.Error(w, "ordered_ids must contain exactly the plan's current change ids", http.StatusBadRequest)
		return
	}
	seen := make(map[uuid.UUID]bool, len(req.OrderedIDs))
	for _, id := range req.OrderedIDs {
		if seen[id] || !existingIDs[id] {
			http.Error(w, "ordered_ids must contain exactly the plan's current change ids", http.StatusBadRequest)
			return
		}
		seen[id] = true
	}

	// Phase 1: vacate the whole seq range for this plan.
	if _, err := tx.Exec(ctx,
		`UPDATE topology_plan_changes SET seq = seq + $2 WHERE plan_id = $1`,
		planID, reorderSeqOffset); err != nil {
		http.Error(w, internalError("Failed to vacate seq range", err), http.StatusInternalServerError)
		return
	}

	// Phase 2: assign the final seq by position.
	for i, id := range req.OrderedIDs {
		seq := reorderSeqStart + i*reorderSeqStep
		if _, err := tx.Exec(ctx,
			`UPDATE topology_plan_changes SET seq = $3 WHERE id = $1 AND plan_id = $2`,
			id, planID, seq); err != nil {
			http.Error(w, internalError("Failed to set change seq", err), http.StatusInternalServerError)
			return
		}
	}

	// Touch the plan. This is a structural edit to the plan's change ordering
	// (unlike a single-change edit, which uses touchPlan and does not bump
	// version), so it also bumps the plan's optimistic-concurrency version.
	var accID *uuid.UUID
	var email *string
	if account != nil {
		accID = &account.ID
		email = account.Email
	}
	if _, err := tx.Exec(ctx, `
		UPDATE topology_plans
		SET updated_at = NOW(), version = version + 1, updated_by_account_id = $2, updated_by_email = $3
		WHERE id = $1 AND deleted_at IS NULL
	`, planID, accID, email); err != nil {
		http.Error(w, internalError("Failed to touch plan", err), http.StatusInternalServerError)
		return
	}

	if err := appendPlanEvent(ctx, tx, planID, nil, account, "change.reorder",
		nil, map[string]any{"ordered_ids": req.OrderedIDs}); err != nil {
		http.Error(w, internalError("Failed to record event", err), http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(ctx); err != nil {
		http.Error(w, internalError("Failed to commit", err), http.StatusInternalServerError)
		return
	}

	plan, err := loadPlanWithChanges(ctx, a.PgPool, planID)
	if err != nil {
		http.Error(w, internalError("Failed to load reordered plan", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(plan)
}

// DeletePlanChange hard-deletes a change from a plan's mutable draft.
func (a *API) DeletePlanChange(w http.ResponseWriter, r *http.Request) {
	planID, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, "Invalid plan ID", http.StatusBadRequest)
		return
	}
	changeID, err := uuid.Parse(chi.URLParam(r, "changeId"))
	if err != nil {
		http.Error(w, "Invalid change ID", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	account := GetAccountFromContext(ctx)

	tx, err := a.PgPool.Begin(ctx)
	if err != nil {
		http.Error(w, internalError("Failed to begin transaction", err), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx,
		`DELETE FROM topology_plan_changes WHERE id = $1 AND plan_id = $2`, changeID, planID)
	if err != nil {
		http.Error(w, internalError("Failed to delete change", err), http.StatusInternalServerError)
		return
	}
	if tag.RowsAffected() == 0 {
		http.Error(w, "Change not found", http.StatusNotFound)
		return
	}

	if err := touchPlan(ctx, tx, planID, account); err != nil {
		http.Error(w, internalError("Failed to touch plan", err), http.StatusInternalServerError)
		return
	}
	if err := appendPlanEvent(ctx, tx, planID, &changeID, account, "change.delete", nil, nil); err != nil {
		http.Error(w, internalError("Failed to record event", err), http.StatusInternalServerError)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		http.Error(w, internalError("Failed to commit", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
