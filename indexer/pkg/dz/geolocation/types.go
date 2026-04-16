package geolocation

import (
	"context"
	"crypto/sha256"
	"fmt"

	geolocsdk "github.com/malbeclabs/doublezero/sdk/geolocation/go"
)

type Probe struct {
	PK                 string
	Owner              string
	ExchangePK         string
	PublicIP           string
	LocationOffsetPort uint16
	MetricsPublisherPK string
	ReferenceCount     uint32
	Code               string
	ParentDevices      []string
	TargetUpdateCount  uint32
}

type User struct {
	PK                   string
	Owner                string
	Code                 string
	TokenAccount         string
	PaymentStatus        string
	Status               string
	TargetCount          uint32
	BillingRate          uint64
	LastDeductionDzEpoch uint64
}

type Target struct {
	GeolocUserPK       string
	ProbePK            string
	TargetType         string
	IP                 string
	LocationOffsetPort uint16
	TargetPK           string
}

// EntityID returns a stable composite key for a target (targets don't have
// their own on-chain account, so we derive an ID from their fields).
func (t Target) EntityID() string {
	h := sha256.New()
	h.Write([]byte(t.GeolocUserPK))
	h.Write([]byte{0})
	h.Write([]byte(t.ProbePK))
	h.Write([]byte{0})
	h.Write([]byte(t.TargetType))
	h.Write([]byte{0})
	h.Write([]byte(t.IP))
	h.Write([]byte{0})
	h.Write([]byte{byte(t.LocationOffsetPort >> 8), byte(t.LocationOffsetPort)})
	h.Write([]byte{0})
	h.Write([]byte(t.TargetPK))
	return fmt.Sprintf("%x", h.Sum(nil))[:32]
}

type GeolocationRPC interface {
	GetGeoProbes(ctx context.Context) ([]geolocsdk.KeyedGeoProbe, error)
	GetGeolocationUsers(ctx context.Context) ([]geolocsdk.KeyedGeolocationUser, error)
}
