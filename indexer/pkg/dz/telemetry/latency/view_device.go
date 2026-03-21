package dztelemlatency

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
	solanarpc "github.com/gagliardetto/solana-go/rpc"
	"github.com/malbeclabs/doublezero/smartcontract/sdk/go/telemetry"
	dzsvc "github.com/malbeclabs/lake/indexer/pkg/dz/serviceability"
)

type DeviceLinkLatencySample struct {
	OriginDevicePK  string
	TargetDevicePK  string
	LinkPK          string
	Epoch           uint64
	SampleIndex     int
	Time            time.Time
	RTTMicroseconds uint32
}

func (v *View) refreshDeviceLinkTelemetrySamples(ctx context.Context, devices []dzsvc.Device, links []dzsvc.Link) error {
	// Get current epoch
	epochInfo, err := v.cfg.EpochRPC.GetEpochInfo(ctx, solanarpc.CommitmentFinalized)
	if err != nil {
		return fmt.Errorf("failed to get epoch info: %w", err)
	}
	currentEpoch := epochInfo.Epoch

	// Fetch samples for current epoch and previous epoch
	epochsToFetch := []uint64{currentEpoch}
	if currentEpoch > 0 {
		epochsToFetch = append(epochsToFetch, currentEpoch-1)
	}

	// Build device map for lookup
	devicesByPK := make(map[string]dzsvc.Device)
	for _, d := range devices {
		devicesByPK[d.PK] = d
	}

	// Get existing max sample_index for each origin_device_pk+target_device_pk+link_pk+epoch to determine what's new
	existingMaxIndices, err := v.store.GetExistingMaxSampleIndices()
	if err != nil {
		return fmt.Errorf("failed to get existing max indices: %w", err)
	}

	var allSamples []DeviceLinkLatencySample
	var allHeaders []DeviceLinkLatencySampleHeader
	var samplesMu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, v.cfg.MaxConcurrency)
	linksProcessed := 0

	for _, link := range links {
		deviceA, okA := devicesByPK[link.SideAPK]
		deviceZ, okZ := devicesByPK[link.SideZPK]
		if !okA || !okZ {
			continue
		}

		// Process both directions: A -> Z and Z -> A
		for _, direction := range []struct {
			originPK, targetPK string
		}{
			{deviceA.PK, deviceZ.PK},
			{deviceZ.PK, deviceA.PK},
		} {
			// Check for context cancellation before starting new goroutines
			select {
			case <-ctx.Done():
				goto done
			default:
			}

			linksProcessed++
			originPK, err := solana.PublicKeyFromBase58(direction.originPK)
			if err != nil {
				continue
			}
			targetPK, err := solana.PublicKeyFromBase58(direction.targetPK)
			if err != nil {
				continue
			}
			linkPK, err := solana.PublicKeyFromBase58(link.PK)
			if err != nil {
				continue
			}

			wg.Add(1)
			// Try to acquire semaphore with context cancellation support
			select {
			case <-ctx.Done():
				wg.Done()
				goto done
			case sem <- struct{}{}:
				go func(originDevicePK, targetDevicePK, linkPKStr string, originPK, targetPK, linkPK solana.PublicKey) {
					defer wg.Done()
					defer func() { <-sem }() // Release semaphore

					linkSamples := make([]DeviceLinkLatencySample, 0, 128)
					var linkHeaders []DeviceLinkLatencySampleHeader

					for _, epoch := range epochsToFetch {
						// Check for context cancellation before each RPC call
						select {
						case <-ctx.Done():
							return
						default:
						}

						// Check what's already in the database for this origin_device_pk+target_device_pk+link_pk+epoch
						key := fmt.Sprintf("%s:%s:%s:%d", originDevicePK, targetDevicePK, linkPKStr, epoch)
						existingMaxIdx := -1
						if maxIdx, ok := existingMaxIndices[key]; ok {
							existingMaxIdx = maxIdx
						}

						hdr, startIdx, tail, err := v.cfg.TelemetryRPC.GetDeviceLatencySamplesTail(ctx, originPK, targetPK, linkPK, epoch, existingMaxIdx)
						if err != nil {
							if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
								return
							}
							if errors.Is(err, telemetry.ErrAccountNotFound) {
								continue
							}
							continue
						}
						if hdr == nil {
							continue
						}

						// Record the on-chain header for this circuit/epoch
						latestIdx := startIdx + len(tail) - 1
						if latestIdx < 0 {
							latestIdx = 0
						}
						linkHeaders = append(linkHeaders, DeviceLinkLatencySampleHeader{
							OriginDevicePK:     originDevicePK,
							TargetDevicePK:     targetDevicePK,
							LinkPK:             linkPKStr,
							Epoch:              epoch,
							StartTimestampUs:   hdr.StartTimestampMicroseconds,
							SamplingIntervalUs: hdr.SamplingIntervalMicroseconds,
							LatestSampleIndex:  latestIdx,
						})

						if len(tail) > 0 {
							step := hdr.SamplingIntervalMicroseconds
							baseTs := hdr.StartTimestampMicroseconds + uint64(startIdx)*step
							for j, rtt := range tail {
								i := startIdx + j
								ts := baseTs + uint64(j)*step
								// Convert microseconds since Unix epoch to time.Time
								sampleTime := time.Unix(int64(ts)/1_000_000, (int64(ts)%1_000_000)*1000)
								linkSamples = append(linkSamples, DeviceLinkLatencySample{
									OriginDevicePK:  originDevicePK,
									TargetDevicePK:  targetDevicePK,
									LinkPK:          linkPKStr,
									Epoch:           epoch,
									SampleIndex:     i,
									Time:            sampleTime,
									RTTMicroseconds: rtt,
								})
							}
						}
					}

					// Append samples and headers to shared slices
					samplesMu.Lock()
					if len(linkSamples) > 0 {
						allSamples = append(allSamples, linkSamples...)
					}
					if len(linkHeaders) > 0 {
						allHeaders = append(allHeaders, linkHeaders...)
					}
					samplesMu.Unlock()
				}(direction.originPK, direction.targetPK, link.PK, originPK, targetPK, linkPK)
			}
		}
	}

done:
	wg.Wait()

	// Append new samples to table (instead of replacing)
	if len(allSamples) > 0 {
		if err := v.store.AppendDeviceLinkLatencySamples(ctx, allSamples); err != nil {
			return fmt.Errorf("failed to append latency samples: %w", err)
		}
	}
	if len(allHeaders) > 0 {
		if err := v.store.AppendDeviceLinkLatencySampleHeaders(ctx, allHeaders); err != nil {
			return fmt.Errorf("failed to append latency sample headers: %w", err)
		}
	}
	if len(allSamples) > 0 || len(allHeaders) > 0 {
		v.log.Debug("telemetry/device-link: sample refresh completed", "links", linksProcessed, "samples", len(allSamples), "headers", len(allHeaders))
	}
	return nil
}
