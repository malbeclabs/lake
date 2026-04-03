package dzshreds

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"unsafe"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	shreds "github.com/malbeclabs/doublezero/sdk/shreds/go"
)

// allProgramAccounts holds the result of a single getProgramAccounts call,
// split by discriminator into typed slices.
type allProgramAccounts struct {
	ClientSeats      []shreds.KeyedClientSeat
	PaymentEscrows   []shreds.KeyedPaymentEscrow
	MetroHistories   []shreds.KeyedMetroHistory
	DeviceHistories  []shreds.KeyedDeviceHistory
	ValidatorRewards []shreds.KeyedValidatorClientRewards
}

// fetchAllProgramAccounts performs a single getProgramAccounts RPC call for the
// shreds program and splits the results by account discriminator. This replaces
// 5 individual getProgramAccounts calls (one per account type) with a single
// call, reducing RPC usage and avoiding rate limits.
func fetchAllProgramAccounts(ctx context.Context, rpcClient ShredsRawRPC, programID solana.PublicKey) (*allProgramAccounts, error) {
	accounts, err := rpcClient.GetProgramAccountsWithOpts(ctx, programID, &rpc.GetProgramAccountsOpts{})
	if err != nil {
		return nil, fmt.Errorf("fetching program accounts: %w", err)
	}

	result := &allProgramAccounts{}
	for _, acct := range accounts {
		data := acct.Account.Data.GetBinary()
		if len(data) < 8 {
			continue
		}

		var disc [8]byte
		copy(disc[:], data[:8])

		switch disc {
		case shreds.DiscriminatorClientSeat:
			item, err := deserializeAccount[shreds.ClientSeat](data, disc)
			if err != nil {
				return nil, fmt.Errorf("deserializing client seat %s: %w", acct.Pubkey, err)
			}
			result.ClientSeats = append(result.ClientSeats, shreds.KeyedClientSeat{
				Pubkey:     acct.Pubkey,
				ClientSeat: *item,
			})
		case shreds.DiscriminatorPaymentEscrow:
			item, err := deserializeAccount[shreds.PaymentEscrow](data, disc)
			if err != nil {
				return nil, fmt.Errorf("deserializing payment escrow %s: %w", acct.Pubkey, err)
			}
			result.PaymentEscrows = append(result.PaymentEscrows, shreds.KeyedPaymentEscrow{
				Pubkey:        acct.Pubkey,
				PaymentEscrow: *item,
			})
		case shreds.DiscriminatorMetroHistory:
			item, err := deserializeAccount[shreds.MetroHistory](data, disc)
			if err != nil {
				return nil, fmt.Errorf("deserializing metro history %s: %w", acct.Pubkey, err)
			}
			result.MetroHistories = append(result.MetroHistories, shreds.KeyedMetroHistory{
				Pubkey:       acct.Pubkey,
				MetroHistory: *item,
			})
		case shreds.DiscriminatorDeviceHistory:
			item, err := deserializeAccount[shreds.DeviceHistory](data, disc)
			if err != nil {
				return nil, fmt.Errorf("deserializing device history %s: %w", acct.Pubkey, err)
			}
			result.DeviceHistories = append(result.DeviceHistories, shreds.KeyedDeviceHistory{
				Pubkey:        acct.Pubkey,
				DeviceHistory: *item,
			})
		case shreds.DiscriminatorValidatorClientRewards:
			item, err := deserializeAccount[shreds.ValidatorClientRewards](data, disc)
			if err != nil {
				return nil, fmt.Errorf("deserializing validator client rewards %s: %w", acct.Pubkey, err)
			}
			result.ValidatorRewards = append(result.ValidatorRewards, shreds.KeyedValidatorClientRewards{
				Pubkey:                 acct.Pubkey,
				ValidatorClientRewards: *item,
			})
		default:
			// Skip unknown account types (e.g. ExecutionController, ProgramConfig,
			// ShredDistribution, ephemeral requests) — these are fetched individually.
		}
	}

	return result, nil
}

const discriminatorSize = 8

// deserializeAccount validates the discriminator and deserializes the account
// data. Mirrors the SDK's internal logic.
func deserializeAccount[T any](data []byte, disc [8]byte) (*T, error) {
	if len(data) < discriminatorSize {
		return nil, fmt.Errorf("account data too short for discriminator")
	}
	var got [8]byte
	copy(got[:], data[:8])
	if got != disc {
		return nil, fmt.Errorf("invalid discriminator: got %x, want %x", got, disc)
	}
	body := data[discriminatorSize:]
	var zero T
	need := int(unsafe.Sizeof(zero))
	if len(body) < need {
		return nil, fmt.Errorf("account data too short: have %d bytes, need at least %d", len(body), need)
	}
	var item T
	if err := binary.Read(bytes.NewReader(body[:need]), binary.LittleEndian, &item); err != nil {
		return nil, fmt.Errorf("deserializing: %w", err)
	}
	return &item, nil
}
