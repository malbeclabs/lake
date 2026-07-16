import { useMutation } from '@tanstack/react-query'
import { createSeatAlert, sendTestAlert, type SeatAlertCreateInput } from '../lib/api'

export function useCreateSeatAlert() {
  return useMutation({
    mutationFn: (input: SeatAlertCreateInput) => createSeatAlert(input),
  })
}

export function useSendTestAlert() {
  return useMutation({
    mutationFn: (id: string) => sendTestAlert(id),
  })
}
