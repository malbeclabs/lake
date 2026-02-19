import { useEffect } from 'react'

const BASE_TITLE = 'DoubleZero Data'

export function useDocumentTitle(title: string) {
  useEffect(() => {
    document.title = `${title} - ${BASE_TITLE}`
    return () => { document.title = BASE_TITLE }
  }, [title])
}
