import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { Dashboard } from '@/pages/Dashboard'
import { Logs } from '@/pages/Logs'
import { Settings } from '@/pages/Settings'
import { Sidebar } from '@/components/Sidebar'

export function App() {
  return (
    <BrowserRouter>
      <div className="flex min-h-screen bg-background">
        <Sidebar />
        <main className="flex-1 min-w-0 p-6 overflow-y-auto">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/logs" element={<Logs />} />
            <Route path="/settings" element={<Settings />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  )
}
