import './App.css'
import Sidebar from './components/sidebar'

function App() {
  return (
    <div style={{ display: 'flex', minHeight: '100vh' }}>
      <Sidebar />
      <main style={{ flex: 1, padding: '16px' }}>
        <h1>Content Area</h1>
        <p>This is a placeholder. We are focusing on the sidebar in checkpoint 1.</p>
      </main>
    </div>
  )
}

export default App
