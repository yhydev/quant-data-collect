import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const proxyTarget = process.env.VITE_API_URL || 'http://localhost:8000'

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 3000,
    proxy: {
      '/api': {
        target: proxyTarget,
        changeOrigin: true,
      },
    },
  },
})
