import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    chunkSizeWarningLimit: 900,
  },
  server: {
    port: 5173,
    // 开发时前端独立跑，接口全部转发到 Go 服务。
    // SSE 必须关掉代理缓冲，否则流式会被攒成一坨再吐出来。
    proxy: {
      '/api': { target: 'http://localhost:8080', changeOrigin: true, ws: false },
      '/healthz': { target: 'http://localhost:8080', changeOrigin: true },
    },
  },
})
