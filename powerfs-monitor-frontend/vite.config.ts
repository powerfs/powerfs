import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  assetsInclude: ['**/*.svg'],
  build: {
    // Vendor chunks (antd ~1.2MB, echarts ~1MB) are large but cached
    // independently and loaded on demand. Suppress the 500KB warning noise.
    chunkSizeWarningLimit: 1500,
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes('node_modules')) {
            if (id.includes('echarts')) return 'echarts'
            if (id.includes('@ant-design') || id.includes('antd/')) return 'antd'
            if (id.includes('reactflow')) return 'reactflow'
            if (id.includes('framer-motion')) return 'motion'
          }
        },
      },
    },
  },
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8083',
        changeOrigin: true,
      },
      '/ws': {
        target: 'http://localhost:8083',
        ws: true,
        changeOrigin: true,
      },
    },
  },
})