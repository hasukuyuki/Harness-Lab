import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

function packageNameFromId(id: string): string | null {
  const marker = 'node_modules/'
  const index = id.lastIndexOf(marker)
  if (index < 0) {
    return null
  }
  const packagePath = id.slice(index + marker.length)
  if (packagePath.startsWith('@')) {
    const [scope, name] = packagePath.split('/')
    return scope && name ? `${scope}/${name}` : null
  }
  return packagePath.split('/')[0] || null
}

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 3000,
    proxy: {
      '/api': {
        target: 'http://localhost:4600',
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
    chunkSizeWarningLimit: 550,
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (!id.includes('node_modules')) {
            return
          }
          const packageName = packageNameFromId(id)
          if (!packageName) {
            return 'vendor-misc'
          }
          if (packageName === '@ant-design/icons' || packageName === '@ant-design/colors') {
            return 'vendor-antd-icons'
          }
          if (packageName === 'antd' || packageName.startsWith('rc-') || packageName === '@rc-component/async-validator') {
            return 'vendor-antd-core'
          }
          if (
            packageName === 'react' ||
            packageName === 'react-dom' ||
            packageName === 'scheduler' ||
            packageName === 'react-is'
          ) {
            return 'vendor-react'
          }
          return 'vendor-misc'
        },
      },
    },
  },
})
