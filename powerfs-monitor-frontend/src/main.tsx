import ReactDOM from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import { ConfigProvider, App as AntdApp } from 'antd'
import zhCN from 'antd/locale/zh_CN'
import * as echarts from 'echarts'
import App from './App'
import { ThemeProvider, useTheme } from '@/styles/ThemeContext'
import { getTheme } from '@/styles/theme'
import { registerEChartsTheme } from '@/styles/echarts.theme'
import { setUseMock } from '@/services/api'
import './styles/theme.css'
import './index.css'

setUseMock(true)

// Register custom ECharts themes once at startup.
registerEChartsTheme(echarts)

function ThemedApp() {
  const { resolved } = useTheme()
  return (
    <ConfigProvider locale={zhCN} theme={getTheme(resolved)}>
      <AntdApp>
        <App />
      </AntdApp>
    </ConfigProvider>
  )
}

ReactDOM.createRoot(document.getElementById('root')!).render(
  <ThemeProvider>
    <BrowserRouter>
      <ThemedApp />
    </BrowserRouter>
  </ThemeProvider>,
)