import ReactDOM from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import { ConfigProvider, App as AntdApp } from 'antd'
import zhCN from 'antd/locale/zh_CN'
import enUS from 'antd/locale/en_US'
import * as echarts from 'echarts'
import { useTranslation } from 'react-i18next'
import App from './App'
import { ThemeProvider, useTheme } from '@/styles/ThemeContext'
import { getTheme } from '@/styles/theme'
import { registerEChartsTheme } from '@/styles/echarts.theme'
import './i18n'
import './styles/theme.css'
import './index.css'

// Register custom ECharts themes once at startup.
registerEChartsTheme(echarts)

function ThemedApp() {
  const { resolved } = useTheme()
  const { i18n } = useTranslation()
  const antdLocale = i18n.language === 'zh' ? zhCN : enUS
  return (
    <ConfigProvider locale={antdLocale} theme={getTheme(resolved)}>
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
