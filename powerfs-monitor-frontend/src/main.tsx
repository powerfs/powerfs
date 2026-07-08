import ReactDOM from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import { ConfigProvider } from 'antd'
import zhCN from 'antd/locale/zh_CN'
import App from './App'
import './index.css'
import { setUseMock } from './services/api'

setUseMock(true)

ReactDOM.createRoot(document.getElementById('root')!).render(
  <BrowserRouter>
    <ConfigProvider locale={zhCN}>
      <App />
    </ConfigProvider>
  </BrowserRouter>,
)