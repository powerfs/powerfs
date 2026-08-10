import i18n from 'i18next'
import { initReactI18next } from 'react-i18next'
import LanguageDetector from 'i18next-browser-languagedetector'

import enCommon from './locales/en/common.json'
import zhCommon from './locales/zh/common.json'
import enNav from './locales/en/nav.json'
import zhNav from './locales/zh/nav.json'
import enDashboard from './locales/en/dashboard.json'
import zhDashboard from './locales/zh/dashboard.json'

void i18n
  .use(LanguageDetector)
  .use(initReactI18next)
  .init({
    resources: {
      en: {
        common: enCommon,
        nav: enNav,
        dashboard: enDashboard,
      },
      zh: {
        common: zhCommon,
        nav: zhNav,
        dashboard: zhDashboard,
      },
    },
    fallbackLng: 'en',
    lng: 'en',
    defaultNS: 'common',
    interpolation: {
      escapeValue: false,
    },
    detection: {
      order: ['localStorage', 'navigator', 'htmlTag'],
      caches: ['localStorage'],
      lookupLocalStorage: 'powerfs.lang',
    },
  })

export default i18n

export type LangCode = 'en' | 'zh'

export const LANGUAGES: { code: LangCode; label: string; flag: string }[] = [
  { code: 'en', label: 'English', flag: '🇺🇸' },
  { code: 'zh', label: '中文', flag: '🇨🇳' },
]
