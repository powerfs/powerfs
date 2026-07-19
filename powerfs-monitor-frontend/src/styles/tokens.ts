/**
 * PowerFS Monitor Design Tokens
 *
 * Single source of truth for color, spacing, typography, radius, shadow.
 * Consumed by:
 *   - antd ConfigProvider (theme algorithm)
 *   - CSS variables (auto-generated in theme.ts)
 *   - inline styles via `tokens.color.brand` etc.
 */

export const tokens = {
  /** Brand & semantic colors */
  color: {
    brand: '#1677FF',
    brandHover: '#4096FF',
    brandActive: '#0958D9',
    brandGradient: 'linear-gradient(135deg, #1677FF 0%, #722ED1 100%)',
    brandGradientSoft: 'linear-gradient(135deg, rgba(22,119,255,0.12) 0%, rgba(114,46,209,0.12) 100%)',

    success: '#52C41A',
    successBg: '#F6FFED',
    successBorder: '#B7EB8F',

    warning: '#FAAD14',
    warningBg: '#FFF7E6',
    warningBorder: '#FFD591',

    danger: '#FF4D4F',
    dangerBg: '#FFF1F0',
    dangerBorder: '#FFA39E',

    info: '#1890FF',
    infoBg: '#E6F7FF',
    infoBorder: '#91D5FF',

    purple: '#722ED1',
    cyan: '#13C2C2',

    /** Neutral grayscale (50=lightest, 900=darkest) */
    neutral: {
      50: '#F9FAFB',
      100: '#F3F4F6',
      200: '#E5E7EB',
      300: '#D1D5DB',
      400: '#9CA3AF',
      500: '#6B7280',
      600: '#4B5563',
      700: '#374151',
      800: '#1F2937',
      900: '#111827',
    },
  },

  /** Border radius scale */
  radius: {
    sm: 4,
    md: 8,
    lg: 12,
    xl: 16,
    pill: 9999,
  },

  /** Box shadows */
  shadow: {
    card: '0 1px 3px rgba(0, 0, 0, 0.06), 0 1px 2px rgba(0, 0, 0, 0.04)',
    hover: '0 4px 12px rgba(0, 0, 0, 0.08), 0 2px 4px rgba(0, 0, 0, 0.04)',
    pop: '0 8px 24px rgba(0, 0, 0, 0.12), 0 4px 8px rgba(0, 0, 0, 0.06)',
    glow: '0 0 0 4px rgba(22, 119, 255, 0.12)',
  },

  /** Spacing scale (4px base) */
  spacing: {
    xs: 4,
    sm: 8,
    md: 16,
    lg: 24,
    xl: 32,
    xxl: 48,
  },

  /** Font size scale */
  fontSize: {
    xs: 12,
    sm: 14,
    md: 16,
    lg: 18,
    xl: 20,
    xxl: 24,
    display: 32,
    hero: 48,
  },

  /** Font families */
  fontFamily: {
    sans: "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', 'Hiragino Sans GB', 'Microsoft YaHei', sans-serif",
    mono: "'JetBrains Mono', 'SF Mono', 'Cascadia Code', 'Fira Code', Consolas, monospace",
  },

  /** Layout dimensions */
  layout: {
    siderWidth: 240,
    siderCollapsedWidth: 64,
    headerHeight: 56,
    contentPadding: 24,
    cardPadding: 20,
  },

  /** Animation durations */
  duration: {
    fast: 150,
    base: 250,
    slow: 400,
  },

  /** Z-index scale */
  zIndex: {
    base: 0,
    dropdown: 1000,
    sticky: 1020,
    modal: 1040,
    popover: 1060,
    tooltip: 1080,
  },
} as const

export type Tokens = typeof tokens
