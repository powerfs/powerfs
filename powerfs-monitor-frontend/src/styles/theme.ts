/**
 * Antd theme configuration for light & dark modes.
 *
 * Exports `lightTheme` and `darkTheme` (antd `ThemeConfig` objects) plus
 * a CSS-variable bridge so non-antd components can read the same tokens.
 */

import type { ThemeConfig } from 'antd'
import { theme as antdTheme } from 'antd'
import { tokens } from './tokens'

type ThemeMode = 'light' | 'dark'

const sharedToken = {
  colorPrimary: tokens.color.brand,
  colorSuccess: tokens.color.success,
  colorWarning: tokens.color.warning,
  colorError: tokens.color.danger,
  colorInfo: tokens.color.info,
  borderRadius: tokens.radius.md,
  borderRadiusLG: tokens.radius.lg,
  borderRadiusSM: tokens.radius.sm,
  fontFamily: tokens.fontFamily.sans,
  fontFamilyCode: tokens.fontFamily.mono,
  fontSize: tokens.fontSize.sm,
  controlHeight: 32,
}

export const lightTheme: ThemeConfig = {
  algorithm: antdTheme.defaultAlgorithm,
  token: {
    ...sharedToken,
    colorBgLayout: '#F5F7FA',
    colorBgContainer: '#FFFFFF',
    colorBgElevated: '#FFFFFF',
    colorTextBase: '#1F2937',
    colorBorder: tokens.color.neutral[200],
    colorBorderSecondary: tokens.color.neutral[100],
    boxShadow: tokens.shadow.card,
    boxShadowSecondary: tokens.shadow.hover,
  },
  components: {
    Layout: {
      headerBg: '#FFFFFF',
      headerHeight: tokens.layout.headerHeight,
      siderBg: '#FFFFFF',
      bodyBg: '#F5F7FA',
    },
    Card: {
      borderRadiusLG: tokens.radius.lg,
      paddingLG: tokens.layout.cardPadding,
      boxShadowTertiary: tokens.shadow.card,
    },
    Menu: {
      itemHeight: 40,
      iconSize: 16,
      activeBarBorderWidth: 0,
    },
    Table: {
      headerBg: tokens.color.neutral[50],
      headerColor: tokens.color.neutral[700],
      rowHoverBg: tokens.color.neutral[50],
      borderColor: tokens.color.neutral[200],
    },
    Statistic: {
      contentFontSize: tokens.fontSize.display,
    },
  },
}

export const darkTheme: ThemeConfig = {
  algorithm: antdTheme.darkAlgorithm,
  token: {
    ...sharedToken,
    colorBgLayout: '#0F172A',
    colorBgContainer: '#1E293B',
    colorBgElevated: '#1E293B',
    colorTextBase: '#F1F5F9',
    colorBorder: '#334155',
    colorBorderSecondary: '#1E293B',
    boxShadow: '0 1px 3px rgba(0,0,0,0.4)',
    boxShadowSecondary: '0 4px 12px rgba(0,0,0,0.5)',
  },
  components: {
    Layout: {
      headerBg: '#1E293B',
      headerHeight: tokens.layout.headerHeight,
      siderBg: '#0F172A',
      bodyBg: '#0F172A',
    },
    Card: {
      borderRadiusLG: tokens.radius.lg,
      paddingLG: tokens.layout.cardPadding,
      boxShadowTertiary: '0 1px 3px rgba(0,0,0,0.4)',
    },
    Menu: {
      itemHeight: 40,
      iconSize: 16,
      activeBarBorderWidth: 0,
      darkItemBg: 'transparent',
      darkSubMenuItemBg: 'transparent',
    },
    Table: {
      headerBg: '#1E293B',
      headerColor: '#CBD5E1',
      rowHoverBg: '#334155',
      borderColor: '#334155',
    },
    Statistic: {
      contentFontSize: tokens.fontSize.display,
    },
  },
}

export function getTheme(mode: ThemeMode): ThemeConfig {
  return mode === 'dark' ? darkTheme : lightTheme
}

export type { ThemeMode }