/**
 * Theme context for PowerFS Monitor.
 *
 * Provides 'light' | 'dark' | 'auto' modes with persistence.
 * On 'auto', follows the system `prefers-color-scheme` and updates live.
 */

import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from 'react'

export type ThemeMode = 'light' | 'dark' | 'auto'

export type ResolvedTheme = 'light' | 'dark'

interface ThemeContextValue {
  /** User-selected mode (may be 'auto') */
  mode: ThemeMode
  /** Resolved actual theme after applying system preference */
  resolved: ResolvedTheme
  /** Switch to a specific mode */
  setMode: (mode: ThemeMode) => void
  /** Toggle between light and dark (resolves 'auto' first) */
  toggle: () => void
}

const STORAGE_KEY = 'powerfs-theme-mode'

const ThemeContext = createContext<ThemeContextValue | null>(null)

function readStoredMode(): ThemeMode {
  if (typeof window === 'undefined') return 'auto'
  const stored = window.localStorage.getItem(STORAGE_KEY)
  if (stored === 'light' || stored === 'dark' || stored === 'auto') {
    return stored
  }
  return 'auto'
}

function getSystemTheme(): ResolvedTheme {
  if (typeof window === 'undefined') return 'light'
  return window.matchMedia('(prefers-color-scheme: dark)').matches
    ? 'dark'
    : 'light'
}

function resolveTheme(mode: ThemeMode): ResolvedTheme {
  return mode === 'auto' ? getSystemTheme() : mode
}

function applyTheme(resolved: ResolvedTheme): void {
  if (typeof document === 'undefined') return
  const root = document.documentElement
  root.dataset.theme = resolved
  root.style.colorScheme = resolved
}

export const ThemeProvider: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => {
  const [mode, setModeState] = useState<ThemeMode>(readStoredMode)
  const [resolved, setResolved] = useState<ResolvedTheme>(() =>
    resolveTheme(readStoredMode()),
  )

  // Apply theme on resolved change
  useEffect(() => {
    applyTheme(resolved)
  }, [resolved])

  // Recompute resolved when mode changes
  useEffect(() => {
    setResolved(resolveTheme(mode))
  }, [mode])

  // Listen to system preference changes when in 'auto' mode
  useEffect(() => {
    if (mode !== 'auto' || typeof window === 'undefined') return
    const mql = window.matchMedia('(prefers-color-scheme: dark)')
    const handler = () => setResolved(getSystemTheme())
    mql.addEventListener('change', handler)
    return () => mql.removeEventListener('change', handler)
  }, [mode])

  const setMode = useCallback((next: ThemeMode) => {
    setModeState(next)
    try {
      window.localStorage.setItem(STORAGE_KEY, next)
    } catch {
      /* ignore quota errors */
    }
  }, [])

  const toggle = useCallback(() => {
    setMode(resolved === 'dark' ? 'light' : 'dark')
  }, [resolved, setMode])

  const value = useMemo<ThemeContextValue>(
    () => ({ mode, resolved, setMode, toggle }),
    [mode, resolved, setMode, toggle],
  )

  return (
    <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>
  )
}

export function useTheme(): ThemeContextValue {
  const ctx = useContext(ThemeContext)
  if (!ctx) {
    throw new Error('useTheme must be used inside <ThemeProvider>')
  }
  return ctx
}