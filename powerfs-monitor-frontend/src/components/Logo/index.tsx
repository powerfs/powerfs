import { useTheme } from '@/styles/ThemeContext'
import logoLight from '../../../logo-light.svg'
import logoDark from '../../../logo-dark.svg'

const logos: Record<string, string> = {
  light: logoLight,
  dark: logoDark,
}

interface LogoProps {
  size?: number
  className?: string
  style?: React.CSSProperties
}

function Logo({ size = 24, className, style }: LogoProps) {
  const { mode } = useTheme()
  const isDark = mode === 'dark' || (mode === 'auto' && window.matchMedia('(prefers-color-scheme: dark)').matches)

  return (
    <img
      src={isDark ? logos.dark : logos.light}
      alt="PowerFS"
      style={{
        width: size,
        height: size,
        ...style,
      }}
      className={className}
    />
  )
}

export default Logo