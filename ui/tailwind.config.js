/** @type {import('tailwindcss').Config} */
export default {
  darkMode: 'class',
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        space: {
          950: '#030712',
          900: '#0b0f19',
          800: '#111827',
          700: '#1f2937',
        },
        electric: {
          blurple: 'var(--color-primary)',
          cyan: 'var(--color-secondary)',
        },
        primary: {
          DEFAULT: 'var(--color-primary)',
          hover: 'var(--color-primary-hover)',
          light: 'var(--color-primary-light)',
        },
        secondary: {
          DEFAULT: 'var(--color-secondary)',
          hover: 'var(--color-secondary-hover)',
          light: 'var(--color-secondary-light)',
        },
        accent: {
          DEFAULT: 'var(--color-accent)',
          hover: 'var(--color-accent-hover)',
          light: 'var(--color-accent-light)',
        },
        'body-bg': 'var(--color-body-bg)',
        'panel-bg': 'var(--color-panel-bg)',
        'navbar-bg': 'var(--color-navbar-bg)',
        'border-subtle': 'var(--color-border-subtle)',
        'text-main': 'var(--color-text-main)',
        'text-muted': 'var(--color-text-muted)',
        card: {
          DEFAULT: 'var(--color-panel-bg)',
          foreground: 'var(--color-text-main)',
        }
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
      },
      boxShadow: {
        glow: '0 0 15px -3px var(--color-primary-glow)',
        'glow-cyan': '0 0 15px -3px var(--color-secondary-glow)',
      }
    },
  },
  plugins: [],
}
