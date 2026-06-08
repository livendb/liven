import { useState, useEffect } from "react";
import { Key, Eye, EyeOff, Sun, Moon } from "lucide-react";
import { submitSystemLogin } from "../utils/requests";

interface LoginProps {
  onLoginSuccess: () => void;
}

export default function Login({ onLoginSuccess }: LoginProps) {
  const [authKey, setAuthKey] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState(false);
  const [loading, setLoading] = useState(false);

  // Initialize theme from localStorage or system class list
  const [theme, setTheme] = useState<"light" | "dark" | "system">(() => {
    const saved = localStorage.getItem("liven-theme") as
      | "light"
      | "dark"
      | "system"
      | null;
    return saved || "dark";
  });

  const [resolvedTheme, setResolvedTheme] = useState<"light" | "dark">("dark");

  useEffect(() => {
    localStorage.setItem("liven-theme", theme);
    const mediaQuery = window.matchMedia("(prefers-color-scheme: dark)");

    const updateTheme = () => {
      let active: "light" | "dark" = "light";
      if (theme === "system") {
        active = mediaQuery.matches ? "dark" : "light";
      } else {
        active = theme;
      }
      setResolvedTheme(active);

      if (active === "dark") {
        document.documentElement.classList.add("dark");
      } else {
        document.documentElement.classList.remove("dark");
      }
    };

    updateTheme();

    if (theme === "system") {
      mediaQuery.addEventListener("change", updateTheme);
      return () => mediaQuery.removeEventListener("change", updateTheme);
    }
  }, [theme]);

  const toggleTheme = () => {
    setTheme((prev) => (prev === "dark" ? "light" : "dark"));
  };

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    setSuccess(false);

    const trimmedKey = authKey.trim();
    if (!trimmedKey) {
      setError("Please enter your auth key.");
      return;
    }

    try {
      setLoading(true);
      await submitSystemLogin(trimmedKey);
      setSuccess(true);
      setTimeout(() => {
        onLoginSuccess();
      }, 800);
    } catch (err: any) {
      console.error(err);
      setError(err.message || "Authentication failed. Invalid auth key");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="relative min-h-screen flex items-center justify-center overflow-hidden font-sans transition-all duration-500 bg-body-bg text-text-main">
      {/* Premium background gradient spheres and glows */}
      {resolvedTheme === "dark" ? (
        <>
          <div className="absolute top-1/4 left-1/4 w-[500px] h-[500px] bg-primary/20 rounded-full blur-[130px] pointer-events-none" />
          <div className="absolute bottom-1/4 right-1/4 w-[450px] h-[450px] bg-blue-950/20 rounded-full blur-[110px] pointer-events-none" />
        </>
      ) : (
        <>
          <div className="absolute top-1/4 left-1/4 w-[500px] h-[500px] bg-accent/20 rounded-full blur-[130px] pointer-events-none" />
          <div className="absolute bottom-1/4 right-1/4 w-[450px] h-[450px] bg-teal-50/50 rounded-full blur-[110px] pointer-events-none" />
        </>
      )}

      {/* Subtle fine tech-mesh pattern overlay */}
      <div
        className={`absolute inset-0 bg-[linear-gradient(rgba(255,255,255,0.007)_1px,transparent_1px),linear-gradient(90deg,rgba(255,255,255,0.007)_1px,transparent_1px)] bg-[size:32px_32px] opacity-25 ${resolvedTheme === "light" && "invert bg-[linear-gradient(rgba(0,0,0,0.015)_1px,transparent_1px),linear-gradient(90deg,rgba(0,0,0,0.015)_1px,transparent_1px)]"}`}
      />

      {/* Floating Theme Switcher */}
      <div className="absolute top-6 right-6 z-20">
        <button
          type="button"
          onClick={toggleTheme}
          className={`p-3 rounded-2xl border transition-all duration-300 shadow-md ${
            resolvedTheme === "dark"
              ? "bg-zinc-950/60 border-zinc-800 text-primary hover:bg-zinc-900 hover:border-primary/60"
              : "bg-white/80 border-zinc-200/60 text-primary hover:bg-zinc-50 hover:border-primary-hover"
          }`}
          title={`Switch to ${resolvedTheme === "dark" ? "Light" : "Dark"} Mode`}
        >
          {resolvedTheme === "dark" ? (
            <Sun className="w-5 h-5" />
          ) : (
            <Moon className="w-5 h-5" />
          )}
        </button>
      </div>

      {/* Login glassmorphic card container */}
      <div
        className={`relative w-full max-w-md mx-4 p-8 rounded-3xl border backdrop-blur-2xl shadow-2xl transition-all  ${
          resolvedTheme === "dark"
            ? "border-zinc-800 bg-zinc-900 shadow-primary/30"
            : "border-primary/20 bg-white/90 shadow-primary/5"
        }`}
      >
        {/* Header/Logo */}
        <div className="text-center mb-8">
          <h1 className="text-4xl font-extrabold tracking-tight text-primary">
            LIVEN
          </h1>
          <p className="text-xs font-bold tracking-widest mt-1.5 uppercase text-accent">
            Admin Login
          </p>
        </div>

        {/* Action feedback notifications */}
        {error && (
          <div
            className={`flex items-start gap-3 p-4 border rounded-2xl text-xs mb-6 animate-shake ${
              resolvedTheme === "dark"
                ? "bg-red-950/30 border-red-500/20 text-red-200"
                : "bg-red-50 border-red-200 text-red-800"
            }`}
          >
            <span>{error}</span>
          </div>
        )}

        {success && (
          <div
            className={`flex items-start gap-3 p-4 border rounded-2xl text-xs mb-6 animate-pulse ${
              resolvedTheme === "dark"
                ? "bg-accent/10 border-accent/20 text-accent"
                : "bg-accent/5 border-accent/25 text-accent"
            }`}
          >
            <span>Key Authenticated! Securing admin session...</span>
          </div>
        )}

        <form onSubmit={handleLogin} className="space-y-6">
          <div>
            <label
              className={`block text-[10px] font-bold uppercase tracking-wider mb-2 ${
                resolvedTheme === "dark" ? "text-zinc-400" : "text-zinc-600"
              }`}
            >
              ENTER AUTH KEY
            </label>
            <div className="relative">
              <input
                type={showPassword ? "text" : "password"}
                value={authKey}
                onChange={(e) => setAuthKey(e.target.value)}
                className={`w-full pl-4 pr-11 py-3.5 font-mono text-xs rounded-2xl border focus:outline-none focus:ring-2 focus:ring-primary/30 transition-all duration-300 ${
                  resolvedTheme === "dark"
                    ? "bg-zinc-950/50 border-zinc-800 text-zinc-100 placeholder-zinc-700"
                    : "bg-zinc-50 border-zinc-200 text-zinc-900 placeholder-zinc-400"
                }`}
              />
              <button
                type="button"
                onClick={() => setShowPassword(!showPassword)}
                className={`absolute right-3.5 top-1/2 -translate-y-1/2 p-1 rounded-lg transition-colors ${
                  resolvedTheme === "dark"
                    ? "text-zinc-600 hover:text-zinc-300"
                    : "text-zinc-400 hover:text-zinc-700"
                }`}
              >
                {showPassword ? (
                  <EyeOff className="w-4.5 h-4.5" />
                ) : (
                  <Eye className="w-4.5 h-4.5" />
                )}
              </button>
            </div>
          </div>

          {/* Submit Action Button */}
          <button
            type="submit"
            disabled={loading || success}
            className="w-full flex items-center justify-center gap-2 py-4 bg-primary hover:bg-primary-hover text-zinc-50 font-bold text-xs uppercase tracking-wider rounded-2xl shadow-lg hover:shadow-primary/10 focus:outline-none focus:ring-2 focus:ring-primary/30 disabled:opacity-50 disabled:cursor-not-allowed transition-all duration-300 active:scale-[0.98]"
          >
            {loading ? (
              <span className="flex items-center gap-2">
                <svg
                  className="animate-spin h-4 w-4 text-white"
                  fill="none"
                  viewBox="0 0 24 24"
                >
                  <circle
                    className="opacity-25"
                    cx="12"
                    cy="12"
                    r="10"
                    stroke="currentColor"
                    strokeWidth="4"
                  />
                  <path
                    className="opacity-75"
                    fill="currentColor"
                    d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.14 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                  />
                </svg>
                <span>Verifying credentials...</span>
              </span>
            ) : (
              <>
                <Key className="w-4 h-4" />
                <span>Sign In</span>
              </>
            )}
          </button>
        </form>
      </div>
    </div>
  );
}
