import {
  Activity,
  Terminal,
  Database,
  Shield,
  Monitor,
  Sun,
  Moon,
  Menu,
  X,
  LogOut,
} from "lucide-react";
import { useState, useEffect } from "react";

export interface NavbarProps {
  activeTab: "dashboard" | "query" | "explorer" | "security";
  setActiveTab: (tab: "dashboard" | "query" | "explorer" | "security") => void;
  theme: "system" | "light" | "dark";
  setTheme: (theme: "system" | "light" | "dark") => void;
  securityMode?: string;
  isAuthenticated?: boolean;
  userRole?: string | null;
  onLogout?: () => void;
}

export default function Navbar({
  activeTab,
  setActiveTab,
  theme,
  setTheme,
  securityMode = "none",
  isAuthenticated = false,
  userRole = null,
  onLogout,
}: NavbarProps) {
  const [isDrawerOpen, setIsDrawerOpen] = useState(false);

  // Debug: Log when userRole changes
  useEffect(() => {
    console.log("Navbar: userRole changed to:", userRole);
  }, [userRole]);

  const showAdminItems = userRole === "admin" && securityMode === "auth_key";

  // FIX: Filter out falsy values before rendering - only include security tab if admin
  const navItems = [
    { id: "dashboard" as const, label: "Dashboard", icon: Activity },
    { id: "query" as const, label: "Query Console", icon: Terminal },
    { id: "explorer" as const, label: "Stream Explorer", icon: Database },
    ...(showAdminItems
      ? [{ id: "security" as const, label: "Security & Keys", icon: Shield }]
      : []),
  ];

  const handleTabClick = (
    tabId: "dashboard" | "query" | "explorer" | "security",
  ) => {
    setActiveTab(tabId);
    setIsDrawerOpen(false);
  };

  const renderNavLinks = () => (
    <nav className="flex flex-col gap-1 w-full mt-10">
      {navItems.map((item) => {
        const Icon = item.icon;
        const isActive = activeTab === item.id;
        return (
          <button
            key={item.id}
            onClick={() => handleTabClick(item.id)}
            className={`w-full text-left pl-4 pr-3 py-3 text-[14px] font-bold tracking-wider transition-all flex items-center gap-3 border-l-2 ${
              isActive
                ? "border-primary bg-zinc-100 dark:bg-zinc-700 text-primary"
                : "border-transparent text-text-muted hover:bg-zinc-100 dark:hover:dark:bg-zinc-700 hover:text-text-main dark:text-text-muted dark:hover:text-text-main"
            }`}
          >
            <Icon
              className={`w-4 h-4 ${isActive ? "text-primary" : "text-text-muted dark:text-text-muted"}`}
            />
            <span>{item.label}</span>
          </button>
        );
      })}
    </nav>
  );

  const renderLogoutButton = () => {
    if (securityMode !== "auth_key" || !isAuthenticated) return null;
    return (
      <div className="px-4 py-2.5 border-t border-gray-200 dark:border-white/10">
        <button
          onClick={onLogout}
          className="w-full flex items-center justify-center gap-2 px-3 py-2 text-[13px] font-bold tracking-wider text-rose-600 hover:text-rose-700 dark:text-rose-400 dark:hover:text-rose-300 hover:bg-rose-50 dark:hover:bg-rose-950/20 border border-transparent hover:border-rose-200 dark:hover:border-rose-900/30 rounded-lg transition-all active:scale-[0.98]"
        >
          <LogOut className="w-3.5 h-3.5" />
          <span>Log out</span>
        </button>
      </div>
    );
  };

  const renderThemeAndVersion = () => (
    <div className="p-4 border-t border-gray-200 dark:border-white/10 flex items-center justify-between">
      <button
        onClick={() => {
          setTheme(
            theme === "system"
              ? "light"
              : theme === "light"
                ? "dark"
                : "system",
          );
        }}
        className="p-1.5 rounded-md border border-gray-200 dark:border-white/10 text-zinc-400 dark:text-zinc-400 hover:opacity-90 transition-colors"
        title={`Theme: ${theme === "system" ? "Auto" : theme === "light" ? "Light" : "Dark"}`}
      >
        <div className="flex items-center gap-1.5 text-[10px] font-medium tracking-wider">
          {theme === "system" && (
            <>
              <Monitor className="w-3.5 h-3.5" />
              <span>Auto</span>
            </>
          )}
          {theme === "light" && (
            <>
              <Sun className="w-3.5 h-3.5 animate-spin-slow" />
              <span>Light</span>
            </>
          )}
          {theme === "dark" && (
            <>
              <Moon className="w-3.5 h-3.5" />
              <span>Dark</span>
            </>
          )}
        </div>
      </button>

      <span className="text-[9px] font-mono tracking-wider font-semibold text-zinc-700 dark:text-zinc-400">
        v0.0.1
      </span>
    </div>
  );

  return (
    <>
      {/* ======================================================== */}
      {/* 1. WIDESCREEN MASTER SIDEBAR (Desktop >= 768px)          */}
      {/* ======================================================== */}
      <aside className="hidden md:flex w-64 h-screen fixed left-0 top-0 flex-col justify-between bg-white dark:bg-zinc-900 border-r border-white/10 dark:border-white/5 z-30 transition-colors duration-300">
        <div className="flex flex-col gap-6 w-full pt-6">
          {/* LOGO AND BRAND HEADER */}
          <div className="px-6 flex flex-row gap-1.5">
            <img src={"logo.svg"} className="h-10" alt="LivenDB Logo" />
            <h1 className="font-semibold text-lg leading-none mt-2 tracking-wider text-zinc-500 dark:text-white select-none">
              LivenDB
            </h1>
          </div>

          {/* MAIN DESKTOP LINKS */}
          <div className="flex-1 w-full">{renderNavLinks()}</div>
        </div>

        {/* BOTTOM PANELS */}
        <div className="flex flex-col w-full">
          {renderLogoutButton()}
          {renderThemeAndVersion()}
        </div>
      </aside>

      {/* ======================================================== */}
      {/* 2. RESPONSIVE TOP NAVBAR (Mobile < 768px)                */}
      {/* ======================================================== */}
      <header className="md:hidden h-14 w-full fixed top-0 left-0 px-4 flex items-center justify-between border-b border-white/10 dark:border-zinc-800/50 bg-white dark:bg-zinc-900 backdrop-blur-md z-40 transition-colors duration-300">
        <div className="flex flex-row gap-1.5 items-center">
          <img src={"logo.svg"} className="h-6" alt="LivenDB Logo" />
          <h1 className="font-semibold text-base leading-none tracking-wider text-zinc-500 dark:text-white select-none">
            LivenDB
          </h1>
        </div>
        <button
          onClick={() => setIsDrawerOpen(true)}
          className="p-1.5 rounded-md hover:bg-white/10 text-zinc-300 hover:text-white transition-colors"
          aria-label="Open Drawer"
        >
          <Menu className="w-5 h-5 text-zinc-900 dark:text-zinc-100" />
        </button>
      </header>

      {/* ======================================================== */}
      {/* 3. MOBILE OVERLAY DRAWER                                 */}
      {/* ======================================================== */}
      {isDrawerOpen && (
        <>
          {/* Backdrop Blur Overlay */}
          <div
            className="md:hidden fixed inset-0 bg-black/40 dark:bg-black/60 backdrop-blur-sm z-50 animate-fade-in"
            onClick={() => setIsDrawerOpen(false)}
          />
          {/* Slide-out drawer */}
          <div className="md:hidden fixed top-0 left-0 h-screen w-64 bg-white dark:bg-zinc-900 border-r border-white/10 dark:border-white/5 z-50 flex flex-col justify-between shadow-2xl animate-slide-right transition-colors duration-300">
            <div className="flex flex-col gap-6 w-full pt-5">
              {/* Drawer Header */}
              <div className="px-5 flex items-center justify-between">
                <div className="flex flex-row gap-1.5 items-center">
                  <img src={"logo.svg"} className="h-8" alt="LivenDB Logo" />
                  <h1 className="font-semibold text-lg leading-none tracking-wider text-zinc-500 dark:text-white select-none">
                    LivenDB
                  </h1>
                </div>
                <button
                  onClick={() => setIsDrawerOpen(false)}
                  className="p-1.5 rounded-md hover:bg-white/10 text-zinc-300 hover:text-white transition-colors"
                  aria-label="Close Drawer"
                >
                  <X className="w-5 h-5 text-zinc-600 dark:text-zinc-400" />
                </button>
              </div>

              {/* Main Links */}
              <div className="flex-1 w-full">{renderNavLinks()}</div>
            </div>

            {/* Drawer Bottom */}
            <div className="flex flex-col w-full">
              {renderLogoutButton()}
              {renderThemeAndVersion()}
            </div>
          </div>
        </>
      )}
    </>
  );
}
